"""
Connect to an IRC server.

Config:
    server:
        host (str):
            Hostname of the server.
        port (int):
            Non-SSL port of the server.
        ssl (bool):
            Whether to connect using SSL.
        password (str):
            Optional password required for the server.
    user:
        nick (str):
            Primary nick for the bot user.
        real-name (str):
            Real name, as displayed in WHO queries.
    quit (str):
        Quit message, sent as part of disconnection from the server.
    accept-invites (bool):
        ``True`` to auto-join channels when an INVITE is received.
    colour-nicks (bool):
        ``True`` to apply IRC colour formatting to nicks when including author names in messages
        (i.e. when not using puppets).
    puppet (bool):
        Whether to use multiple IRC clients for sending.  If enabled, new client connections will
        be made when sending a message with an unseen username, and reused for later messages.
    puppet-prefix (str):
        Leading characters to include in nicks of puppet users.

Channel sources should include the correct channel prefix or prefixes (typically just ``#``) for
shared channels, and bare IRC nicks for private channels.
"""

from asyncio import (CancelledError, ensure_future, Event, Future, open_connection, sleep,
                     TimeoutError, wait_for)
import codecs
from hashlib import md5
import logging
import re

import immp


log = logging.getLogger(__name__)


def _codec_error_latin1(exc):
    return (exc.object[exc.start:exc.end].decode("latin-1", "ignore"), exc.end)


# Fall back to Latin-1 decoding if UTF-8 fails: bytes.decode("utf-8", "retry-latin1")
codecs.register_error("retry-latin1", _codec_error_latin1)


class IRCError(immp.PlugError):
    """
    Generic error from the IRC server.
    """


class IRCTryAgain(IRCError):
    """
    Rate-limited response to an IRC command (RPL_TRYAGAIN, 263).
    """
    COMMAND = "263"


class Line:
    """
    Low-level representation of an IRC message, either sent or received.  Calling :func:`str` on a
    line will encode it suitable for transmission.

    Attributes:
        command (str):
            IRC verb or numeric.
        args (str list):
            Additional arguments for this message.
        source (str list):
            Optional source of this message.
        tags ((str, str) dict):
            Any tags attached to the message.
    """

    _format = re.compile("(?:@(?P<tags>[a-z0-9-]+(?:=[^; ]+)?(?:;[a-z0-9-]+(?:=[^; ]+)?)*) +)?"
                         "(?::(?P<source>[^ ]+) +)?(?P<command>[a-z]+|[0-9]{3})"
                         "(?P<args>(?: +[^: ][^ ]*)*)(?: +:(?P<trailing>.*))?", re.I)

    def __init__(self, command, *args, source=None, tags=None):
        self.command = command.upper()
        self.args = args
        self.source = source
        self.tags = tags

    # Conveniently, this generates timestamp identifiers of the desired format.
    next_ts = immp.IDGen()

    @classmethod
    def parse(cls, line):
        """
        Take a raw IRC line and decode it into a :class:`.Line`.

        Args:
            line (str):
                Raw IRC message.

        Returns:
            .Line:
                Parsed line.
        """
        match = cls._format.match(line)
        if not match:
            raise ValueError("Invalid line: '{}'".format(line))
        tagpart, source, command, argpart, trailing = match.groups()
        tags = {}
        args = []
        if tagpart:
            for item in tagpart.split(";"):
                key, *val = item.split("=", 1)
                tags[key] = val[0] if val else True
        if argpart:
            args = argpart.split()
        if trailing:
            args.append(trailing)
        return cls(command, *args, source=source, tags=tags)

    def __str__(self):
        line = self.command
        if self.source:
            line = ":{} {}".format(self.source, line)
        if self.tags:
            tagpart = []
            for key, value in self.tags:
                tagpart.append(key if value is True else "{}={}".format(key, value))
            line = "@{} {}".format(";".join(tagpart), line)
        if self.args:
            line = " ".join([line, *self.args[:-1], ":{}".format(self.args[-1])])
        return line

    def __repr__(self):
        return "<{}: {}{}{}>".format(self.__class__.__name__, self.command,
                                     " @ {}".format(repr(self.source)) if self.source else "",
                                     " {}".format(repr(list(self.args))) if self.args else "")


class IRCSegment(immp.Segment):
    """
    Plug-friendly representation of IRC message formatting.
    """

    @classmethod
    def _coloured(cls, colour, text):
        # Includes a default background colour to avoid accidental combinations with a literal comma
        # following the text.
        return "\x03{}{}\x0399,99".format(colour, text)

    @classmethod
    def to_formatted(cls, segment):
        """
        Convert a :class:`.Segment` into text formatted using IRC ASCII escape sequences.

        Args:
            segment (.Segment)
                Message segment created by another plug.

        Returns:
            str:
                Code-formatted string.
        """
        text = segment.text
        if segment.bold:
            text = "\x02{}\x02".format(text)
        if segment.italic:
            text = "\x1d{}\x1d".format(text)
        if segment.underline:
            text = "\x1f{}\x1f".format(text)
        if segment.strike:
            # Muted text by colouring it grey.
            text = cls._coloured(14, text)
        if segment.link and segment.text != segment.link:
            text = "{} [{}]".format(text, segment.link)
        return text


class IRCRichText(immp.RichText):
    """
    Wrapper for IRC-specific encoding of formatting.
    """

    @classmethod
    def to_formatted(cls, rich):
        """
        Convert a :class:`.RichText` into text formatted using IRC ASCII escape sequences.

        Args:
            rich (.RichText):
                Parsed rich text container.

        Returns:
            str:
                Code-formatted string.
        """
        return "".join(IRCSegment.to_formatted(segment) for segment in rich).replace("\t", " ")


class IRCUser(immp.User):
    """
    User present in IRC.
    """

    # All non-greyscale colours: https://modern.ircdocs.horse/formatting.html#color
    _nick_colours = [str(colour).zfill(2) for colour in range(2, 14)]

    @classmethod
    def nick_colour(cls, nick):
        """
        Assign a random but stable colour to a string, similar to IRC clients' display of nicks.

        Args:
            nick (str):
                Name of a user.

        Returns:
            int:
                IRC colour code, between 2 and 13 inclusive.
        """
        # Values from Python's hash() will vary across restarts, so use a stable hash function.
        hashed = int.from_bytes(md5(nick.encode()).digest(), "little")
        return cls._nick_colours[hashed % len(cls._nick_colours)]

    @classmethod
    def from_id(cls, irc, id_, real_name=None):
        """
        Extract the nick from a nickmask into a :class:`.User`.

        Args:
            irc (.IRCPlug):
                Related plug instance that provides the user.
            id_ (str):
                Nickmask of the target user.
            real_name (str):
                Display name of the user, if known.

        Returns:
            .User:
                Parsed user object.
        """
        nick = id_.split("!", 1)[0]
        return immp.User(id_=id_, plug=irc, username=nick, real_name=real_name, raw=id_)

    @classmethod
    def from_who(cls, irc, line):
        """
        Convert the response of a ``WHO`` query into a :class:`.User`.

        Args:
            irc (.IRCPlug):
                Related plug instance that provides the user.
            line (.Line):
                352-numeric line containing a user's nickmask and real name.

        Returns:
            .User:
                Parsed user object.
        """
        id_ = "{}!{}@{}".format(line.args[5], line.args[2], line.args[3])
        username = line.args[5]
        real_name = line.args[-1].split(" ", 1)[-1]
        return immp.User(id_=id_, plug=irc, username=username, real_name=real_name, raw=line)


class IRCMessage(immp.Message):
    """
    Message originating from IRC.
    """

    @classmethod
    async def from_line(cls, irc, line):
        """
        Convert a :class:`.Line` into a :class:`.Message`.

        Args:
            irc (.IRCPlug):
                Related plug instance that provides the line.
            line (.Line):
                Raw message line from the server.

        Returns:
            .IRCMessage:
                Parsed message object.
        """
        channel = line.args[0]
        nick = line.source.split("!", 1)[0]
        if channel == irc.config["user"]["nick"]:
            # Private messages arrive "from A to B", and should be sent "from B to A".
            channel = nick
        user = IRCUser.from_id(irc, line.source)
        action = False
        joined = []
        left = []
        if line.command == "JOIN":
            text = "joined {}".format(channel)
            action = True
            joined.append(user)
        elif line.command == "PART":
            text = "left {}".format(channel)
            action = True
            left.append(user)
        elif line.command == "KICK":
            target = await irc.user_from_username(line.args[1])
            text = immp.RichText([immp.Segment("kicked "),
                                  immp.Segment(target.username, bold=True, mention=target),
                                  immp.Segment(" ({})".format(line.args[2]))])
            action = True
            left.append(target)
        elif line.command == "PRIVMSG":
            plain = line.args[1]
            match = re.match(r"\x01ACTION ([^\x01]*)\x01", plain)
            if match:
                plain = match.group(1)
                action = True
            text = immp.RichText()
            puppets = {client.nick: user for user, client in irc._puppets.items()}
            for match in re.finditer(r"[\w\d_\-\[\]{}\|`]+", plain):
                word = match.group(0)
                if word in puppets:
                    target = puppets[word]
                else:
                    target = irc.get_user(word)
                if target:
                    if len(text) < match.start():
                        text.append(immp.Segment(plain[len(text):match.start()]))
                    text.append(immp.Segment(word, mention=target))
            if len(text) < len(plain):
                text.append(immp.Segment(plain[len(text):]))
        else:
            raise NotImplementedError
        return immp.SentMessage(id_=Line.next_ts(),
                                channel=immp.Channel(irc, channel),
                                text=text,
                                user=user,
                                action=action,
                                joined=joined,
                                left=left,
                                raw=line)


class Wait:
    """
    Request-like object for sending a :class:`.Line` and waiting on a response.

    After sending, await an instance of this class to retrieve all collected lines after a success
    line is received.  On failure, an :class:`IRCError` is raised.

    Attributes:
        done (bool):
            ``True`` once a success or fail line has been received.
    """

    def __init__(self, success, fail, collect):
        self._success = tuple(success)
        self._fail = tuple(fail)
        self._collect = tuple(collect)
        self._data = []
        self._result = Future()

    @property
    def done(self):
        return self._result.done()

    def add(self, line):
        """
        Consume and handle a line, or raise :class:`TypeError` if it's not applicable to this wait.

        Args:
            line (.Line):
                Incoming line to be handled.
        """
        if line.command == IRCTryAgain.COMMAND:
            self._result.set_exception(IRCTryAgain(line))
            return
        if line.command in self._collect:
            self._data.append(line)
        if line.command in self._success:
            self._result.set_result(self._data)
        elif line.command in self._fail:
            self._result.set_exception(IRCError(line))
        elif line.command not in self._collect:
            raise TypeError("Line not applicable")

    def cancel(self):
        """
        Cancel any tasks waiting on this wait.
        """
        self._result.set_exception(CancelledError("Wait was cancelled"))

    def __await__(self):
        return self._result.__await__()

    def __repr__(self):
        parts = []
        for key in ("success", "fail", "collect"):
            value = getattr(self, "_{}".format(key))
            if value:
                parts.append("{} {}".format(key, "/".join(value)))
        return "<{}: {}>".format(self.__class__.__name__, ", ".join(parts))


class IRCClient:
    """
    Minimal, standalone IRC client.

    Attributes:
        nick (str):
            Connected user's nickname.
        nickmask (str):
            Combination of the connected user's nick and hostname.
        name (str):
            Connected user's display name.
    """

    # Acceptable characters: A-Z 0-9 ^ _ - [ ] { } |
    _nick_bad_chars = re.compile(r"[^A-Z0-9^_\-\[\]{}\|]", re.I)

    def __init__(self, plug, host, port, ssl, nick, password=None,
                 user=None, name=None, on_connect=None, on_receive=None):
        self._plug = plug
        # Server parameters for (re)connections.
        self._host = host
        self._port = port
        self._ssl = ssl
        self._nick = self._nick_bad_chars.sub("", nick)
        self._password = password
        self._user = user
        self._name = name
        self._on_connect = on_connect
        self._on_receive = on_receive
        # Cache our user-host as seen by the server.
        self._mask = None
        # Connection streams.
        self._reader = self._writer = self._read_task = None
        self._closing = False
        # Background task to send pings if we don't receive any for a while.
        self._live = Event()
        self._live_task = None
        # Store channel and nick prefixes for matching and cleanup.
        self.types = ""
        self.prefixes = ""
        self._nicklen = None
        self.network = None
        # Track public channels, and joined channels' members.
        self.users = {}
        self.members = {}
        # Capture answers to pending queries.
        self._waits = []

    async def set_nick(self, value):
        """
        Set the user's nick, and wait until it's successful.  If connected and the desired nick is
        not available, underscores will be appended until a free nick is found.

        Args:
            value (str):
                Desired new nick.
        """
        value = self._nick_bad_chars.sub("", value)
        if self._nicklen:
            value = value[:self._nicklen]
        if self._nick == value:
            return
        if self._writer:
            await self._wait(Line("NICK", value),
                             success=("001", "NICK"),
                             fail=("431", "432", "433", "436"))
        else:
            self._nick = value

    @property
    def nick(self):
        return self._nick

    @nick.setter
    def nick(self, value):
        ensure_future(self.set_nick(value))

    @property
    def nickmask(self):
        if self._mask:
            return "{}!{}".format(self._nick, self._mask)
        else:
            return None

    @property
    def name(self):
        return self._name

    @property
    def closing(self):
        return self._closing

    async def _read_loop(self):
        while True:
            try:
                raw = await self._reader.readline()
            except ConnectionError:
                log.debug("Client %r disconnected", self._nick, exc_info=True)
                break
            if not raw:
                log.debug("Client %r reached EOF", self._nick)
                break
            try:
                line = Line.parse(raw.decode("utf-8", "retry-latin1").rstrip("\r\n"))
            except UnicodeDecodeError:
                log.warning("Client %r failed to decode IRC line", self._nick, exc_info=True)
                continue
            if line.command == "QUIT" and line.source == self.nickmask:
                log.debug("Client %r quitting", self._nick)
                break
            log.debug("Client %r received line: %r", self._nick, line)
            await self._handle(line)
        self._writer.close()
        self._reader = self._writer = None
        if not self._closing:
            ensure_future(self._reconnect("Disconnected"))

    async def _keepalive_loop(self):
        while True:
            try:
                await wait_for(self._live.wait(), timeout=120)
            except TimeoutError:
                # Nothing from the server in 2 minutes, send a PING.
                try:
                    await self._wait(Line("PING", self._nick), success=("PONG",))
                except TimeoutError:
                    log.debug("Client %r ping timeout", self._nick)
                    break
            finally:
                self._live.clear()
        if not self._closing:
            ensure_future(self._reconnect("Disconnected"))

    def _write(self, *lines):
        for line in lines:
            log.debug("Client %r sending line: %r", self._nick, line)
            self._writer.write("{}\r\n".format(line).encode())

    async def _wait(self, *lines, success=(), fail=(), collect=()):
        wait = Wait(success, fail, collect)
        log.debug("Client %r adding wait: %r", self._nick, wait)
        self._waits.append(wait)
        self._write(*lines)
        try:
            result = await wait_for(wait, 10)
            log.debug("Client %r completing wait: %r", self._nick, wait)
        except TimeoutError:
            log.warning("Client %r timed out on wait: %r", self._nick, wait)
            raise
        finally:
            self._waits.remove(wait)
        return result

    async def _handle(self, line):
        self._live.set()
        # Route lines to any waits listening for them.
        for wait in self._waits:
            try:
                wait.add(line)
            except TypeError:
                continue
            else:
                break
        if line.command == "001":
            # Update our nick again in case it was truncated or otherwise changed.
            if self._nick != line.args[0]:
                log.debug("Caught nickname rewrite: %s -> %s", self._nick, line.args[0])
                self._nick = line.args[0]
        elif line.command == "005":
            # Listen for channel type prefixes within ISUPPORT tokens.
            # Also listen for nick prefixes, of the form `(modes)prefixes`.
            for param in line.args[1:]:
                if "=" in param:
                    key, value = param.split("=", 1)
                else:
                    key = param
                    value = True
                if key == "CHANTYPES":
                    self.types = value
                elif key == "PREFIX":
                    self.prefixes = value.split(")", 1)[-1]
                elif key == "NICKLEN":
                    self._nicklen = int(value)
                elif key == "NETWORK":
                    self.network = value
        elif line.command in ("431", "432"):
            # Nick change rejected, revert internal nick to who the server says we are.
            if self._nick != line.args[0]:
                log.debug("Reverting failed nick change: %s -> %s", self._nick, line.args[0])
                self._nick = line.args[0]
        elif line.command in ("433", "436"):
            # Re-request the current nick with a trailing underscore.
            # Remove characters from the nick if needed to make it fit.
            parsed = line.args[1]
            log.debug("Nick collision: %s", parsed)
            if len(parsed) < len(self._nick):
                # We got silently truncated, set the max nick length.
                self._nicklen = len(parsed)
                self._nick = parsed
            if len(self._nick.rstrip("_")) < 2:
                raise ValueError("Nick exhausted")
            if self._nicklen and len(self._nick) >= self._nicklen:
                base = self._nick[:self._nicklen].rstrip("_")
                self.nick = base[:-1].ljust(self._nicklen, "_")
            else:
                self.nick += "_"
        elif line.command == "PING":
            self._write(Line("PONG", *line.args))
        elif line.command in ("JOIN", "PART", "KICK", "PRIVMSG"):
            nick = line.source.split("!", 1)[0]
            channel = line.args[0]
            if channel in self.members:
                if line.command == "JOIN":
                    log.debug("Adding %s to %s member list", nick, channel)
                    self.members[channel].add(nick)
                elif line.command in ("PART", "KICK"):
                    log.debug("Removing %s from %s member list", nick, channel)
                    self.members[channel].remove(nick)
        elif line.command == "QUIT":
            nick = line.source.split("!", 1)[0]
            for name, members in list(self.members.items()):
                if name == nick:
                    log.debug("Removing %s self entry", nick)
                    del self.members[nick]
                elif nick in members:
                    log.debug("Converting QUIT to PART for %s in %s", nick, name)
                    await self._handle(Line("PART", name, source=line.source))
        elif line.command == "NICK":
            old = line.source.split("!", 1)[0]
            new = line.args[0]
            if self._nick == old:
                log.debug("Updating own nick: %s -> %s", self._nick, new)
                self._nick = new
            for name, members in list(self.members.items()):
                if name == old:
                    log.debug("Replacing %s with %s in self entry", old, new)
                    del self.members[old]
                    self.members[new] = {new}
                elif old in members:
                    log.debug("Replacing %s with %s in %s members", old, new, name)
                    members.remove(old)
                    members.add(new)
        if self._on_receive:
            await self._on_receive(line)

    async def connect(self):
        """
        Join the target IRC server.
        """
        self._closing = False
        self._reader, self._writer = await open_connection(self._host, self._port, ssl=self._ssl)
        self._read_task = ensure_future(self._read_loop())
        if self._password:
            self._write(Line("PASS", self._password))
        await self._wait(Line("USER", self._user, "0", "*", self._name),
                         Line("NICK", self._nick),
                         success=("001",))
        for user in await self.who(self._nick):
            if user.username == self._nick:
                self._mask = user.id.split("!", 1)[-1]
        self._live_task = ensure_future(self._keepalive_loop())
        if self._on_connect:
            await self._on_connect()

    async def disconnect(self, msg):
        """
        Quit from the connected IRC server.

        Args:
            msg (str):
                Disconnect message sent to the server.
        """
        self._closing = True
        for wait in self._waits:
            wait.cancel()
        self._waits.clear()
        if self._live_task:
            self._live_task.cancel()
            self._live_task = None
        if self._writer:
            self._write(Line("QUIT", msg))
            if self._read_task:
                try:
                    await wait_for(self._read_task, 10)
                except TimeoutError:
                    log.debug("Server didn't hangup after QUIT")
        if self._read_task:
            self._read_task.cancel()
            self._read_task = None
        if self._writer:
            self._writer.close()
            self._writer = None
        self._reader = None
        self.users.clear()
        self.members.clear()

    async def _reconnect(self, msg):
        await self.disconnect(msg)
        log.debug("Reconnecting in 3 seconds")
        await sleep(3)
        await self.connect()

    async def who(self, name):
        """
        Perform a user lookup on the server.

        Args:
            name (str):
                User nick or channel name.

        Returns:
            .Line list:
                Response lines from the server.
        """
        if name in self.users:
            self.members[name] = {name}
            return {self.users[name]}
        members = set()
        users = set()
        for line in await self._wait(Line("WHO", name), success=("315",), collect=("352",)):
            user = IRCUser.from_who(self._plug, line)
            members.add(user.username)
            users.add(user)
            self.members[user.username] = {user.username}
            self.users[user.username] = user
        if members:
            self.members[name] = members
        elif name in self.members:
            del self.members[name]
        return users

    async def join(self, channel):
        """
        Attempt to join the given channel.

        Args:
            channel (str):
                Target channel name.
        """
        if channel in self.members and self._nick in self.members[channel]:
            return
        await self._wait(Line("JOIN", channel), success=("JOIN",))
        await self.who(channel)

    def invite(self, channel, nick):
        """
        Invite another user to a channel you're partipating in.

        Args:
            channel (str):
                Target channel name.
            nick (str):
                User to be invited.
        """
        self._write(Line("INVITE", nick, channel))

    def kick(self, channel, nick):
        """
        Remove a user from a channel you're an operator of.

        Args:
            channel (str):
                Target channel name.
            nick (str):
                User to be kicked.
        """
        self._write(Line("KICK", channel, nick))

    async def list(self):
        """
        Request a list of open channels on the server.

        Returns:
            .Line list:
                Response lines from the server.
        """
        return await self._wait(Line("LIST"), success=("323",), collect=("322",))

    async def names(self):
        """
        Request a list of users in channels you're participating in.

        Returns:
            .Line list:
                Response lines from the server.
        """
        return await self._wait(Line("NAMES"), success=("366",), fail=("401",), collect=("353",))

    def send(self, channel, text):
        """
        Send a message to a user or channel.

        Args:
            channel (str):
                Target channel name or user nick.
            text (str):
                IRC-formatted message text.

        Returns:
            .Line:
                Resulting line sent to the server.
        """
        line = Line("PRIVMSG", channel, text)
        self._write(line)
        line.source = self.nickmask
        return line

    def __repr__(self):
        return "<{}: {!r}>".format(self.__class__.__name__, self.nickmask or self.nick)


class IRCPlug(immp.Plug):
    """
    Plug for an IRC server.
    """

    schema = immp.Schema({"server": {"host": str,
                                     "port": int,
                                     immp.Optional("ssl", False): bool,
                                     immp.Optional("password"): immp.Nullable(str)},
                          "user": {"nick": str,
                                   "real-name": str},
                          immp.Optional("quit", "Disconnecting"): str,
                          immp.Optional("accept-invites", False): bool,
                          immp.Optional("colour-nicks", False): bool,
                          immp.Optional("puppet", False): bool,
                          immp.Optional("puppet-prefix", ""): str})

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        self._client = None
        # Don't yield messages for initial self-joins.
        self._joins = set()
        # Maintain puppet clients by nick for cleaner sending.
        self._puppets = {}

    @property
    def network_name(self):
        if self._client and self._client.network:
            network = self._client.network
        else:
            network = self.config["server"]["host"]
        return "{} IRC".format(network)

    @property
    def network_id(self):
        return "irc:{}".format(self.config["server"]["host"])

    async def start(self):
        await super().start()
        self._client = IRCClient(self,
                                 self.config["server"]["host"],
                                 self.config["server"]["port"],
                                 self.config["server"]["ssl"],
                                 self.config["user"]["nick"],
                                 self.config["server"]["password"],
                                 "immp",
                                 self.config["user"]["real-name"],
                                 self._connected,
                                 self._handle)
        await self._client.connect()

    async def stop(self):
        await super().stop()
        if self._client:
            await self._client.disconnect(self.config["quit"])
            self._client = None
        for client in self._puppets.values():
            await client.disconnect(self.config["quit"])
        self._puppets.clear()

    def get_user(self, nick):
        return self._client.users.get(nick)

    async def user_from_id(self, id_):
        nick = id_.split("!", 1)[0]
        user = await self.user_from_username(nick)
        return user or IRCUser.from_id(self, id_)

    async def user_from_username(self, username):
        user = self.get_user(username)
        if user:
            return user
        for client in self._puppets.values():
            if username == client.nick:
                return IRCUser.from_id(self, client.nickmask, client.name)
        for user in await self._client.who(username):
            if user.username == username:
                return user
        else:
            return None

    async def user_is_system(self, user):
        if user.id == self._client.nickmask:
            return True
        for client in self._puppets.values():
            if user.id == client.nickmask:
                return True
        else:
            return False

    async def public_channels(self):
        try:
            raw = await self._client.list()
        except IRCTryAgain:
            return None
        channels = (line.args[1] for line in raw)
        return [immp.Channel(self, channel) for channel in channels]

    async def private_channels(self):
        try:
            raw = await self._client.names()
        except IRCTryAgain:
            return None
        names = set()
        for line in raw:
            names.update(name.lstrip(self._client.prefixes) for name in line.args[3].split())
        for client in self._puppets.values():
            names.remove(client.nick)
        return [immp.Channel(self, name) for name in names
                if name != self.config["user"]["nick"]]

    async def channel_for_user(self, user):
        return immp.Channel(self, user.username)

    async def channel_is_private(self, channel):
        return not channel.source.startswith(tuple(self._client.types))

    async def channel_title(self, channel):
        return channel.source

    async def channel_members(self, channel):
        if self._client.closing:
            return None
        try:
            nicks = self._client.members[channel.source]
        except KeyError:
            members = list(await self._client.who(channel.source))
        else:
            members = [await self.user_from_username(nick) for nick in nicks]
        if await channel.is_private() and members[0].id != self._client.nickmask:
            members.append(await self.user_from_id(self._client.nickmask))
        return members

    async def channel_invite(self, channel, user):
        self._client.invite(channel.source, user.username)

    async def channel_remove(self, channel, user):
        self._client.kick(channel.source, user.username)

    async def _connected(self):
        for channel in self.host.channels.values():
            if channel.plug == self and channel.source.startswith("#"):
                self._joins.add(channel.source)
                await self._client.join(channel.source)

    async def _handle(self, line):
        if line.command in ("JOIN", "PART", "KICK", "PRIVMSG"):
            sent = await IRCMessage.from_line(self, line)
            # Suppress initial joins and final parts.
            if sent.joined and sent.joined[0].id == self._client.nickmask:
                if sent.channel.source in self._joins:
                    self._joins.remove(sent.channel.source)
                    return
            elif sent.left and sent.left[0].id == self._client.nickmask and self._client.closing:
                return
            puppets = [client.nickmask for client in self._puppets.values()]
            # Don't yield messages sent by puppets, or for puppet kicks.
            if sent.user.id in puppets:
                pass
            elif sent.joined and all(user.id in puppets for user in sent.joined):
                pass
            elif sent.left and all(user.id in puppets for user in sent.left):
                pass
            else:
                self.queue(sent)
        elif line.command == "INVITE" and self.config["accept-invites"]:
            await self._client.join(line.args[1])

    def _lines(self, rich, user, action, edited, plain=False):
        if not rich:
            return []
        elif not isinstance(rich, immp.RichText):
            rich = immp.RichText([immp.Segment(rich)])
        lines = []
        for text in IRCRichText.to_formatted(rich).split("\n"):
            if user:
                template = "* {} {}" if action else "<{}> {}"
                name = user.username or user.real_name
                if self.config["colour-nicks"]:
                    name = IRCSegment._coloured(IRCUser.nick_colour(name), name)
                text = template.format(name, text)
            if edited:
                text = "[edit] {}".format(text)
            if not user and action:
                template = "* {}" if plain else "\x01ACTION {}\x01"
                text = template.format(text)
            lines.append(text)
        return lines

    async def _puppet(self, user):
        username = user.username or user.real_name
        nick = self.config["puppet-prefix"] + "-".join(username.split())
        try:
            puppet = self._puppets[user]
        except KeyError:
            pass
        else:
            log.debug("Reusing puppet %r for user %r", puppet, user)
            if puppet.nick.rstrip("_") != nick:
                await puppet.set_nick(nick)
            return puppet
        if user.plug and user.plug.network_id == self.network_id:
            for puppet in self._puppets.values():
                if user.id == puppet.nickmask:
                    log.debug("Matched nickmask with puppet %r", user.id)
                    return puppet
        log.debug("Adding puppet %r for user %r", nick, user)
        real_name = user.real_name or user.username
        if user.plug:
            real_name = "{} ({}{})".format(real_name, user.plug.network_name,
                                           ": {}".format(user.id) if user.id else "")
        puppet = IRCClient(self,
                           self.config["server"]["host"],
                           self.config["server"]["port"],
                           self.config["server"]["ssl"],
                           nick,
                           self.config["server"]["password"],
                           "immp",
                           real_name)
        self._puppets[user] = puppet
        await puppet.connect()
        return puppet

    async def put(self, channel, msg):
        user = None if self.config["puppet"] else msg.user
        lines = []
        if isinstance(msg.reply_to, immp.Message):
            quote = self._lines(msg.reply_to.text, msg.reply_to.user,
                                msg.reply_to.action, msg.edited, True)
            if quote:
                snippet = "{}{}".format(quote[0], " [...]" if len(quote) > 1 else "")
                lines += self._lines(snippet, user, False, msg.edited)
        if msg.text:
            lines += self._lines(msg.text, user, msg.action, msg.edited)
        for attach in msg.attachments:
            if isinstance(attach, immp.File):
                text = "uploaded a file{}".format(": {}".format(attach) if str(attach) else "")
                lines += self._lines(text, user, True, msg.edited)
            elif isinstance(attach, immp.Location):
                text = "shared a location: {}".format(attach)
                lines += self._lines(text, user, True, msg.edited)
            elif isinstance(attach, immp.Message) and attach.text:
                forward = self._lines(attach.text, attach.user, attach.action, attach.edited, True)
                if forward:
                    lines += self._lines("\n".join(forward), user, False, msg.edited)
        receipts = []
        if self.config["puppet"] and msg.user:
            client = await self._puppet(msg.user)
            if not await channel.is_private():
                await client.join(channel.source)
        else:
            client = self._client
        for text in lines:
            line = client.send(channel.source, text)
            sent = await IRCMessage.from_line(self, line)
            self.queue(sent)
            receipts.append(sent)
        return receipts
