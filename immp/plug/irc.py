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
    puppet (bool):
        Whether to use multiple IRC clients for sending.  If enabled, new client connections will
        be made when sending a message with an unseen username, and reused for later messages.
    puppet-prefix (str):
        Leading characters to include in nicks of puppet users.
"""

from asyncio import (CancelledError, Future, ensure_future,
                     open_connection, sleep, TimeoutError, wait_for)
import logging
import re

import immp


log = logging.getLogger(__name__)


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
            # Muted text by colouring it grey.  Includes a default background colour to avoid
            # accidental combinations with a literal comma in a following segment.
            text = "\x0314{}\x0399,99".format(text)
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

    @classmethod
    def from_id(cls, irc, id_):
        nick = id_.split("!", 1)[0]
        return immp.User(id_=id_, plug=irc, username=nick, raw=id_)

    @classmethod
    def from_who(cls, irc, line):
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
    line is received.  On failure, a :class:`ValueError` is raised.
    """

    def __init__(self, success, fail, collect):
        self._success = tuple(success)
        self._fail = tuple(fail)
        self._collect = tuple(collect)
        self._data = []
        self._result = Future()

    def wants(self, line):
        return (line.command == IRCTryAgain.COMMAND or
                line.command in self._success + self._fail + self._collect)

    @property
    def done(self):
        return self._result.done()

    def add(self, line):
        if line.command == IRCTryAgain.COMMAND:
            self._result.set_exception(IRCTryAgain(line))
        if line.command in self._collect:
            self._data.append(line)
        if line.command in self._success:
            self._result.set_result(self._data)
        elif line.command in self._fail:
            self._result.set_exception(IRCError(line))

    def cancel(self):
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
    """

    def __init__(self, plug, host, port, ssl, nick, password=None,
                 user=None, name=None, listener=False):
        self._plug = plug
        # Server parameters for (re)connections.
        self._host = host
        self._port = port
        self._ssl = ssl
        self._nick = nick
        self._password = password
        self._user = user
        self._name = name
        self._listener = listener
        # Cache our user-host as seen by the server.
        self._mask = None
        # Connection streams.
        self._reader = self._writer = self._task = None
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

    async def _read_loop(self):
        while True:
            raw = await self._reader.readline()
            if not raw:
                # Connection has been closed.
                self._writer.close()
                self._reader = self._writer = None
                break
            line = Line.parse(raw.decode().rstrip("\r\n"))
            log.debug("Client %r received line: %r", self._nick, line)
            await self._handle(line)
        log.debug("Reconnecting in 3 seconds")
        await sleep(3)
        await self.connect()

    def _write(self, *lines):
        for line in lines:
            log.debug("Client %r sending line: %r", self._nick, line)
            self._writer.write("{}\r\n".format(line).encode())

    async def _wait(self, *lines, success=(), fail=(), collect=()):
        wait = Wait(success, fail, collect)
        log.debug("Adding wait: %r", wait)
        self._waits.append(wait)
        self._write(*lines)
        try:
            result = await wait_for(wait, 10)
            log.debug("Completing wait: %r", wait)
        except TimeoutError:
            log.warning("Timeout on wait: %r", wait, exc_info=True)
            raise
        finally:
            self._waits.remove(wait)
        return result

    async def _handle(self, line):
        # Route lines to any waits listening for them.
        for wait in self._waits:
            if wait.wants(line):
                wait.add(line)
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
        if self._listener:
            await self._plug._handle(line)

    async def connect(self):
        self._reader, self._writer = await open_connection(self._host, self._port, ssl=self._ssl)
        self._task = ensure_future(self._read_loop())
        if self._password:
            self._write(Line("PASS", self._password))
        await self._wait(Line("USER", self._user, "0", "*", self._name),
                         Line("NICK", self._nick),
                         success=("001",))
        for user in await self.who(self._nick):
            if user.username == self._nick:
                self._mask = user.id.split("!", 1)[-1]

    async def disconnect(self, msg):
        for wait in self._waits:
            wait.cancel()
        self._waits.clear()
        if self._task:
            self._task.cancel()
            self._task = None
        self._reader = None
        if self._writer:
            self._write(Line("QUIT", msg))
            await self._writer.drain()
            self._writer.close()
            self._writer = None

    async def who(self, name):
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
        if channel in self.members and self._nick in self.members[channel]:
            return
        await self._wait(Line("JOIN", channel), success=("JOIN",))
        await self.who(channel)

    def invite(self, channel, nick):
        self._write(Line("INVITE", nick, channel))

    def kick(self, channel, nick):
        self._write(Line("KICK", channel, nick))

    async def list(self):
        return await self._wait(Line("LIST"), success=("323",), collect=("322",))

    async def names(self):
        return await self._wait(Line("NAMES"), success=("366",), fail=("401",), collect=("353",))

    def send(self, channel, text):
        line = Line("PRIVMSG", channel, text)
        self._write(line)
        line.source = self.nickmask
        return line


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
        self._client = IRCClient(self,
                                 self.config["server"]["host"],
                                 self.config["server"]["port"],
                                 self.config["server"]["ssl"],
                                 self.config["user"]["nick"],
                                 self.config["server"]["password"],
                                 "immp",
                                 self.config["user"]["real-name"],
                                 True)
        await self._client.connect()
        for channel in self.host.channels.values():
            if channel.plug == self and channel.source.startswith("#"):
                self._joins.add(channel.source)
                await self._client.join(channel.source)

    async def stop(self):
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
        for user in await self._client.who(username):
            if user.username == username:
                return user
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
        return [immp.Channel(self, name) for name in names
                if name != self.config["user"]["nick"]]

    async def channel_for_user(self, user):
        return immp.Channel(self, user.username)

    async def channel_is_private(self, channel):
        return not channel.source.startswith(tuple(self._client.types))

    async def channel_title(self, channel):
        return channel.source

    async def channel_members(self, channel):
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

    async def _handle(self, line):
        if line.command in ("JOIN", "PART", "KICK", "PRIVMSG"):
            sent = await IRCMessage.from_line(self, line)
            if sent.joined and sent.joined[0].id == self._client.nickmask:
                if sent.channel.source in self._joins:
                    self._joins.remove(sent.channel.source)
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

    @classmethod
    def _lines(cls, rich, user, action, edited):
        if not rich:
            return []
        elif not isinstance(rich, immp.RichText):
            rich = immp.RichText([immp.Segment(rich)])
        lines = []
        for text in IRCRichText.to_formatted(rich).split("\n"):
            if user:
                template = "* {} {}" if action else "<{}> {}"
                text = template.format(user.username or user.real_name, text)
            if edited:
                text = "[edit] {}".format(text)
            if not user and action:
                text = "\x01ACTION {}\x01".format(text)
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
            log.debug("Reusing puppet %r for user %r", puppet.nickmask, user)
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
            real_name = "{} ({})".format(real_name, user.plug.network_name)
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
                lines += self._lines(attach.text, attach.user, attach.action, attach.edited)
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
