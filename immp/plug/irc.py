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
"""

from asyncio import CancelledError, Future, Queue, ensure_future, open_connection, sleep
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
        tags (dict):
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
        user = immp.User(id_=line.source, plug=irc, username=nick, raw=line)
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
                                  immp.Segment(target.username, bold=True),
                                  immp.Segment(" ({})".format(line.args[2]))])
            action = True
            left.append(target)
        elif line.command == "PRIVMSG":
            text = line.args[1]
            match = re.match(r"\x01ACTION ([^\x01]*)\x01", text)
            if match:
                text = match.group(1)
                action = True
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

    def __init__(self, lines, success, fail, collect):
        self.lines = lines
        self._success = success
        self._fail = fail
        self._collect = collect
        self._data = []
        self._result = Future()

    def add(self, line):
        if line.command == "263":
            self._result.set_exception(IRCTryAgain(line))
            return True
        if line.command in self._collect:
            self._data.append(line)
        if line.command in self._success:
            self._result.set_result(self._data)
            return True
        elif line.command in self._fail:
            self._result.set_exception(IRCError(line))
            return True
        else:
            return False

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
                          immp.Optional("quit"): immp.Nullable(str),
                          immp.Optional("accept-invites", False): bool})

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        self._reader = self._writer = None
        self._prefixes = ""
        # Bot's own identifier as seen by the IRC server.
        self._source = None
        # Tracking fields for storing requested data by type.
        self._waits = Queue()
        self._current_wait = None
        # Don't yield messages for initial self-joins.
        self._joins = set()
        # Cache responses that may be rate-limited by the server.
        self._channels = self._names = None
        self._members = {}

    @property
    def network_name(self):
        return "{} IRC".format(self.config["server"]["host"])

    @property
    def network_id(self):
        return "irc:{}".format(self.config["server"]["host"])

    async def start(self):
        host = self.config["server"]["host"]
        port = self.config["server"]["port"]
        ssl = self.config["server"]["ssl"] or None
        reader, self._writer = await open_connection(host, port, ssl=ssl)
        self._reader = ensure_future(self._read_loop(reader, host, port))
        if self.config["server"]["password"]:
            self.write(Line("PASS", self.config["server"]["password"]))
        # We won't receive this until a valid nick has been set.
        await self.wait(Line("NICK", self.config["user"]["nick"]),
                        Line("USER", "immp", "0", "*", self.config["user"]["real-name"]),
                        success=("001",))
        self._source = (await self.user_from_username(self.config["user"]["nick"])).id
        for channel in self.host.channels.values():
            if channel.plug == self and channel.source.startswith("#"):
                self._joins.add(channel.source)
                await self._join(channel.source)

    async def stop(self):
        if self._current_wait:
            self._current_wait.cancel()
            self._current_wait = None
        while not self._waits.empty():
            self._waits.get_nowait().cancel()
        self._joins.clear()
        self.write(Line("QUIT", self.config["quit"] or "IMMP: stopping"))
        if self._reader:
            log.debug("Closing reader")
            self._reader.cancel()
            self._reader = None
        if self._writer:
            log.debug("Closing writer")
            await self._writer.drain()
            self._writer.close()
            self._writer = None
        self._source = None

    async def _read_loop(self, reader, host, port):
        while True:
            raw = await reader.readline()
            if not raw:
                # Connection has been closed.
                self._writer.close()
                self._reader = self._writer = None
                break
            line = Line.parse(raw.decode().rstrip("\r\n"))
            log.debug("Received line: %r", line)
            await self._handle(line)
        log.debug("Reconnecting in 3 seconds")
        await sleep(3)
        await self.start()

    async def _who(self, name):
        if name in self._members:
            return self._members[name]
        users = set()
        for line in await self.wait(Line("WHO", name), success=("315",), collect=("352",)):
            id_ = "{}!{}@{}".format(line.args[5], line.args[2], line.args[3])
            users.add(immp.User(id_=id_, plug=self, username=line.args[5], raw=line))
        if users:
            self._members[name] = users
        elif name in self._members:
            del self._members[name]
        return users

    async def _join(self, name):
        await self.wait(Line("JOIN", name), success=("366",), collect=("353",))
        await self._who(name)

    def _strip_prefix(self, name):
        return re.sub("^[{}]+".format(self._prefixes), "", name) if self._prefixes else name

    async def user_from_id(self, id_):
        nick = id_.split("!", 1)[0]
        return immp.User(id_=id_, plug=self, username=nick)

    async def user_from_username(self, username):
        for user in await self._who(username):
            if user.username == username:
                return user
        return None

    async def user_is_system(self, user):
        return user.id == self._source

    async def public_channels(self):
        try:
            self._channels = await self.wait(Line("LIST"), success=("323",), collect=("322",))
        except IRCTryAgain:
            pass
        if self._channels:
            return [immp.Channel(self, line.args[1]) for line in self._channels]
        else:
            return None

    async def private_channels(self):
        try:
            raw = await self.wait(Line("NAMES"), success=("366",), fail=("401",), collect=("353",))
        except IRCTryAgain:
            pass
        else:
            self._names = set()
            for line in raw:
                self._names.update(self._strip_prefix(name) for name in line.args[3].split())
        if self._names:
            return [immp.Channel(self, name) for name in self._names
                    if name != self.config["user"]["nick"]]
        else:
            return None

    async def channel_for_user(self, user):
        return immp.Channel(self, user.username)

    async def channel_is_private(self, channel):
        return bool(await self.user_from_username(channel.source))

    async def channel_title(self, channel):
        return channel.source

    async def channel_members(self, channel):
        members = list(await self._who(channel.source))
        if await channel.is_private() and members[0].id != self._source:
            members.append(await self.user_from_id(self._source))
        return members

    async def channel_invite(self, channel, user):
        self.write(Line("INVITE", user.username, channel.source))

    async def channel_remove(self, channel, user):
        self.write(Line("KICK", channel.source, user.username))

    def _sync_wait(self):
        if self._current_wait or self._waits.empty():
            return
        wait = self._waits.get_nowait()
        self._current_wait = wait
        self.write(*wait.lines)

    async def _handle(self, line):
        self._sync_wait()
        if self._current_wait and self._current_wait.add(line):
            log.debug("Completing wait: %r", self._current_wait)
            self._current_wait = None
            self._sync_wait()
        if line.command == "PING":
            self.write(Line("PONG", *line.args))
        elif line.command in ("JOIN", "PART", "KICK", "PRIVMSG"):
            sent = await IRCMessage.from_line(self, line)
            if sent.joined and sent.joined[0].username == self._source.split("!", 1)[0]:
                if sent.channel.source in self._joins:
                    self._joins.remove(sent.channel.source)
                    return
            self.queue(sent)
            if line.command == "JOIN":
                log.debug("Adding %s to %s members", sent.joined[0].username, sent.channel.source)
                self._members[sent.channel.source].add(sent.joined[0])
            elif line.command in ("PART", "KICK"):
                log.debug("Removing %s from %s members", sent.left[0].username, sent.channel.source)
                self._members[sent.channel.source].remove(sent.left[0])
        elif line.command == "QUIT":
            nick = line.source.split("!", 1)[0]
            find = immp.User(id_=line.source, plug=self, username=nick)
            for name, members in list(self._members.items()):
                if name == nick:
                    log.debug("Removing %s self entry", nick)
                    del self._members[nick]
                elif find in members:
                    log.debug("Converting QUIT to PART for %s in %s", nick, name)
                    await self._handle(Line("PART", name, source=line.source))
        elif line.command == "NICK":
            old, host = line.source.split("!", 1)
            new = line.args[0]
            find = immp.User(id_=line.source, plug=self, username=old)
            replace = immp.User(id_="{}!{}".format(new, host), plug=self, username=new, raw=line)
            for name, members in list(self._members.items()):
                if name == old:
                    log.debug("Replacing %s with %s in self entry", old, new)
                    del self._members[old]
                    self._members[new] = {replace}
                elif find in members:
                    log.debug("Replacing %s with %s in %s members", old, new, name)
                    members.remove(find)
                    members.add(replace)
        elif line.command == "INVITE" and self.config["accept-invites"]:
            await self._join(line.args[1])
        elif line.command == "005":
            for param in line.args[1:]:
                if param.startswith("PREFIX="):
                    self._prefixes = param.split(")", 1)[1]
        elif line.command == "433":
            # Nickname in use, try another.
            self.config["user"]["nick"] += "_"
            self.write(Line("NICK", self.config["user"]["nick"]),
                       Line("USER", "immp", "0", "*", self.config["user"]["real-name"]))

    def wait(self, *lines, success=(), fail=(), collect=()):
        wait = Wait(lines, success, fail, collect)
        log.debug("Adding wait: %r", wait)
        self._waits.put_nowait(wait)
        self._sync_wait()
        return wait

    def write(self, *lines):
        for line in lines:
            log.debug("Sending line: %r", line)
            self._writer.write("{}\r\n".format(line).encode())

    def _lines(self, rich, user, action, edited):
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

    async def put(self, channel, msg):
        lines = []
        edited = msg.edited if isinstance(msg, immp.Receipt) else False
        if msg.text:
            lines += self._lines(msg.text, msg.user, msg.action, edited)
        for attach in msg.attachments:
            if isinstance(attach, immp.File):
                text = "uploaded a file{}".format(": {}".format(attach) if str(attach) else "")
                lines += self._lines(text, msg.user, True, edited)
            elif isinstance(attach, immp.Location):
                text = "shared a location: {}".format(attach)
                lines += self._lines(text, msg.user, True, edited)
            elif isinstance(attach, immp.Message) and attach.text:
                lines += self._lines(attach.text, attach.user, attach.action,
                                     attach.edited if isinstance(attach, immp.Receipt) else False)
        ids = []
        for text in lines:
            line = Line("PRIVMSG", channel.source, text)
            self.write(line)
            line.source = self._source
            sent = await IRCMessage.from_line(self, line)
            self.queue(sent)
            ids.append(sent.id)
        return ids
