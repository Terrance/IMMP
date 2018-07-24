"""
Connect to an IRC server.

Config:
    server.host (str):
        Hostname of the server.
    server.port (int):
        Non-SSL port of the server.
    server.ssl (bool):
        Whether to connect using SSL.
    server.password (str):
        Optional password required for the server.
    user.nick (str):
        Primary nick for the bot user.
    user.real-name (str):
        Real name, as displayed in WHO queries.
"""

from asyncio import Condition, ensure_future, open_connection, sleep
from collections import defaultdict
import logging
import re
import time

from voluptuous import ALLOW_EXTRA, Any, Optional, Schema

import immp


log = logging.getLogger(__name__)


class _Schema():

    config = Schema({"server": {"host": str,
                                "port": int,
                                Optional("ssl", default=False): bool,
                                Optional("password", default=None): Any(str, None)},
                     "user": {"nick": str,
                              "real-name": str}},
                    extra=ALLOW_EXTRA, required=True)


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

    FORMAT = re.compile("(?:@(?P<tags>[a-z0-9-]+(?:=[^; ]+)?(?:;[a-z0-9-]+(?:=[^; ]+)?)*) +)?"
                        "(?::(?P<source>[^ ]+) +)?(?P<command>[a-z]+|[0-9]{3})"
                        "(?P<args>(?: +[^: ][^ ]*)*)(?: +:(?P<trailing>.*))?", re.I)

    def __init__(self, command, *args, source=None, tags=None):
        self.command = command
        self.args = args
        self.source = source
        self.tags = tags

    @classmethod
    def now(cls):
        """
        Generate a timestamp suitable for use as a TS value.

        Returns:
            str:
                Current timestamp in seconds.
        """
        return str(int(time.time()))

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
        match = cls.FORMAT.match(line)
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
    def from_line(cls, irc, line):
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
        user = immp.User(plug=irc, id=line.source, username=nick, raw=line)
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
        else:
            text = line.args[1]
            match = re.match(r"\x01ACTION ([^\x01]*)\x01", text)
            if match:
                text = match.group(1)
                action = True
        return (immp.Channel(irc, channel),
                immp.Message(id=Line.now(),
                             user=user,
                             text=text,
                             action=action,
                             joined=joined,
                             left=left,
                             raw=line))


class IRCPlug(immp.Plug):
    """
    Plug for an IRC server.
    """

    def __init__(self, name, config, host):
        super().__init__(name, _Schema.config(config), host)
        self._conds = defaultdict(Condition)
        self._reader = self._writer = None
        # Don't yield messages for initial self-joins.
        self._joins = set()

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
        self.write(Line("NICK", self.config["user"]["nick"]),
                   Line("USER", "immp", "0", "*", self.config["user"]["real-name"]))
        await self.wait("001")
        for channel in self.host.channels.values():
            if channel.plug == self and channel.source.startswith("#"):
                self._joins.add(channel.source)
                self.write(Line("JOIN", channel.source))
        await self._writer.drain()

    async def stop(self):
        if self._reader:
            log.debug("Closing reader")
            self._reader.cancel()
            self._reader = None
        if self._writer:
            log.debug("Closing writer")
            self._writer.close()
            self._writer = None

    async def _read_loop(self, reader, host, port):
        while True:
            raw = await reader.readline()
            if not raw:
                # Connection has been closed.
                self._writer.close()
                self._reader = self._writer = None
                break
            line = Line.parse(raw.decode().rstrip("\r\n"))
            log.debug("Received line: {}".format(repr(line)))
            await self.handle(line)
        log.debug("Reconnecting in 3 seconds")
        await sleep(3)
        await self.start()

    async def handle(self, line):
        with await self._conds[line.command]:
            self._conds[line.command].notify_all()
        if line.command == "PING":
            self.write(Line("PONG", *line.args))
        elif line.command in ("JOIN", "PART", "PRIVMSG"):
            channel, msg = IRCMessage.from_line(self, line)
            if msg.joined and msg.joined[0].username == self.config["user"]["nick"]:
                if channel.source in self._joins:
                    self._joins.remove(channel.source)
                    return
            self.queue(channel, msg)
        elif line.command == "433":
            # Nickname in use, try another.
            self.config["user"]["nick"] += "_"
            self.write(Line("NICK", self.config["user"]["nick"]),
                       Line("USER", "immp", "0", "*", self.config["user"]["real-name"]))

    def write(self, *lines):
        for line in lines:
            log.debug("Sending line: {}".format(repr(line)))
            self._writer.write("{}\r\n".format(line).encode())

    async def send(self, channel, msg):
        if msg.deleted or not msg.text:
            return
        if isinstance(msg.text, immp.RichText):
            formatted = IRCRichText.to_formatted(msg.text)
        else:
            formatted = str(msg.text)
        for text in formatted.split("\n"):
            if msg.edited:
                text = "[edit] {}".format(text)
            if msg.user:
                template = "* {} {}" if msg.action else "<{}> {}"
                text = template.format(msg.user.username or msg.user.real_name, text)
            elif msg.action:
                text = "\x01ACTION {}\x01".format(text)
            self.write(Line("PRIVMSG", channel.source, text))
        return []

    async def wait(self, command):
        # Block until we receive the response code we're looking for.
        with await (self._conds[command]):
            await (self._conds[command].wait())
