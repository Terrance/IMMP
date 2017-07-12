from collections import defaultdict
from datetime import datetime
import logging

import aiohttp
from voluptuous import Schema, Invalid, Any, Optional, ALLOW_EXTRA

import imirror


log = logging.getLogger(__name__)


class _Schema(object):

    config = Schema({"token": str}, extra=ALLOW_EXTRA, required=True)

    user = Schema({"id": int,
                   Optional("username", default=None): Any(str, None),
                   "first_name": str,
                   Optional("last_name", default=None): Any(str, None)},
                  extra=ALLOW_EXTRA, required=True)

    entity = Schema({"type": str,
                     "offset": int,
                     "length": int},
                    extra=ALLOW_EXTRA, required=True)

    message = Schema({"message_id": int,
                      "chat": {"id": int},
                      "date": int,
                      Optional("from", default=None): Any(user, None),
                      Optional("text", default=None): Any(str, None),
                      Optional("entities", default=[]): [entity],
                      Optional("reply_to_message", default=None):
                              Any(lambda v: _Schema.message(v), None),
                      Optional("new_chat_member", default=None): Any(user, None),
                      Optional("left_chat_member", default=None): Any(user, None)},
                     extra=ALLOW_EXTRA, required=True)

    update = Schema({"update_id": int,
                     Any("message", "edited_message",
                         "channel_post", "edited_channel_post"): message},
                    extra=ALLOW_EXTRA, required=True)


class TelegramAPIError(imirror.TransportError):
    """
    Generic error from the Telegram API.
    """


class TelegramUser(imirror.User):
    """
    User present in Telegram.
    """

    @classmethod
    def from_user(cls, telegram, json):
        """
        Convert a user :class:`dict` (attached to a message) to a :class:`.User`.

        Args:
            telegram (.TelegramTransport):
                Related transport instance that provides the user.
            json (dict):
                Telegram API `User <https://core.telegram.org/bots/api#user>`_ object.

        Returns:
            .TelegramUser:
                Parsed user object.
        """
        if json is None:
            return None
        user = _Schema.user(json)
        return cls(id=user["id"],
                   username=user["username"],
                   real_name=" ".join(filter(None, [user["first_name"], user["last_name"]])),
                   raw=user)


class TelegramRichText(imirror.RichText):
    """
    Wrapper for Telegram-specific parsing of formatting.
    """

    @classmethod
    def from_entities(cls, text, entities):
        """
        Convert a string annotated by Telegram's entities to :class:`.RichText`.

        Args:
            text (str):
                Plain text without formatting.
            entities (dict list):
                List of Telegram API `MessageEntity
                <https://core.telegram.org/bots/api#messageentity>`_ objects.

        Returns:
            .TelegramRichText:
                Parsed rich text container.
        """
        if text is None:
            return None
        changes = defaultdict(dict)
        for json in entities:
            entity = _Schema.entity(json)
            if entity["type"] not in ("bold", "italic", "code", "pre"):
                continue
            start = entity["offset"]
            end = start + entity["length"]
            changes[start][entity["type"]] = True
            changes[end][entity["type"]] = False
        segments = []
        points = list(changes.keys())
        # Iterate through text in change start/end pairs.
        for start, end in zip([0] + points, points + [len(text)]):
            if start == end:
                # Zero-length segment at the start or end, ignore it.
                continue
            segments.append(TelegramSegment(text[start:end], **changes[start]))
        return cls(segments)


class TelegramSegment(imirror.RichText.Segment):
    """
    Transport-friendly representation of Telegram message formatting.
    """

    @classmethod
    def to_html(cls, segment):
        """
        Convert a :class:`.RichText.Segment` into HTML suitable for Telegram's automatic parsing.

        Args:
            segment (.RichText.Segment)
                Message segment created by another transport.

        Returns:
            str:
                HTML-formatted string.
        """
        text = segment.text
        if segment.bold:
            text = "<b>{}</b>".format(text)
        if segment.italic:
            text = "<i>{}</i>".format(text)
        if segment.code:
            text = "<code>{}</code>".format(text)
        if segment.pre:
            text = "<pre>{}</pre>".format(text)
        return text


class TelegramMessage(imirror.Message):
    """
    Message originating from Telegram.
    """

    @classmethod
    def from_message(cls, telegram, json):
        """
        Convert an API message :class:`dict` to a :class:`.Message`.

        Args:
            telegram (.TelegramTransport):
                Related transport instance that provides the event.
            json (dict):
                Telegram API `message <https://core.telegram.org/bots/api#message>`_ object.

        Returns:
            .TelegramMessage:
                Parsed message object.
        """
        message = _Schema.message(json)
        reply_to = None
        joined = []
        left = []
        if message["reply_to_message"]:
            reply_to = message["reply_to_message"]["message_id"]
        if message["new_chat_member"]:
            joined.append(TelegramUser.from_user(telegram, message["new_chat_member"]))
        if message["left_chat_member"]:
            left.append(TelegramUser.from_user(telegram, message["left_chat_member"]))
        return cls(id=message["message_id"],
                   channel=telegram.host.resolve_channel(telegram, message["chat"]["id"]),
                   at=datetime.fromtimestamp(message["date"]),
                   text=TelegramRichText.from_entities(message["text"], message["entities"]),
                   user=TelegramUser.from_user(telegram, message["from"]),
                   reply_to=reply_to,
                   joined=joined,
                   left=left,
                   raw=message)

    @classmethod
    def from_update(cls, telegram, update):
        """
        Convert an API update :class:`dict` to a :class:`.Message`.

        Args:
            telegram (.TelegramTransport):
                Related transport instance that provides the event.
            update (dict):
                Telegram API `update <https://core.telegram.org/bots/api#update>`_ object.

        Returns:
            .TelegramMessage:
                Parsed message object.
        """
        for key in ("message", "channel_post"):
            if update.get(key):
                return cls.from_message(telegram, update[key])
            elif update.get("edited_{}".format(key)):
                msg = cls.from_message(telegram, update["edited_{}".format(key)])
                # Messages are edited in-place, no new ID is issued.
                msg.original = msg.id
                return msg


class TelegramTransport(imirror.Transport):
    """
    Transport for a `Telegram <https://telegram.org>`_ bot.

    Config
        token (str):
            Telegram API token for a bot user (obtained from ``@BotFather``).
    """

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        try:
            self.token = config["token"]
        except KeyError:
            raise imirror.ConfigError("Telegram token not specified") from None
        self.base = "https://api.telegram.org/bot{}".format(self.token)
        # Connection objects that need to be closed on disconnect.
        self.session = None
        # Update ID from which to retrieve the next batch.  Should be one higher than the max seen.
        self.offset = 0

    async def connect(self):
        await super().connect()
        self.session = aiohttp.ClientSession()

    async def disconnect(self):
        await super().disconnect()
        self.session = None
        self.offset = 0

    async def send(self, channel, msg):
        if msg.deleted:
            # TODO
            return
        if isinstance(msg.text, imirror.RichText):
            rich = msg.text.copy()
        else:
            # Unformatted text received, make a basic rich text instance out of it.
            rich = imirror.RichText([imirror.RichText.Segment(msg.text)])
        if msg.user:
            name = msg.user.real_name or msg.user.username
            prefix = ("{} " if msg.action else "{}: ").format(name)
            rich.insert(0, imirror.RichText.Segment(prefix, bold=True))
        if msg.action:
            for segment in rich:
                segment.italic = True
        text = "".join(TelegramSegment.to_html(segment) for segment in rich)
        async with self.session.post("{}/sendMessage".format(self.base),
                                     json={"chat_id": channel.source,
                                           "text": text,
                                           "parse_mode": "HTML"}) as resp:
            json = await resp.json()
        if not json.get("ok"):
            raise TelegramAPIError(json.get("description", json.get("error_code")))
        return json.get("result", {}).get("message_id")

    async def receive(self):
        await super().receive()
        while True:
            log.debug("Making long-poll request")
            async with self.session.get("{}/getUpdates".format(self.base),
                                        params={"offset": self.offset or "",
                                                "timeout": 240}) as resp:
                json = await resp.json()
            if not json.get("ok"):
                raise TelegramAPIError(json.get("description", json.get("error_code")))
            updates = json.get("result", [])
            for json in updates:
                try:
                    update = _Schema.update(json)
                except Invalid:
                    log.debug("Ignoring non-message update")
                    continue
                log.debug("Received a message")
                if any(key in update or "edited_{}".format(key) in update
                       for key in ("message", "channel_post")):
                    yield TelegramMessage.from_update(self, update)
                if update.get("update_id"):
                    self.offset = max(update["update_id"] + 1, self.offset)
