from asyncio import sleep
from collections import defaultdict
from datetime import datetime
import logging

from aiohttp import ClientSession, ClientResponseError, FormData
from voluptuous import Schema, Any, All, Optional, ALLOW_EXTRA

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
                      Optional("photo", default=[]): [{"file_id": str, "width": int}],
                      Optional("new_chat_members", default=[]): [user],
                      Optional("left_chat_member", default=None): Any(user, None),
                      Optional("new_chat_title", default=None): Any(str, None)},
                     extra=ALLOW_EXTRA, required=True)

    update = Schema({"update_id": int,
                     Optional(Any("message", "edited_message",
                                  "channel_post", "edited_channel_post")): message},
                    extra=ALLOW_EXTRA, required=True)

    file = Schema({"file_path": str}, extra=ALLOW_EXTRA, required=True)

    send = Schema({"message_id": int}, extra=ALLOW_EXTRA, required=True)

    def api(value, nested=All()):
        return Schema(Any({"ok": True,
                           "result": nested},
                          {"ok": False,
                           "description": str,
                           "error_code": int}),
                      extra=ALLOW_EXTRA, required=True)(value)


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


class TelegramSegment(imirror.Segment):
    """
    Transport-friendly representation of Telegram message formatting.
    """

    @classmethod
    def to_html(cls, segment):
        """
        Convert a :class:`.Segment` into HTML suitable for Telegram's automatic parsing.

        Args:
            segment (.Segment)
                Message segment created by another transport.

        Returns:
            str:
                HTML-formatted string.
        """
        text = segment.text
        # Any form of tag nesting (e.g. bold inside italic) isn't supported, so at most one type of
        # formatting may apply for each segment.
        if segment.link:
            text = "<a href=\"{}\">{}</a>".format(segment.link, text)
        elif segment.pre:
            text = "<pre>{}</pre>".format(text)
        elif segment.code:
            text = "<code>{}</code>".format(text)
        elif segment.bold:
            text = "<b>{}</b>".format(text)
        elif segment.italic:
            text = "<i>{}</i>".format(text)
        return text


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


class TelegramMessage(imirror.Message):
    """
    Message originating from Telegram.
    """

    @classmethod
    async def from_message(cls, telegram, json):
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
        text = None
        user = None
        action = False
        reply_to = None
        joined = None
        left = None
        attachments = []
        if message["from"]:
            user = TelegramUser.from_user(telegram, message["from"])
        if message["new_chat_title"]:
            action = True
        if message["new_chat_members"]:
            joined = [TelegramUser.from_user(telegram, member)
                      for member in message["new_chat_members"]]
            action = True
        if message["left_chat_member"]:
            left = [TelegramUser.from_user(telegram, message["left_chat_member"])]
            action = True
        if message["text"]:
            text = TelegramRichText.from_entities(message["text"], message["entities"])
        elif message["new_chat_title"]:
            text = TelegramRichText([TelegramSegment("changed group name to "),
                                     TelegramSegment(message["new_chat_title"], bold=True)])
        elif message["new_chat_members"]:
            if joined == [user]:
                text = TelegramRichText([TelegramSegment("joined group via invite link")])
            else:
                text = TelegramRichText([TelegramSegment("invited ")])
                for join in joined:
                    link = "https://t.me/{}".format(join.username) if join.username else None
                    text.append(TelegramSegment(join.real_name, bold=(not link), link=link),
                                TelegramSegment(", "))
                text = text[:-1]
        elif message["left_chat_member"]:
            if left == [user]:
                text = TelegramRichText([TelegramSegment("left group")])
            else:
                part = left[0]
                link = "https://t.me/{}".format(part.username) if part.username else None
                text = TelegramRichText([TelegramSegment("removed "),
                                         TelegramSegment(part.real_name,
                                                         bold=(not link), link=link)])
        if message["reply_to_message"]:
            reply_to = (await cls.from_message(telegram, message["reply_to_message"]))[1]
        if message["photo"]:
            # This is a list of resolutions, find the original sized one to return.
            photo = max(message["photo"], key=lambda photo: photo["height"])
            async with telegram._session.get("{}/getFile".format(telegram._base),
                                             params={"file_id": photo["file_id"]}) as resp:
                file_json = _Schema.api(await resp.json(), _Schema.file)
            url = ("https://api.telegram.org/file/bot{}/{}"
                   .format(telegram._token, file_json["result"]["file_path"]))
            attachments.append(imirror.File(type=imirror.File.Type.image, source=url))
        return (telegram.host.resolve_channel(telegram, message["chat"]["id"]),
                cls(id=message["message_id"],
                    at=datetime.fromtimestamp(message["date"]),
                    text=text,
                    user=user,
                    action=action,
                    reply_to=reply_to,
                    joined=joined,
                    left=left,
                    attachments=attachments,
                    raw=message))

    @classmethod
    async def from_update(cls, telegram, update):
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
                return await cls.from_message(telegram, update[key])
            elif update.get("edited_{}".format(key)):
                msg = await cls.from_message(telegram, update["edited_{}".format(key)])
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
            self._token = config["token"]
        except KeyError:
            raise imirror.ConfigError("Telegram token not specified") from None
        self._base = "https://api.telegram.org/bot{}".format(self._token)
        # Connection objects that need to be closed on disconnect.
        self._session = None
        # Update ID from which to retrieve the next batch.  Should be one higher than the max seen.
        self._offset = 0

    async def connect(self):
        await super().connect()
        self._session = ClientSession()

    async def disconnect(self):
        await super().disconnect()
        if self._session:
            log.debug("Closing session")
            await self._session.close()
            self._session = None
        self._offset = 0

    async def put(self, channel, msg):
        if msg.deleted:
            # TODO
            return
        text = None
        if msg.text:
            if isinstance(msg.text, imirror.RichText):
                rich = msg.text.clone()
            elif msg.text:
                # Unformatted text received, make a basic rich text instance out of it.
                rich = imirror.RichText([imirror.Segment(msg.text)])
            else:
                rich = imirror.RichText()
            if msg.user:
                name = msg.user.real_name or msg.user.username
                prefix = ("{} " if msg.action else "{}: ").format(name)
                rich.prepend(imirror.Segment(prefix, bold=True))
            if msg.action:
                for segment in rich:
                    segment.italic = True
            text = "".join(TelegramSegment.to_html(segment) for segment in rich)
        parts = []
        for attach in msg.attachments:
            if isinstance(attach, imirror.File) and attach.type == imirror.File.Type.image:
                # Upload an image file to Telegram in its own message.
                # Prefer a source URL if available, else fall back to re-uploading the file.
                data = FormData({"chat_id": str(channel.source)})
                if msg.user:
                    data.add_field("caption", "{} sent an image"
                                              .format(msg.user.real_name or msg.user.username))
                if attach.source:
                    data.add_field("photo", attach.source)
                else:
                    img_resp = await attach.get_content(self._session)
                    data.add_field("photo", img_resp.content, filename=attach.title or "photo")
                parts.append(("sendPhoto", data))
        if text:
            parts.append(("sendMessage", {"chat_id": channel.source,
                                          "text": text,
                                          "parse_mode": "HTML"}))
        sent = []
        for endpoint, data in parts:
            async with self._session.post("{}/{}".format(self._base, endpoint), data=data) as resp:
                json = _Schema.api(await resp.json(), _Schema.send)
                if not json["ok"]:
                    raise TelegramAPIError(json["description"], json["error_code"])
                sent.append(json["result"]["message_id"])
        return sent

    async def get(self):
        while True:
            log.debug("Making long-poll request")
            async with self._session.get("{}/getUpdates".format(self._base),
                                         params={"offset": self._offset or "",
                                                 "timeout": 240}) as resp:
                try:
                    resp.raise_for_status()
                except ClientResponseError:
                    log.debug("Unexpected response code: {}".format(resp.status))
                    await sleep(3)
                    continue
                json = _Schema.api(await resp.json(), [_Schema.update])
            if not json["ok"]:
                raise TelegramAPIError(json["description"], json["error_code"])
            for update in json["result"]:
                log.debug("Received a message")
                if any(key in update or "edited_{}".format(key) in update
                       for key in ("message", "channel_post")):
                    yield await TelegramMessage.from_update(self, update)
                self._offset = max(update["update_id"] + 1, self._offset)
