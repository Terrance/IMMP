"""
Connect to `Telegram <https://telegram.org>`_ as a bot.

Config:
    token (str):
        Telegram bot token for the bot API.
    api-id (int):
        Optional Telegram application ID for the MTProto API.
    api-hash (str):
        Corresponding Telegram application secret.

Bots can be created by talking to `@BotFather <https://t.me/BotFather>`_.  Use the ``/token``
command to retrieve your API token.  You should also call ``/setprivacy`` to grant the bot
permission to see all messages as they come in.

Telegram bots are rather limited by the bot API.  To make use of some features (including lookup of
arbitrary users and listing members in chats), you'll need to combine bot authorisation with an
MTProto application.  This is done via `app credentials <https://my.telegram.org/apps>`_ applied to
a bot session -- the bot gains some extra permissions from the "app", but accesses them itself.

.. note::
    Use of app features requires the `telethon-aio <https://telethon.readthedocs.io/en/asyncio/>`_
    Python module.
"""

from asyncio import CancelledError, ensure_future, sleep
from collections import defaultdict
from datetime import datetime
import logging

from aiohttp import ClientError, ClientResponseError, ClientSession, FormData
from voluptuous import ALLOW_EXTRA, Any, Optional, Schema

try:
    from telethon import TelegramClient, tl
    from telethon.errors import BadRequestError
except ImportError:
    TelegramClient = tl = BadRequestError = None

import immp


log = logging.getLogger(__name__)


class _Schema:

    config = Schema({"token": str,
                     Optional("api-id", default=None): Any(int, None),
                     Optional("api-hash", default=None): Any(str, None)},
                    extra=ALLOW_EXTRA, required=True)

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
                      Optional("location", default=None):
                          Any({"latitude": float, "longitude": float}, None),
                      Optional("new_chat_members", default=[]): [user],
                      Optional("left_chat_member", default=None): Any(user, None),
                      Optional("new_chat_title", default=None): Any(str, None)},
                     extra=ALLOW_EXTRA, required=True)

    update = Schema({"update_id": int,
                     Optional(Any("message", "edited_message",
                                  "channel_post", "edited_channel_post")): message},
                    extra=ALLOW_EXTRA, required=True)

    def api(result=None):
        success = {"ok": True}
        if result:
            success["result"] = result
        return Schema(Any(success,
                          {"ok": False,
                           "description": str,
                           "error_code": int}),
                      extra=ALLOW_EXTRA, required=True)

    me = api(user)

    file = api({"file_path": str})

    send = api(message)

    chat = api({"type": str,
                Optional("title", default=None): Any(str, None)})

    updates = api([update])


class TelegramAPIError(immp.PlugError):
    """
    Generic error from the Telegram API.
    """


class TelegramUser(immp.User):
    """
    User present in Telegram.
    """

    @classmethod
    def from_bot_user(cls, telegram, json):
        """
        Convert a user :class:`dict` (attached to a message) to a :class:`.User`.

        Args:
            telegram (.TelegramPlug):
                Related plug instance that provides the user.
            json (dict):
                Telegram API `User <https://core.telegram.org/bots/api#user>`_ object.

        Returns:
            .TelegramUser:
                Parsed user object.
        """
        user = _Schema.user(json)
        real_name = user["first_name"]
        if user["last_name"]:
            real_name = "{} {}".format(real_name, user["last_name"])
        avatar = None
        if user["username"]:
            avatar = "https://t.me/i/userpic/320/{}.jpg".format(user["username"])
        return cls(id=user["id"],
                   plug=telegram,
                   username=user["username"],
                   real_name=real_name,
                   avatar=avatar,
                   raw=user)

    @classmethod
    def from_proto_user(cls, telegram, user):
        """
        Convert a :class:`telethon.tl.types.User` into a :class:`.User`.

        Args:
            telegram (.TelegramPlug):
                Related plug instance that provides the user.
            user (telethon.tl.types.User):
                Telegram user retrieved from the MTProto API.

        Returns:
            .TelegramUser:
                Parsed user object.
        """
        real_name = user.first_name
        if user.last_name:
            real_name = "{} {}".format(real_name, user.last_name)
        avatar = None
        if user.username:
            avatar = "https://t.me/i/userpic/320/{}.jpg".format(user.username)
        return cls(id=user.id,
                   plug=telegram,
                   username=user.username,
                   real_name=real_name,
                   avatar=avatar,
                   raw=user)

    @property
    def link(self):
        if self.username:
            return "https://t.me/{}".format(self.username)


class TelegramSegment(immp.Segment):
    """
    Plug-friendly representation of Telegram message formatting.
    """

    @classmethod
    def to_html(cls, telegram, segment):
        """
        Convert a :class:`.Segment` into HTML suitable for Telegram's automatic parsing.

        Args:
            telegram (.TelegramPlug):
                Related plug instance to cross-reference users.
            segment (.Segment)
                Message segment created by another plug.

        Returns:
            str:
                HTML-formatted string.
        """
        text = segment.text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        # Any form of tag nesting (e.g. bold inside italic) isn't supported, so at most one type of
        # formatting may apply for each segment.
        if segment.mention and isinstance(segment.mention.plug, TelegramPlug):
            if segment.mention.username:
                # Telegram will parse this automatically.
                text = "@{}".format(segment.mention.username)
            else:
                # Make a link that looks like a mention.
                text = ("<a href=\"tg://user?id={}\">@{}</a>"
                        .format(segment.mention.id, segment.mention.real_name))
        elif segment.link:
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


class TelegramRichText(immp.RichText):
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


class TelegramMessage(immp.Message):
    """
    Message originating from Telegram.
    """

    @classmethod
    async def from_message(cls, telegram, json):
        """
        Convert an API message :class:`dict` to a :class:`.Message`.

        Args:
            telegram (.TelegramPlug):
                Related plug instance that provides the event.
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
        title = None
        attachments = []
        if message["from"]:
            user = TelegramUser.from_bot_user(telegram, message["from"])
        if message["new_chat_title"]:
            title = message["new_chat_title"]
            action = True
        if message["new_chat_members"]:
            joined = [(TelegramUser.from_bot_user(telegram, member))
                      for member in message["new_chat_members"]]
            action = True
        if message["left_chat_member"]:
            left = [TelegramUser.from_bot_user(telegram, message["left_chat_member"])]
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
            params = {"file_id": photo["file_id"]}
            file = await telegram._api("getFile", _Schema.file, params=params)
            url = ("https://api.telegram.org/file/bot{}/{}"
                   .format(telegram.config["token"], file["file_path"]))
            attachments.append(immp.File(type=immp.File.Type.image, source=url))
        if message["location"]:
            attachments.append(immp.Location(latitude=message["location"]["latitude"],
                                             longitude=message["location"]["longitude"]))
        return (immp.Channel(telegram, message["chat"]["id"]),
                cls(id=message["message_id"],
                    at=datetime.fromtimestamp(message["date"]),
                    text=text,
                    user=user,
                    action=action,
                    reply_to=reply_to,
                    joined=joined,
                    left=left,
                    title=title,
                    attachments=attachments,
                    raw=message))

    @classmethod
    async def from_update(cls, telegram, update):
        """
        Convert an API update :class:`dict` to a :class:`.Message`.

        Args:
            telegram (.TelegramPlug):
                Related plug instance that provides the event.
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
                channel, msg = await cls.from_message(telegram, update["edited_{}".format(key)])
                # Messages are edited in-place, no new ID is issued.
                msg.original = msg.id
                return (channel, msg)


class TelegramPlug(immp.Plug):
    """
    Plug for a `Telegram <https://telegram.org>`_ bot.
    """

    network_name = "Telegram"

    @property
    def network_id(self):
        return "telegram:{}".format(self._bot_user["id"]) if self._bot_user else None

    def __init__(self, name, config, host):
        super().__init__(name, _Schema.config(config), host)
        if self.config["api-id"] and self.config["api-hash"]:
            if not TelegramClient:
                raise immp.ConfigError("API ID/hash specified but Telethon is not installed")
        elif self.config["api-id"] or self.config["api-hash"]:
            raise immp.ConfigError("Both of API ID and hash must be given")
        # Connection objects that need to be closed on disconnect.
        self._session = self._bot_user = self._receive = self._client = None
        self._closing = False
        # Update ID from which to retrieve the next batch.  Should be one higher than the max seen.
        self._offset = 0

    async def _api(self, endpoint, schema=_Schema.api(), **kwargs):
        url = "https://api.telegram.org/bot{}/{}".format(self.config["token"], endpoint)
        try:
            async with self._session.post(url, **kwargs) as resp:
                try:
                    resp.raise_for_status()
                except ClientResponseError as e:
                    raise TelegramAPIError("Bad response code: {}".format(resp.status)) from e
                else:
                    json = await resp.json()
        except ClientError as e:
            raise TelegramAPIError("Request failed") from e
        data = schema(json)
        if not data["ok"]:
            raise TelegramAPIError(data["description"], data["error_code"])
        return data["result"]

    async def start(self):
        await super().start()
        self._closing = False
        self._session = ClientSession()
        self._bot_user = await self._api("getMe", _Schema.me)
        self._receive = ensure_future(self._poll())
        if self.config["api-id"]:
            log.debug("Starting client")
            self._client = TelegramClient(None, self.config["api-id"], self.config["api-hash"])
            await self._client.start(bot_token=self.config["token"])

    async def stop(self):
        await super().stop()
        self._closing = True
        if self._receive:
            self._receive.cancel()
            self._receive = None
        if self._session:
            log.debug("Closing session")
            await self._session.close()
            self._session = None
        if self._client:
            log.debug("Closing client")
            await self._client.log_out()
            self._client.disconnect()
            self._client = None
        self._bot_user = None
        self._offset = 0

    async def user_from_id(self, id):
        if not self._client:
            log.debug("Client auth required to look up users")
            return None
        try:
            data = await self._client(tl.functions.users.GetFullUserRequest(id))
        except BadRequestError:
            return None
        else:
            return TelegramUser.from_proto_user(self, data.user)

    async def user_from_username(self, username):
        if not self._client:
            log.debug("Client auth required to look up users")
            return None
        try:
            data = await self._client(tl.functions.contacts.ResolveUsernameRequest(username))
        except BadRequestError:
            return None
        else:
            return TelegramUser.from_proto_user(self, data.users[0])

    async def channel_for_user(self, user):
        if not isinstance(user, TelegramUser):
            return None
        try:
            await self._api("getChat", params={"chat_id": user.id})
        except TelegramAPIError:
            # Can't create private channels, users must initiate conversations with bots.
            return None
        else:
            return immp.Channel(self, user.id)

    async def channel_is_private(self, channel):
        try:
            data = await self._api("getChat", _Schema.chat, params={"chat_id": channel.source})
        except TelegramAPIError:
            return None
        else:
            return data["type"] == "private"

    async def channel_title(self, channel):
        try:
            data = await self._api("getChat", _Schema.chat, params={"chat_id": channel.source})
        except TelegramAPIError:
            return None
        else:
            return data["title"]

    async def channel_members(self, channel):
        if not self._client:
            log.debug("Client auth required to list channel members")
            return None
        try:
            # Chat IDs can be negative in the bot API dependent on type, not over MTProto.
            data = await self._client(tl.functions.messages.GetFullChatRequest(abs(channel.source)))
        except BadRequestError:
            return None
        else:
            return [TelegramUser.from_proto_user(self, user) for user in data.users]

    async def channel_remove(self, channel, user):
        data = {"chat_id": channel.source,
                "user_id": user.id}
        await self._api("kickChatMember", params=data)

    async def put(self, channel, msg):
        if msg.deleted:
            # TODO
            return []
        parts = []
        if msg.text or msg.reply_to:
            rich = msg.render(quote_reply=True)
            text = "".join(TelegramSegment.to_html(self, segment) for segment in rich)
            parts.append(("sendMessage", {"chat_id": channel.source,
                                          "text": text,
                                          "parse_mode": "HTML"}))
        for attach in msg.attachments:
            if isinstance(attach, immp.File) and attach.type == immp.File.Type.image:
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
            elif isinstance(attach, immp.Location):
                parts.append(("sendLocation", {"chat_id": channel.source,
                                               "latitude": attach.latitude,
                                               "longitude": attach.longitude}))
                if msg.user:
                    caption = immp.Message(user=msg.user, text="sent a location", action=True)
                    text = "".join(TelegramSegment.to_html(self, segment)
                                   for segment in caption.render())
                    parts.append(("sendMessage", {"chat_id": channel.source,
                                                  "text": text,
                                                  "parse_mode": "HTML"}))
        sent = []
        for endpoint, data in parts:
            result = await self._api(endpoint, _Schema.send, data=data)
            ext_channel, ext_msg = await TelegramMessage.from_message(self, result)
            self.queue(ext_channel, ext_msg)
            sent.append(result["message_id"])
        return sent

    async def _poll(self):
        while not self._closing:
            log.debug("Making long-poll request")
            params = {"offset": self._offset,
                      "timeout": 240}
            fetch = ensure_future(self._api("getUpdates", _Schema.updates, params=params))
            try:
                result = await fetch
            except CancelledError:
                log.debug("Cancel request for plug '{}' getter".format(self.name))
                return
            except TelegramAPIError as e:
                log.debug("Unexpected response or timeout: {}".format(e))
                log.debug("Reconnecting in 3 seconds")
                await sleep(3)
                continue
            for update in result:
                log.debug("Received a message")
                if any(key in update or "edited_{}".format(key) in update
                       for key in ("message", "channel_post")):
                    try:
                        channel, msg = await TelegramMessage.from_update(self, update)
                    except CancelledError:
                        log.debug("Cancel request for plug '{}' getter".format(self.name))
                        return
                    self.queue(channel, msg)
                self._offset = max(update["update_id"] + 1, self._offset)
