"""
Connect to `Telegram <https://telegram.org>`_ as a bot.

Config:
    token (str):
        Telegram bot token for the bot API.
    api-id (int):
        Optional Telegram application ID for the MTProto API.
    api-hash (str):
        Corresponding Telegram application secret.
    session (str):
        Optional path to store a session file, used to cache access hashes.

Bots can be created by talking to `@BotFather <https://t.me/BotFather>`_.  Use the ``/token``
command to retrieve your API token.  You should also call ``/setprivacy`` to grant the bot
permission to see all messages as they come in.

Telegram bots are rather limited by the bot API.  To make use of some features (including lookup of
arbitrary users and listing members in chats), you'll need to combine bot authorisation with an
MTProto application.  This is done via `app credentials <https://my.telegram.org/apps>`_ applied to
a bot session -- the bot gains some extra permissions from the "app", but accesses them itself.

Note that most objects in Telegram cannot be retrieved by ID until you've "seen" them via other
methods.  With a session file, you need only do this once, after which the reference to it (the
"access hash") is cached.

.. note::
    Use of app features requires the `telethon <https://telethon.readthedocs.io/en/latest/>`_
    Python module.
"""

from asyncio import CancelledError, TimeoutError, ensure_future, sleep
from collections import defaultdict
from datetime import datetime
import logging

from aiohttp import ClientError, ClientResponseError, ClientSession, FormData
from voluptuous import ALLOW_EXTRA, Any, Optional, Schema

import immp


try:
    from telethon import TelegramClient, tl
    from telethon.errors import BadRequestError
except ImportError:
    TelegramClient = tl = BadRequestError = None


log = logging.getLogger(__name__)


class _Schema:

    config = Schema({"token": str,
                     Optional("api-id", default=None): Any(int, None),
                     Optional("api-hash", default=None): Any(str, None),
                     Optional("session", default=None): Any(str, None)},
                    extra=ALLOW_EXTRA, required=True)

    user = Schema({"id": int,
                   Optional("username", default=None): Any(str, None),
                   "first_name": str,
                   Optional("last_name", default=None): Any(str, None)},
                  extra=ALLOW_EXTRA, required=True)

    channel = Schema({"id": int,
                      "title": str,
                      "type": "channel",
                      Optional("username", default=None): Any(str, None)},
                     extra=ALLOW_EXTRA, required=True)

    entity = Schema({"type": str,
                     "offset": int,
                     "length": int},
                    extra=ALLOW_EXTRA, required=True)

    message = Schema({"message_id": int,
                      "chat": {"id": int},
                      "date": int,
                      Optional("edit_date", default=None): Any(int, None),
                      Optional("from", default=None): Any(user, None),
                      Optional("forward_from", default=None): Any(user, None),
                      Optional("forward_date", default=None): Any(int, None),
                      Optional("forward_from_chat", default=None): Any(channel, None),
                      Optional("forward_from_message_id", default=None): Any(int, None),
                      Optional("text", default=None): Any(str, None),
                      Optional("caption", default=None): Any(str, None),
                      Optional("entities", default=[]): [entity],
                      Optional("caption_entities", default=[]): [entity],
                      Optional("reply_to_message", default=None):
                          Any(lambda v: _Schema.message(v), None),
                      Optional("photo", default=[]): [{"file_id": str}],
                      Optional("sticker", default=None):
                          Any({Optional("emoji", default=None): Any(str, None),
                               "file_id": str}, None),
                      Optional("location", default=None):
                          Any({"latitude": float, "longitude": float}, None),
                      Optional("group_chat_created", default=False): bool,
                      Optional("new_chat_members", default=[]): [user],
                      Optional("left_chat_member", default=None): Any(user, None),
                      Optional("new_chat_title", default=None): Any(str, None),
                      Optional("migrate_to_chat_id", default=None): Any(int, None)},
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
    def from_bot_channel(cls, telegram, json):
        """
        Convert a chat :class:`dict` (attached to a message) to a :class:`.User`.

        Args:
            telegram (.TelegramPlug):
                Related plug instance that provides the user.
            json (dict):
                Telegram API `Chat <https://core.telegram.org/bots/api#chat>`_ object.

        Returns:
            .TelegramUser:
                Parsed user object.
        """
        chat = _Schema.channel(json)
        return cls(id=chat["id"],
                   plug=telegram,
                   username=chat["username"],
                   real_name=chat["title"],
                   raw=chat)

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
        else:
            return "tg://user?id={}".format(self.id)


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
                text = ("<a href=\"tg://user?id={}\">{}</a>"
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
            start = entity["offset"] * 2
            end = start + (entity["length"] * 2)
            changes[start][entity["type"]] = True
            changes[end][entity["type"]] = False
        segments = []
        points = list(sorted(changes.keys()))
        formatting = {}
        # Telegram entities assume the text is UTF-16.
        encoded = text.encode("utf-16-le")
        # Iterate through text in change start/end pairs.
        for start, end in zip([0] + points, points + [len(encoded)]):
            formatting.update(changes[start])
            if start == end:
                # Zero-length segment at the start or end, ignore it.
                continue
            segments.append(TelegramSegment(encoded[start:end].decode("utf-16-le"), **formatting))
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
        # Message IDs are just a sequence, only unique to their channel and not the whole network.
        # Pair with the chat ID for a network-unique value.
        id = "{}:{}".format(message["chat"]["id"], message["message_id"])
        revision = message["edit_date"] or message["date"]
        at = datetime.fromtimestamp(message["date"])
        channel = immp.Channel(telegram, message["chat"]["id"])
        edited = bool(message["edit_date"])
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
        if message["reply_to_message"]:
            reply_to = await cls.from_message(telegram, message["reply_to_message"])
        # At most one of these fields will be set.
        if message["text"]:
            text = TelegramRichText.from_entities(message["text"], message["entities"])
        elif message["group_chat_created"]:
            action = True
            text = TelegramRichText([TelegramSegment("created the group "),
                                     TelegramSegment(message["chat"]["title"], bold=True)])
        elif message["new_chat_title"]:
            title = message["new_chat_title"]
            action = True
            text = TelegramRichText([TelegramSegment("changed group name to "),
                                     TelegramSegment(message["new_chat_title"], bold=True)])
        elif message["new_chat_members"]:
            joined = [(TelegramUser.from_bot_user(telegram, member))
                      for member in message["new_chat_members"]]
            action = True
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
            left = [TelegramUser.from_bot_user(telegram, message["left_chat_member"])]
            action = True
            if left == [user]:
                text = TelegramRichText([TelegramSegment("left group")])
            else:
                part = left[0]
                link = "https://t.me/{}".format(part.username) if part.username else None
                text = TelegramRichText([TelegramSegment("removed "),
                                         TelegramSegment(part.real_name,
                                                         bold=(not link), link=link)])
        elif message["photo"]:
            # This is a list of resolutions, find the original sized one to return.
            photo = max(message["photo"], key=lambda photo: photo["height"])
            params = {"file_id": photo["file_id"]}
            file = await telegram._api("getFile", _Schema.file, params=params)
            url = ("https://api.telegram.org/file/bot{}/{}"
                   .format(telegram.config["token"], file["file_path"]))
            attachments.append(immp.File(type=immp.File.Type.image, source=url))
            if message["caption"]:
                text = TelegramRichText.from_entities(message["caption"],
                                                      message["caption_entities"])
        elif message["sticker"]:
            params = {"file_id": message["sticker"]["file_id"]}
            file = await telegram._api("getFile", _Schema.file, params=params)
            url = ("https://api.telegram.org/file/bot{}/{}"
                   .format(telegram.config["token"], file["file_path"]))
            attachments.append(immp.File(type=immp.File.Type.image, source=url))
            # All real stickers should have an emoji, but webp images uploaded as photos are
            # incorrectly categorised as stickers in the API response.
            if not text and message["sticker"]["emoji"]:
                action = True
                text = "sent {} sticker".format(message["sticker"]["emoji"])
        elif message["location"]:
            attachments.append(immp.Location(latitude=message["location"]["latitude"],
                                             longitude=message["location"]["longitude"]))
        else:
            # No support for this message type.
            raise NotImplementedError
        common = dict(id=id,
                      revision=revision,
                      at=at,
                      channel=channel,
                      edited=edited,
                      user=user,
                      raw=message)
        if message["forward_date"]:
            # Event is a message containing another message.  Forwarded messages have no ID, so we
            # use a Message instead of a SentMessage here, unless they come from a channel.
            forward_id = forward_channel = forward_user = None
            if message["forward_from"]:
                forward_user = TelegramUser.from_bot_user(telegram, message["forward_from"])
            elif message["forward_from_chat"]:
                forward_channel = immp.Channel(telegram, message["forward_from_chat"]["id"])
                forward_user = TelegramUser.from_bot_channel(telegram, message["forward_from_chat"])
                if message["forward_from_message_id"]:
                    forward_id = "{}:{}".format(message["forward_from_chat"]["id"],
                                                message["forward_from_message_id"])
            forward_common = dict(text=text,
                                  user=forward_user,
                                  action=action,
                                  reply_to=reply_to,
                                  joined=joined,
                                  left=left,
                                  title=title,
                                  attachments=attachments,
                                  raw=message)
            if forward_id:
                forward = immp.SentMessage(id=forward_id,
                                           channel=forward_channel,
                                           **forward_common)
            else:
                forward = immp.Message(**forward_common)
            # Embed the inner message as an attachment.
            return immp.SentMessage(attachments=[forward], **common)
        else:
            return immp.SentMessage(text=text,
                                    action=action,
                                    reply_to=reply_to,
                                    joined=joined,
                                    left=left,
                                    title=title,
                                    attachments=attachments,
                                    **common)

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
                return await cls.from_message(telegram, update["edited_{}".format(key)])


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
        # Temporary tracking of migrated chats for the current session.
        self._migrations = {}
        # Update ID from which to retrieve the next batch.  Should be one higher than the max seen.
        self._offset = 0

    async def _api(self, endpoint, schema=_Schema.api(), **kwargs):
        url = "https://api.telegram.org/bot{}/{}".format(self.config["token"], endpoint)
        log.debug("Making API request to %r", endpoint)
        try:
            async with self._session.post(url, **kwargs) as resp:
                try:
                    json = await resp.json()
                    data = schema(json)
                except ClientResponseError as e:
                    raise TelegramAPIError("Bad response with code: {}".format(resp.status)) from e
        except ClientError as e:
            raise TelegramAPIError("Request failed") from e
        except TimeoutError as e:
            raise TelegramAPIError("Request timed out") from e
        if not data["ok"]:
            raise TelegramAPIError(data["error_code"], data["description"])
        return data["result"]

    async def start(self):
        await super().start()
        self._closing = False
        self._session = ClientSession()
        self._bot_user = await self._api("getMe", _Schema.me)
        self._receive = ensure_future(self._poll())
        if self.config["api-id"]:
            log.debug("Starting client")
            self._client = TelegramClient(self.config["session"], self.config["api-id"],
                                          self.config["api-hash"])
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
            await self._client.disconnect()
            self._client = None
        self._bot_user = None
        self._offset = 0
        if self._migrations:
            log.warning("Chat migrations require a config update before next run")

    async def user_from_id(self, id):
        if not self._client:
            log.debug("Client auth required to look up users")
            return None
        try:
            data = await self._client(tl.functions.users.GetFullUserRequest(int(id)))
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

    async def user_is_system(self, user):
        return user.id == str(self._bot_user["id"])

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

    async def channel_rename(self, channel, title):
        await self._api("setChatTitle", params={"chat_id": channel.source, "title": title})

    async def channel_members(self, channel):
        if not self._client:
            log.debug("Client auth required to list channel members")
            return None
        # Channel and supergroup chat IDs have a bot-API-only prefix to distinguish them.
        if channel.source.startswith("-100"):
            chat = int(channel.source[4:])
            users = []
            try:
                while True:
                    data = await self._client(tl.functions.channels.GetParticipantsRequest(
                        chat, tl.types.ChannelParticipantsRecent(), len(users), 1000, 0))
                    if data.users:
                        users += [TelegramUser.from_proto_user(self, user) for user in data.users]
                    else:
                        break
            except BadRequestError:
                return None
            else:
                return users
        else:
            chat = abs(int(channel.source))
            try:
                data = await self._client(tl.functions.messages.GetFullChatRequest(chat))
            except BadRequestError:
                return None
            else:
                return [TelegramUser.from_proto_user(self, user) for user in data.users]

    async def channel_remove(self, channel, user):
        await self._api("kickChatMember", params={"chat_id": channel.source, "user_id": user.id})

    async def _form_data(self, base, field, attach):
        data = FormData(base)
        if attach.source:
            data.add_field(field, attach.source)
        else:
            img_resp = await attach.get_content(self._session)
            data.add_field(field, img_resp.content, filename=attach.title or field)
        return data

    async def _upload_attachment(self, chat, msg, attach):
        # Upload an image file to Telegram in its own message.
        # Prefer a source URL if available, else fall back to re-uploading the file.
        base = {"chat_id": str(chat)}
        if msg.user:
            rich = immp.RichText([immp.Segment(msg.user.real_name or msg.user.username,
                                               bold=True, italic=True),
                                  immp.Segment(" sent an image", italic=True)])
            text = "".join(TelegramSegment.to_html(self, segment) for segment in rich)
            base["caption"] = text
            base["parse_mode"] = "HTML"
        if attach.type == immp.File.Type.image:
            data = await self._form_data(base, "photo", attach)
            try:
                return await self._api("sendPhoto", _Schema.send, data=data)
            except TelegramAPIError:
                log.debug("Failed to upload image, falling back to document upload")
        data = await self._form_data(base, "document", attach)
        try:
            return await self._api("sendDocument", _Schema.send, data=data)
        except TelegramAPIError:
            log.exception("Failed to upload file")
            return None

    def _requests(self, chat, msg):
        requests = []
        if msg.text or msg.reply_to:
            quote = False
            reply_to = ""
            if isinstance(msg.reply_to, immp.SentMessage):
                # Reply natively to the given parent message.
                reply_to = int(msg.reply_to.id.split(":")[1])
            elif isinstance(msg.reply_to, immp.Message):
                quote = True
            edited = msg.edited if isinstance(msg, immp.SentMessage) else False
            rich = msg.render(edit=edited, quote_reply=quote)
            text = "".join(TelegramSegment.to_html(self, segment) for segment in rich)
            requests.append(self._api("sendMessage", _Schema.send,
                                      params={"chat_id": chat,
                                              "text": text,
                                              "parse_mode": "HTML",
                                              "reply_to_message_id": reply_to}))
        for attach in msg.attachments:
            if isinstance(attach, immp.File):
                requests.append(self._upload_attachment(chat, msg, attach))
            elif isinstance(attach, immp.Location):
                requests.append(self._api("sendLocation", _Schema.send,
                                          params={"chat_id": chat,
                                                  "latitude": str(attach.latitude),
                                                  "longitude": str(attach.longitude)}))
                if msg.user:
                    caption = immp.Message(user=msg.user, text="sent a location", action=True)
                    text = "".join(TelegramSegment.to_html(self, segment)
                                   for segment in caption.render())
                    requests.append(self._api("sendMessage", _Schema.send,
                                              params={"chat_id": chat,
                                                      "text": text,
                                                      "parse_mode": "HTML"}))
        return requests

    async def put(self, channel, msg):
        chat = channel.source
        while chat in self._migrations:
            log.debug("Following chat migration: %r -> %r", chat, self._migrations[chat])
            chat = self._migrations[chat]
        requests = []
        for attach in msg.attachments:
            # Generate requests for attached messages first.
            if isinstance(attach, immp.SentMessage):
                # Forward the messages natively using the given chat/ID.
                forward_chat, forward_id = map(int, attach.id.split(":"))
                requests.append(self._api("forwardMessage", _Schema.send,
                                          params={"chat_id": chat,
                                                  "from_chat_id": forward_chat,
                                                  "message_id": forward_id}))
            elif isinstance(attach, immp.Message):
                requests += self._requests(chat, attach)
        own_requests = self._requests(chat, msg)
        if requests and not own_requests:
            # Forwarding a message but no content to show who forwarded it.
            info = immp.Message(user=msg.user, action=True, text="forwarded a message")
            own_requests = self._requests(chat, info)
        requests += own_requests
        ids = []
        for request in requests:
            result = await request
            if not result:
                continue
            sent = await TelegramMessage.from_message(self, result)
            self.queue(sent)
            ids.append(sent.id)
        return ids

    async def delete(self, sent):
        chat, message = sent.id.split(":", 1)
        await self._api("deleteMessage", params={"chat_id": chat, "message_id": message})

    async def _poll(self):
        while not self._closing:
            params = {"offset": self._offset,
                      "timeout": 240}
            fetch = ensure_future(self._api("getUpdates", _Schema.updates, params=params))
            try:
                result = await fetch
            except CancelledError:
                log.debug("Cancelling polling")
                return
            except TelegramAPIError as e:
                log.debug("Unexpected response or timeout: %r", e)
                log.debug("Reconnecting in 3 seconds")
                await sleep(3)
                continue
            except Exception as e:
                log.exception("Uncaught exception during long-poll: %r", e)
                raise
            for update in result:
                log.debug("Received a message")
                if "message" in update and update["message"]["migrate_to_chat_id"]:
                    old = update["message"]["chat"]["id"]
                    new = update["message"]["migrate_to_chat_id"]
                    log.warning("Chat has migrated: %r -> %r", old, new)
                    self._migrations[old] = new
                    for name, channel in self.host.channels.items():
                        if channel.plug is self and channel.source == old:
                            log.debug("Updating named channel %r in place", name)
                            channel.source = new
                if any(key in update or "edited_{}".format(key) in update
                       for key in ("message", "channel_post")):
                    try:
                        sent = await TelegramMessage.from_update(self, update)
                    except NotImplementedError:
                        log.debug("Skipping message with no usable parts")
                    except CancelledError:
                        log.debug("Cancel request for plug %r getter", self.name)
                        return
                    else:
                        self.queue(sent)
                self._offset = max(update["update_id"] + 1, self._offset)
