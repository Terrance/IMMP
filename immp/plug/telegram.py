"""
Connect to `Telegram <https://telegram.org>`_ as a bot.

Config:
    token (str):
        Telegram bot token for the bot API.
    api-id (int):
        Optional Telegram application ID for the MTProto API.
    api-hash (str):
        Corresponding Telegram application secret.
    client-updates (bool):
        ``True`` (when API credentials are configured) to listen for messages over the MTProto
        connection instead of long-polling with the bot API.

        This allows you to maintain multiple connections with a single bot account, however it will
        not mark messages as read within the bot API.  This means if you switch back to the bot API
        at a later date, you'll receive a backlog of any messages not yet picked up.
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

from asyncio import CancelledError, TimeoutError, ensure_future, gather, sleep
from collections import defaultdict
from datetime import datetime, timezone
import logging

from aiohttp import ClientError, ClientResponseError, FormData

import immp


try:
    from telethon import TelegramClient, events, tl
    from telethon.errors import BadRequestError
    from telethon.sessions import SQLiteSession
    from telethon.utils import pack_bot_file_id
except ImportError:
    TelegramClient = SQLiteSession = None


log = logging.getLogger(__name__)


class _Schema:

    config = immp.Schema({"token": str,
                          immp.Optional("api-id"): immp.Nullable(int),
                          immp.Optional("api-hash"): immp.Nullable(str),
                          immp.Optional("client-updates", False): bool,
                          immp.Optional("session"): immp.Nullable(str)})

    user = immp.Schema({"id": int,
                        immp.Optional("username"): immp.Nullable(str),
                        "first_name": str,
                        immp.Optional("last_name"): immp.Nullable(str)})

    channel = immp.Schema({"id": int,
                           "title": str,
                           "type": "channel",
                           immp.Optional("username"): immp.Nullable(str)})

    entity = immp.Schema({"type": str,
                          "offset": int,
                          "length": int,
                          immp.Optional("url"): immp.Nullable(str),
                          immp.Optional("user"): immp.Nullable(user)})

    _file = {"file_id": str, immp.Optional("file_name"): immp.Nullable(str)}

    _location = {"latitude": float, "longitude": float}

    message = immp.Schema({"message_id": int,
                           "chat": {"id": int},
                           "date": int,
                           immp.Optional("edit_date"): immp.Nullable(int),
                           immp.Optional("from"): immp.Nullable(user),
                           immp.Optional("forward_from"): immp.Nullable(user),
                           immp.Optional("forward_date"): immp.Nullable(int),
                           immp.Optional("forward_from_chat"): immp.Nullable(channel),
                           immp.Optional("forward_from_message_id"): immp.Nullable(int),
                           immp.Optional("forward_signature"): immp.Nullable(str),
                           immp.Optional("text"): immp.Nullable(str),
                           immp.Optional("caption"): immp.Nullable(str),
                           immp.Optional("entities", list): [entity],
                           immp.Optional("caption_entities", list): [entity],
                           immp.Optional("photo", list): [_file],
                           immp.Optional("sticker"): immp.Nullable({immp.Optional("emoji"):
                                                                        immp.Nullable(str),
                                                                    "file_id": str}),
                           immp.Optional("animation"): immp.Nullable(_file),
                           immp.Optional("video"): immp.Nullable(_file),
                           immp.Optional("video_note"): immp.Nullable(_file),
                           immp.Optional("audio"): immp.Nullable(_file),
                           immp.Optional("voice"): immp.Nullable(_file),
                           immp.Optional("document"): immp.Nullable(_file),
                           immp.Optional("location"): immp.Nullable(_location),
                           immp.Optional("venue"): immp.Nullable({"location": _location,
                                                                  "title": str,
                                                                  "address": str}),
                           immp.Optional("poll"): immp.Nullable({"question": str,
                                                                 "is_closed": bool}),
                           immp.Optional("group_chat_created", False): bool,
                           immp.Optional("new_chat_members", list): [user],
                           immp.Optional("left_chat_member"): immp.Nullable(user),
                           immp.Optional("new_chat_title"): immp.Nullable(str),
                           immp.Optional("new_chat_photo", list): [_file],
                           immp.Optional("delete_chat_photo", False): bool,
                           immp.Optional("migrate_to_chat_id"): immp.Nullable(int)})

    # Circular references to embedded messages.
    message.raw.update({immp.Optional("reply_to_message"): immp.Nullable(message),
                        immp.Optional("pinned_message"): immp.Nullable(message)})

    update = immp.Schema({"update_id": int,
                          immp.Optional(immp.Any("message", "edited_message",
                                                 "channel_post", "edited_channel_post")): message})

    def api(result=None):
        success = {"ok": True}
        if result:
            success["result"] = result
        return immp.Schema(immp.Any(success,
                                    {"ok": False,
                                     "description": str,
                                     "error_code": int}))

    me = api(user)

    file = api({"file_path": str})

    send = api(message)

    chat = api({"type": str,
                immp.Optional("title"): immp.Nullable(str)})

    updates = api([update])


class TelegramAPIConnectError(immp.PlugError):
    """
    Generic error whilst attempting to call the Telegram API.
    """


class TelegramAPIRequestError(immp.PlugError):
    """
    Generic error from the Telegram API.
    """


class _HiddenSender(Exception):
    pass


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
        return cls(id_=user["id"],
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
        if chat["id"] == -1001228946795:
            raise _HiddenSender
        return cls(id_=chat["id"],
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
        if user.username and user.photo:
            avatar = "https://t.me/i/userpic/320/{}.jpg".format(user.username)
        return cls(id_=user.id,
                   plug=telegram,
                   username=user.username,
                   real_name=real_name,
                   avatar=avatar,
                   raw=user)

    @classmethod
    def from_entity(cls, telegram, entity):
        """
        Convert a client entity row into a :class:`.User`.

        Args:
            telegram (.TelegramPlug):
                Related plug instance that provides the user.
            entity ((str, str, str) tuple):
                ID, username and real name of a cached Telegram user.

        Returns:
            .TelegramUser:
                Parsed user object.
        """
        id_, username, name = entity
        return cls(id_=id_,
                   plug=telegram,
                   username=username,
                   real_name=name,
                   raw=entity)

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
        if segment.mention and segment.mention.plug.network_id == telegram.network_id:
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
    def _from_changes(cls, text, changes):
        segments = []
        points = list(sorted(changes.keys()))
        formatting = {}
        # Iterate through text in change start/end pairs.
        for start, end in zip([0] + points, points + [len(text)]):
            formatting.update(changes[start])
            if start == end:
                # Zero-length segment at the start or end, ignore it.
                continue
            segments.append(immp.Segment(text[start:end], **formatting))
        return cls(segments)

    @classmethod
    async def from_bot_entities(cls, telegram, text, entities):
        """
        Convert a string annotated by Telegram's entities to :class:`.RichText`.

        Args:
            telegram (.TelegramPlug):
                Related plug instance that provides the text.
            text (str):
                Plain text without formatting.
            entities (dict list):
                List of Telegram API `MessageEntity
                <https://core.telegram.org/bots/api#messageentity>`_ objects.

        Returns:
            .TelegramRichText:
                Parsed rich text container.
        """
        if not text:
            return None
        elif not entities:
            return immp.RichText([immp.Segment(text)])
        # Telegram entities assume the text is UTF-16.
        encoded = text.encode("utf-16-le")
        changes = defaultdict(dict)
        for json in entities:
            entity = _Schema.entity(json)
            start = entity["offset"] * 2
            end = start + (entity["length"] * 2)
            if entity["type"] in ("bold", "italic", "code", "pre"):
                key = entity["type"]
                value, clear = True, False
            elif entity["type"] in ("url", "email"):
                key = "link"
                value, clear = encoded[start:end].decode("utf-16-le"), None
            elif entity["type"] == "text_url":
                key = "link"
                value, clear = entity["url"], None
            elif entity["type"] == "mention":
                key = "mention"
                username = encoded[start + 2:end].decode("utf-16-le")
                value, clear = await telegram.user_from_username(username), None
            elif entity["type"] == "text_mention":
                key = "mention"
                value, clear = TelegramUser.from_bot_user(telegram, entity["user"]), None
            else:
                continue
            changes[start][key] = value
            changes[end][key] = clear
        rich = cls._from_changes(encoded, changes)
        for segment in rich:
            segment.text = segment.text.decode("utf-16-le")
        return rich

    @classmethod
    async def from_proto_entities(cls, telegram, text, entities):
        """
        Convert a string annotated by Telegram's entities to :class:`.RichText`.

        Args:
            telegram (.TelegramPlug):
                Related plug instance that provides the text.
            text (str):
                Plain text without formatting.
            entities (telethon.types.TypeMessageEntity list):
                List of Telegram entity objects.

        Returns:
            .TelegramRichText:
                Parsed rich text container.
        """
        if not text:
            return None
        elif not entities:
            return immp.RichText([immp.Segment(text)])
        changes = defaultdict(dict)
        for entity in entities:
            value, clear = True, False
            if isinstance(entity, tl.types.MessageEntityBold):
                key = "bold"
            elif isinstance(entity, tl.types.MessageEntityItalic):
                key = "italic"
            elif isinstance(entity, tl.types.MessageEntityCode):
                key = "code"
            elif isinstance(entity, tl.types.MessageEntityPre):
                key = "pre"
            elif isinstance(entity, (tl.types.MessageEntityEmail, tl.types.MessageEntityUrl)):
                key = "link"
                value, clear = text[entity.offset:entity.offset + entity.length], None
            elif isinstance(entity, tl.types.MessageEntityTextUrl):
                key = "link"
                value, clear = entity.url, None
            elif isinstance(entity, tl.types.MessageEntityMention):
                key = "mention"
                username = text[entity.offset + 1:entity.offset + entity.length]
                value, clear = await telegram.user_from_username(username), None
            elif isinstance(entity, tl.types.MessageEntityMentionName):
                key = "mention"
                value, clear = await telegram.user_from_id(entity.user_id), None
            else:
                continue
            changes[entity.offset][key] = value
            changes[entity.offset + entity.length][key] = clear
        return cls._from_changes(text, changes)


class TelegramFile(immp.File):
    """
    File attachment originating from Telegram.
    """

    @classmethod
    async def from_id(cls, telegram, id_, type_=immp.File.Type.unknown, name=None):
        """
        Generate a file using the bot API URL for a Telegram file.

        Args:
            telegram (.TelegramPlug):
                Related plug instance that provides the file.
            id (str):
                File ID as provided in the bot API, or constructed from a raw MTProto file.
            type (.File.Type):
                Corresponding file type.
            name (str):
                Original filename, if available for the file format.

        Returns:
            .TelegramFile:
                Parsed file object.
        """
        file = await telegram._api("getFile", _Schema.file, params={"file_id": id_})
        url = ("https://api.telegram.org/file/bot{}/{}"
               .format(telegram.config["token"], file["file_path"]))
        return immp.File(name, type_, url)


class TelegramMessage(immp.Message):
    """
    Message originating from Telegram.
    """

    _file_types = ("animation", "video", "video_note", "audio", "voice", "document")

    @classmethod
    async def from_bot_message(cls, telegram, json):
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
        id_ = "{}:{}".format(message["chat"]["id"], message["message_id"])
        revision = message["edit_date"] or message["date"]
        at = datetime.fromtimestamp(message["date"], timezone.utc)
        channel = immp.Channel(telegram, message["chat"]["id"])
        edited = bool(message["edit_date"])
        text = await TelegramRichText.from_bot_entities(telegram, message["text"],
                                                        message["entities"])
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
            reply_to = await cls.from_bot_message(telegram, message["reply_to_message"])
        # At most one of these fields will be set.
        if message["group_chat_created"]:
            action = True
            text = immp.RichText([immp.Segment("created the group "),
                                  immp.Segment(message["chat"]["title"], bold=True)])
        elif message["new_chat_members"]:
            joined = [(TelegramUser.from_bot_user(telegram, member))
                      for member in message["new_chat_members"]]
            action = True
            if joined == [user]:
                text = "joined group via invite link"
            else:
                text = immp.RichText()
                for join in joined:
                    text.append(immp.Segment(", " if text else "invited "),
                                immp.Segment(join.real_name, bold=True, link=join.link))
        elif message["left_chat_member"]:
            left = [TelegramUser.from_bot_user(telegram, message["left_chat_member"])]
            action = True
            if left == [user]:
                text = "left group"
            else:
                part = left[0]
                text = immp.RichText([immp.Segment("removed "),
                                      immp.Segment(part.real_name, bold=True, link=part.link)])
        elif message["new_chat_title"]:
            title = message["new_chat_title"]
            action = True
            text = immp.RichText([immp.Segment("changed group name to "),
                                  immp.Segment(title, bold=True)])
        elif message["new_chat_photo"]:
            action = True
            text = "changed group photo"
            photo = max(message["new_chat_photo"], key=lambda photo: photo["height"])
            attachments.append(await TelegramFile.from_id(telegram, photo["file_id"],
                                                          immp.File.Type.image))
        elif message["delete_chat_photo"]:
            action = True
            text = "removed group photo"
        elif message["pinned_message"]:
            action = True
            text = "pinned a message"
            attachments.append(await cls.from_bot_message(telegram, message["pinned_message"]))
        elif message["photo"]:
            # This is a list of resolutions, find the original sized one to return.
            photo = max(message["photo"], key=lambda photo: photo["height"])
            attachments.append(await TelegramFile.from_id(telegram, photo["file_id"],
                                                          immp.File.Type.image))
            if message["caption"]:
                text = await TelegramRichText.from_bot_entities(telegram, message["caption"],
                                                                message["caption_entities"])
        elif message["sticker"]:
            attachments.append(await TelegramFile.from_id(telegram, message["sticker"]["file_id"],
                                                          immp.File.Type.image))
            # All real stickers should have an emoji, but webp images uploaded as photos are
            # incorrectly categorised as stickers in the API response.
            if not text and message["sticker"]["emoji"]:
                action = True
                text = "sent {} sticker".format(message["sticker"]["emoji"])
        elif any(message[key] for key in cls._file_types):
            for key in cls._file_types:
                if message[key]:
                    obj = message[key]
                    break
            type_ = immp.File.Type.image if key == "animation" else immp.File.Type.unknown
            attachments.append(await TelegramFile.from_id(telegram, obj["file_id"], type_,
                                                          obj["file_name"]))
        elif message["venue"]:
            attachments.append(immp.Location(latitude=message["venue"]["location"]["latitude"],
                                             longitude=message["venue"]["location"]["longitude"],
                                             name=message["venue"]["title"],
                                             address=message["venue"]["address"]))
        elif message["location"]:
            attachments.append(immp.Location(latitude=message["location"]["latitude"],
                                             longitude=message["location"]["longitude"]))
        elif message["poll"]:
            action = True
            prefix = "closed the" if message["poll"]["is_closed"] else "opened a"
            text = immp.RichText([immp.Segment("{} poll: ".format(prefix)),
                                  immp.Segment(message["poll"]["question"], bold=True)])
        elif not text:
            # No support for this message type.
            raise NotImplementedError
        common = dict(id_=id_,
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
                try:
                    forward_user = TelegramUser.from_bot_channel(telegram,
                                                                 message["forward_from_chat"])
                except _HiddenSender:
                    if message["forward_signature"]:
                        forward_user = immp.User(real_name=message["forward_signature"])
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
                forward = immp.SentMessage(id_=forward_id,
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
    async def from_bot_update(cls, telegram, update):
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
                return await cls.from_bot_message(telegram, update[key])
            elif update.get("edited_{}".format(key)):
                return await cls.from_bot_message(telegram, update["edited_{}".format(key)])

    @classmethod
    async def from_proto_message(cls, telegram, message):
        """
        Convert a Telegram message event to a :class:`.Message`.

        Args:
            telegram (.TelegramPlug):
                Related plug instance that provides the event.
            message (telethon.tl.custom.Message):
                Received message from an event or get request.

        Returns:
            .TelegramMessage:
                Parsed message object.
        """
        id_ = "{}:{}".format(message.chat_id, message.id)
        edited = bool(message.edit_date)
        if edited:
            revision = int(message.edit_date.timestamp())
        elif message.date:
            revision = int(message.date.timestamp())
        else:
            revision = None
        text = await TelegramRichText.from_proto_entities(telegram, message.message,
                                                          message.entities)
        user = TelegramUser.from_proto_user(telegram, await message.get_sender())
        action = False
        reply_to = None
        joined = []
        left = []
        title = None
        attachments = []
        if message.reply_to_msg_id:
            reply_to = await telegram.get_message(message.reply_to_msg_id)
        if message.photo:
            try:
                attach = await TelegramFile.from_id(telegram, pack_bot_file_id(message.photo),
                                                    immp.File.Type.image)
            except TelegramAPIRequestError as e:
                log.warning("Unable to fetch attachment", exc_info=e)
            else:
                attachments.append(attach)
        elif message.document:
            type_ = immp.File.Type.unknown
            name = None
            for attr in message.document.attributes:
                if isinstance(attr, tl.types.DocumentAttributeSticker):
                    type_ = immp.File.Type.image
                    if attr.alt and not text:
                        text = "sent {} sticker".format(attr.alt)
                        action = True
                elif isinstance(attr, tl.types.DocumentAttributeAnimated):
                    type_ = immp.File.Type.image
                elif isinstance(attr, tl.types.DocumentAttributeFilename):
                    name = attr.file_name
            try:
                attach = await TelegramFile.from_id(telegram, pack_bot_file_id(message.document),
                                                    type_, name)
            except TelegramAPIRequestError as e:
                log.warning("Unable to fetch attachment", exc_info=e)
            else:
                attachments.append(attach)
        elif message.poll:
            action = True
            prefix = "closed the" if message.poll.poll.closed else "opened a"
            text = immp.RichText([immp.Segment("{} poll: ".format(prefix)),
                                  immp.Segment(message.poll.poll.question, bold=True)])
        if message.action:
            action = True
            if isinstance(message.action, tl.types.MessageActionChatCreate):
                text = immp.RichText([immp.Segment("created the group "),
                                      immp.Segment(message.action.title, bold=True)])
            elif isinstance(message.action, tl.types.MessageActionChatJoinedByLink):
                joined = [user]
                text = "joined group via invite link"
            elif isinstance(message.action, tl.types.MessageActionChatAddUser):
                joined = await gather(*(telegram.user_from_id(id_) for id_ in message.action.users))
                if joined == [user]:
                    text = "joined group"
                else:
                    text = immp.RichText()
                    for join in joined:
                        text.append(immp.Segment(", " if text else "invited "),
                                    immp.Segment(join.real_name, link=join.link))
            elif isinstance(message.action, tl.types.MessageActionChatDeleteUser):
                left = [await telegram.user_from_id(message.action.user_id)]
                if left == [user]:
                    text = "left group"
                else:
                    part = left[0]
                    text = immp.RichText([immp.Segment("removed "),
                                          immp.Segment(part.real_name, bold=True, link=part.link)])
            elif isinstance(message.action, tl.types.MessageActionChatEditTitle):
                title = message.action.title
                text = immp.RichText([immp.Segment("changed group name to "),
                                      immp.Segment(title, bold=True)])
            elif isinstance(message.action, tl.types.MessageActionChatEditPhoto):
                text = "changed group photo"
            elif isinstance(message.action, tl.types.MessageActionChatDeletePhoto):
                text = "removed group photo"
            elif isinstance(message.action, tl.types.MessageActionPinMessage):
                attachments.append(reply_to)
                reply_to = None
                text = "pinned message"
            else:
                raise NotImplementedError
        if not text and not attachments:
            # No support for this message type.
            raise NotImplementedError
        common = dict(id_=id_,
                      revision=revision,
                      at=message.date,
                      channel=immp.Channel(telegram, message.chat_id),
                      edited=edited,
                      user=user,
                      raw=message)
        if message.forward:
            # Event is a message containing another message.  Forwarded messages have no ID, so we
            # use a Message instead of a SentMessage here, unless they come from a channel.
            forward_id = forward_channel = forward_user = None
            if message.forward.channel_id and message.forward.channel_post:
                forward_channel = immp.Channel(telegram, message.forward.chat_id)
                forward_id = "{}:{}".format(message.forward.chat_id,
                                            message.forward.channel_post)
                if message.forward.post_author:
                    forward_user = immp.User(real_name=message.forward.post_author)
                else:
                    chat = await message.forward.get_chat()
                    forward_user = immp.User(real_name=chat.title)
            elif message.forward.sender_id:
                forward_user = TelegramUser.from_proto_user(telegram,
                                                            await message.forward.get_sender())
            elif message.forward.from_name:
                forward_user = immp.User(real_name=message.forward.from_name)
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
                forward = immp.SentMessage(id_=forward_id,
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


if SQLiteSession:

    class Session(SQLiteSession):

        def _execute_multi(self, statement, *values):
            cursor = self._cursor()
            try:
                return cursor.execute(statement, values).fetchall()
            finally:
                cursor.close()

        def get_user_entities(self):
            return self._execute_multi("SELECT id, username, name FROM entities WHERE id > 0")

        def get_chat_entities(self):
            return self._execute_multi("SELECT id, username, name FROM entities WHERE id < 0")

        def get_entity(self, id_):
            return self._execute("SELECT id, username, name FROM entities WHERE id = ?", id_)


class TelegramPlug(immp.HTTPOpenable, immp.Plug):
    """
    Plug for a `Telegram <https://telegram.org>`_ bot.
    """

    schema = _Schema.config

    network_name = "Telegram"

    @property
    def network_id(self):
        return "telegram:{}".format(self._bot_user["id"]) if self._bot_user else None

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        if bool(self.config["api-id"]) != bool(self.config["api-hash"]):
            raise immp.ConfigError("Both of API ID and hash must be given")
        if self.config["client-updates"] and not self.config["api-id"]:
            raise immp.ConfigError("Client updates require API ID and hash")
        if self.config["session"] and not self.config["api-id"]:
            raise immp.ConfigError("Session file requires API ID and hash")
        # Connection objects that need to be closed on disconnect.
        self._bot_user = self._receive = self._client = None
        self._closing = False
        # Temporary tracking of migrated chats for the current session.
        self._migrations = {}
        # Update ID from which to retrieve the next batch.  Should be one higher than the max seen.
        self._offset = 0

    async def _api(self, endpoint, schema=_Schema.api(), **kwargs):
        url = "https://api.telegram.org/bot{}/{}".format(self.config["token"], endpoint)
        log.debug("Making API request to %r", endpoint)
        try:
            async with self.session.post(url, **kwargs) as resp:
                try:
                    json = await resp.json()
                    data = schema(json)
                except ClientResponseError as e:
                    raise TelegramAPIConnectError("Bad response with code: {}"
                                                  .format(resp.status)) from e
        except ClientError as e:
            raise TelegramAPIConnectError("Request failed") from e
        except TimeoutError as e:
            raise TelegramAPIConnectError("Request timed out") from e
        if not data["ok"]:
            raise TelegramAPIRequestError(data["error_code"], data["description"])
        return data["result"]

    async def start(self):
        await super().start()
        self._closing = False
        self._bot_user = await self._api("getMe", _Schema.me)
        if self.config["api-id"] and self.config["api-hash"]:
            if not TelegramClient:
                raise immp.ConfigError("API ID/hash specified but Telethon is not installed")
            log.debug("Starting client")
            self._client = TelegramClient(Session(self.config["session"]),
                                          self.config["api-id"], self.config["api-hash"])
        if self._client and self.config["client-updates"]:
            log.debug("Adding client event handlers")
            self._client.add_event_handler(self._handle_raw)
            for event in (events.NewMessage, events.MessageEdited, events.ChatAction):
                self._client.add_event_handler(self._handle, event)
        else:
            log.debug("Starting update long-poll")
            self._receive = ensure_future(self._poll())
        if self._client:
            await self._client.start(bot_token=self.config["token"])

    async def stop(self):
        await super().stop()
        self._closing = True
        if self._receive:
            log.debug("Stopping update long-poll")
            self._receive.cancel()
            self._receive = None
        if self._client:
            log.debug("Closing client")
            await self._client.disconnect()
            self._client = None
        self._bot_user = None
        self._offset = 0
        if self._migrations:
            log.warning("Chat migrations require a config update before next run")

    async def user_from_id(self, id_):
        if not self._client:
            log.debug("Client auth required to look up users")
            return None
        try:
            data = await self._client(tl.functions.users.GetFullUserRequest(int(id_)))
        except BadRequestError:
            entity = self._client.session.get_entity(id_)
            return TelegramUser.from_entity(self, entity) if entity else None
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
            return TelegramUser.from_proto_user(self, data.users[0]) if data.users else None

    async def user_is_system(self, user):
        return user.id == str(self._bot_user["id"])

    async def public_channels(self):
        if not self._client:
            log.debug("Client auth required to look up channels")
            return None
        # Use the session cache to find all "seen" chats -- not guaranteed to be a complete list.
        return [immp.Channel(self, chat[0]) for chat in self._client.session.get_chat_entities()]

    async def private_channels(self):
        if not self._client:
            log.debug("Client auth required to look up channels")
            return None
        # Private channels just use user IDs, so return all users we know about.  Note that these
        # channels aren't usable unless the user has messaged the bot first.
        return [immp.Channel(self, chat[0]) for chat in self._client.session.get_user_entities()]

    async def channel_for_user(self, user):
        if not isinstance(user, TelegramUser):
            return None
        try:
            await self._api("getChat", params={"chat_id": user.id})
        except TelegramAPIRequestError as e:
            log.warning("Failed to retrieve user %s channel", user.id, exc_info=e)
            # Can't create private channels, users must initiate conversations with bots.
            return None
        else:
            return immp.Channel(self, user.id)

    async def channel_is_private(self, channel):
        return int(channel.source) > 0

    async def channel_title(self, channel):
        if await channel.is_private():
            return None
        if self._client:
            row = self._client.session.get_entity(channel.source)
            if row and row[2]:
                return row[2]
        try:
            data = await self._api("getChat", _Schema.chat, params={"chat_id": channel.source})
        except TelegramAPIRequestError as e:
            log.warning("Failed to retrieve channel %s title", channel.source, exc_info=e)
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
                # Private channels should just contain the bot and the corresponding user.
                if channel.source == str(self._bot_user["id"]):
                    return [TelegramUser.from_bot_user(self, self._bot_user)]
                elif int(channel.source) > 0:
                    entity = self._client.session.get_entity(channel.source)
                    if entity:
                        return [TelegramUser.from_bot_user(self, self._bot_user),
                                await self.user_from_id(channel.source)]
                    else:
                        return None
            else:
                return [TelegramUser.from_proto_user(self, user) for user in data.users]

    async def channel_remove(self, channel, user):
        await self._api("kickChatMember", params={"chat_id": channel.source, "user_id": user.id})

    async def channel_history(self, channel, before=None):
        if not self._client:
            log.debug("Client auth required to retrieve messages")
            return []
        elif not before:
            log.debug("Before message required to retrieve messages")
            return []
        chat, message = (int(field) for field in before.id.split(":", 1))
        ids = list(range(max(message - 50, 1), message))
        history = filter(None, await self._client.get_messages(entity=chat, ids=ids))
        tasks = (TelegramMessage.from_proto_message(self, message) for message in history)
        results = await gather(*tasks, return_exceptions=True)
        messages = []
        for result in results:
            if isinstance(result, NotImplementedError):
                continue
            elif isinstance(result, Exception):
                raise result
            else:
                messages.append(result)
        return messages

    async def get_message(self, id_):
        if not self._client:
            log.debug("Client auth required to retrieve messages")
            return None
        message = await self._client.get_messages(None, ids=id_)
        if not message:
            return None
        try:
            return await TelegramMessage.from_proto_message(self, message)
        except NotImplementedError:
            return None

    async def _form_data(self, base, field, attach):
        data = FormData(base)
        if attach.source:
            data.add_field(field, attach.source)
        else:
            img_resp = await attach.get_content(self.session)
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
            except (TelegramAPIConnectError, TelegramAPIRequestError):
                log.debug("Failed to upload image, falling back to document upload")
        data = await self._form_data(base, "document", attach)
        try:
            return await self._api("sendDocument", _Schema.send, data=data)
        except TelegramAPIConnectError as e:
            log.warning("Failed to upload file", exc_info=e)
            return None

    def _requests(self, chat, msg):
        requests = []
        if msg.text or msg.reply_to:
            quote = False
            reply_to = ""
            if isinstance(msg.reply_to, immp.Receipt):
                # Reply natively to the given parent message.
                reply_to = int(msg.reply_to.id.split(":")[1])
            elif isinstance(msg.reply_to, immp.Message):
                quote = True
            edited = msg.edited if isinstance(msg, immp.Receipt) else False
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
            if isinstance(attach, immp.Receipt):
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
            sent = await TelegramMessage.from_bot_message(self, result)
            self.queue(sent)
            ids.append(sent.id)
        return ids

    async def delete(self, sent):
        chat, message = sent.id.split(":", 1)
        await self._api("deleteMessage", params={"chat_id": chat, "message_id": message})

    def _migrate(self, old, new):
        log.warning("Chat has migrated: %r -> %r", old, new)
        self._migrations[old] = new
        for name, channel in self.host.channels.items():
            if channel.plug is self and channel.source == old:
                log.debug("Updating named channel %r in place", name)
                channel.source = new

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
            except (TelegramAPIConnectError, TelegramAPIRequestError) as e:
                log.debug("Unexpected response or timeout: %r", e)
                log.debug("Reconnecting in 3 seconds")
                await sleep(3)
                continue
            except Exception as e:
                log.exception("Uncaught exception during long-poll: %r", e)
                raise
            for update in result:
                log.debug("Received an update")
                if "message" in update and update["message"]["migrate_to_chat_id"]:
                    old = update["message"]["chat"]["id"]
                    new = update["message"]["migrate_to_chat_id"]
                    self._migrate(old, new)
                if any(key in update or "edited_{}".format(key) in update
                       for key in ("message", "channel_post")):
                    try:
                        sent = await TelegramMessage.from_bot_update(self, update)
                    except NotImplementedError:
                        log.debug("Skipping message with no usable parts")
                    except CancelledError:
                        log.debug("Cancel request for plug %r getter", self.name)
                        return
                    else:
                        self.queue(sent)
                else:
                    log.debug("Ignoring update with unknown keys: %s", ", ".join(update.keys()))
                self._offset = max(update["update_id"] + 1, self._offset)

    async def _handle_raw(self, event):
        log.debug("Received a %s event", event.__class__.__qualname__)
        if isinstance(event, tl.types.UpdateNewMessage):
            if isinstance(event.message.action, tl.types.MessageActionChatMigrateTo):
                old = event.message.chat_id
                new = int("-100{}".format(event.message.action.channel_id))
                self._migrate(old, new)

    async def _handle(self, event):
        if isinstance(event, events.ChatAction.Event):
            message = event.action_message
        else:
            message = event.message
        try:
            sent = await TelegramMessage.from_proto_message(self, message)
        except NotImplementedError:
            log.debug("Skipping message with no usable parts")
        else:
            self.queue(sent)
