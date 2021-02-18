"""
Connect to `Telegram <https://telegram.org>`_ as a bot.

Requirements:
    Extra name: ``telegram``

    `aiohttp <https://aiohttp.readthedocs.io/en/latest/>`_

    `telethon <https://telethon.readthedocs.io/en/latest/>`_:
        Required for use of app features (client updates, user lookups, message history).

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
    stickers (bool):
        ``True`` to include stickers in messages as proprietary-format attachments (.tgs files).

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
"""

from asyncio import CancelledError, TimeoutError, ensure_future, gather, sleep, wait
from collections import defaultdict
from datetime import datetime, timezone
import logging

from aiohttp import ClientError, ClientResponseError, FormData

import immp


try:
    from telethon import TelegramClient, events, tl
    from telethon.errors import BadRequestError, ChannelPrivateError
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
                          immp.Optional("session"): immp.Nullable(str),
                          immp.Optional("stickers", True): bool})

    user = immp.Schema({"id": int,
                        immp.Optional("username"): immp.Nullable(str),
                        "first_name": str,
                        immp.Optional("last_name"): immp.Nullable(str)})

    chat = immp.Schema({"id": int,
                        "type": str,
                        immp.Optional("title"): immp.Nullable(str),
                        immp.Optional("username"): immp.Nullable(str)})

    entity = immp.Schema({"type": str,
                          "offset": int,
                          "length": int,
                          immp.Optional("url"): immp.Nullable(str),
                          immp.Optional("user"): immp.Nullable(user)})

    file = {"file_id": str,
            immp.Optional("file_name"): immp.Nullable(str),
            immp.Optional("file_path"): immp.Nullable(str),
            immp.Optional("mime_type"): immp.Nullable(str)}

    _sticker = {immp.Optional("emoji"): immp.Nullable(str), **file}

    _location = {"latitude": float, "longitude": float}

    message = immp.Schema({"message_id": int,
                           "chat": chat,
                           "date": int,
                           immp.Optional("edit_date"): immp.Nullable(int),
                           immp.Optional("from"): immp.Nullable(user),
                           immp.Optional("sender_chat"): immp.Nullable(chat),
                           immp.Optional("author_signature"): immp.Nullable(str),
                           immp.Optional("forward_from"): immp.Nullable(user),
                           immp.Optional("forward_date"): immp.Nullable(int),
                           immp.Optional("forward_from_chat"): immp.Nullable(chat),
                           immp.Optional("forward_from_message_id"): immp.Nullable(int),
                           immp.Optional("forward_signature"): immp.Nullable(str),
                           immp.Optional("forward_sender_name"): immp.Nullable(str),
                           immp.Optional("text"): immp.Nullable(str),
                           immp.Optional("caption"): immp.Nullable(str),
                           immp.Optional("entities", list): [entity],
                           immp.Optional("caption_entities", list): [entity],
                           immp.Optional("photo", list): [file],
                           immp.Optional("sticker"): immp.Nullable(_sticker),
                           immp.Optional("animation"): immp.Nullable(file),
                           immp.Optional("video"): immp.Nullable(file),
                           immp.Optional("video_note"): immp.Nullable(file),
                           immp.Optional("audio"): immp.Nullable(file),
                           immp.Optional("voice"): immp.Nullable(file),
                           immp.Optional("document"): immp.Nullable(file),
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
                           immp.Optional("new_chat_photo", list): [file],
                           immp.Optional("delete_chat_photo", False): bool,
                           immp.Optional("migrate_to_chat_id"): immp.Nullable(int)})

    # Circular references to embedded messages.
    message.raw.update({immp.Optional("reply_to_message"): immp.Nullable(message),
                        immp.Optional("pinned_message"): immp.Nullable(message)})

    update = immp.Schema({"update_id": int,
                          immp.Optional(immp.Any("message", "edited_message",
                                                 "channel_post", "edited_channel_post")): message})

    @staticmethod
    def api(result=None):
        success = {"ok": True}
        if result:
            success["result"] = result
        return immp.Schema(immp.Any(success,
                                    {"ok": False,
                                     "description": str,
                                     "error_code": int}))


class TelegramAPIConnectError(immp.PlugError):
    """
    Generic error whilst attempting to call the Telegram API.
    """


class TelegramAPIRequestError(immp.PlugError):
    """
    Generic error from the Telegram API.
    """


class _HiddenSender:

    # @HiddenSender, "a user": author of message forwards when opted to be linked back to them.
    hidden_chat_id = 1228946795
    hidden_channel_id = int("-100{}".format(hidden_chat_id))

    # "Telegram": official service account, author of channel messages relayed to discussion chats.
    service_user_id = 777000

    # @GroupAnonymousBot, "Group": author of group chat messages on behalf of anonymous admins.
    anonymous_user_id = 1087968824

    # @ChatsImportBot, "Imported Message": author of group chat messages imported from other apps.
    import_user_id = 1474613229

    @classmethod
    def has(cls, value):
        return value in (cls.hidden_channel_id, cls.hidden_chat_id, cls.service_user_id,
                         cls.anonymous_user_id, cls.import_user_id)


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
    def from_bot_channel(cls, telegram, json, author=None):
        """
        Convert a chat :class:`dict` (attached to a message) to a :class:`.User`.

        Args:
            telegram (.TelegramPlug):
                Related plug instance that provides the user.
            json (dict):
                Telegram API `Chat <https://core.telegram.org/bots/api#chat>`_ object.
            author (str):
                Optional post author, for channel messages.

        Returns:
            .TelegramUser:
                Parsed user object.
        """
        chat = _Schema.chat(json)
        return cls(id_=chat["id"],
                   plug=telegram,
                   username=chat["username"],
                   real_name=author or chat["title"],
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
    def from_proto_channel(cls, telegram, chat, author=None):
        """
        Convert a :class:`telethon.tl.types.Channel` into a :class:`.User`.

        Args:
            telegram (.TelegramPlug):
                Related plug instance that provides the user.
            chat (telethon.tl.types.Channel):
                Telegram channel retrieved from the MTProto API.
            author (str):
                Optional post author, for channel messages.

        Returns:
            .TelegramUser:
                Parsed user object.
        """
        return cls(id_="-100{}".format(chat.id),
                   plug=telegram,
                   username=chat.username,
                   real_name=author or chat.title,
                   raw=chat)

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
        avatar = None
        if username:
            avatar = "https://t.me/i/userpic/320/{}.jpg".format(username)
        return cls(id_=id_,
                   plug=telegram,
                   username=username,
                   real_name=name,
                   avatar=avatar,
                   raw=entity)

    @property
    def link(self):
        if self.username:
            return "https://t.me/{}".format(self.username)
        else:
            return "tg://user?id={}".format(self.id)

    @link.setter
    def link(self, value):
        pass


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
        if segment.code:
            text = "<code>{}</code>".format(text)
        if segment.pre:
            text = "<pre>{}</pre>".format(text)
        if segment.bold:
            text = "<b>{}</b>".format(text)
        if segment.italic:
            text = "<i>{}</i>".format(text)
        if segment.underline:
            text = "<u>{}</u>".format(text)
        if segment.strike:
            text = "<s>{}</s>".format(text)
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
            part = text[start:end]
            if isinstance(part, bytes):
                part = part.decode("utf-16-le")
            segments.append(immp.Segment(part, **formatting))
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
            if entity["type"] in ("bold", "italic", "underline", "code", "pre"):
                key = entity["type"]
                value = True
            elif entity["type"] == "strikethrough":
                key = "strike"
                value = True
            elif entity["type"] == "url":
                key = "link"
                value = encoded[start:end].decode("utf-16-le")
            elif entity["type"] == "email":
                key = "link"
                value = "mailto:{}".format(encoded[start:end].decode("utf-16-le"))
            elif entity["type"] == "text_link":
                key = "link"
                value = entity["url"]
            elif entity["type"] == "mention":
                key = "mention"
                username = encoded[start + 2:end].decode("utf-16-le")
                value = await telegram.user_from_username(username)
            elif entity["type"] == "text_mention":
                key = "mention"
                value = TelegramUser.from_bot_user(telegram, entity["user"])
            else:
                continue
            clear = False if value is True else None
            changes[start][key] = value
            changes[end][key] = clear
        return cls._from_changes(encoded, changes)

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
            value = True
            if isinstance(entity, tl.types.MessageEntityBold):
                key = "bold"
            elif isinstance(entity, tl.types.MessageEntityItalic):
                key = "italic"
            elif isinstance(entity, tl.types.MessageEntityUnderline):
                key = "underline"
            elif isinstance(entity, tl.types.MessageEntityStrike):
                key = "strike"
            elif isinstance(entity, tl.types.MessageEntityCode):
                key = "code"
            elif isinstance(entity, tl.types.MessageEntityPre):
                key = "pre"
            elif isinstance(entity, tl.types.MessageEntityUrl):
                key = "link"
                value = text[entity.offset:entity.offset + entity.length]
            elif isinstance(entity, tl.types.MessageEntityTextUrl):
                key = "link"
                value = entity.url
            elif isinstance(entity, tl.types.MessageEntityEmail):
                key = "link"
                value = "mailto:{}".format(text[entity.offset:entity.offset + entity.length])
            elif isinstance(entity, tl.types.MessageEntityMention):
                key = "mention"
                username = text[entity.offset + 1:entity.offset + entity.length]
                value = await telegram.user_from_username(username)
            elif isinstance(entity, tl.types.MessageEntityMentionName):
                key = "mention"
                value = await telegram.user_from_id(entity.user_id)
            else:
                continue
            clear = False if value is True else None
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
        try:
            file_ = await telegram._api("getFile", _Schema.file, params={"file_id": id_})
        except TelegramAPIRequestError:
            # Can happen if the file is too big, in which case just return a placeholder.
            log.warning("Failed to retrieve message attachment", exc_info=True)
            return immp.File(name, type_)
        else:
            url = ("https://api.telegram.org/file/bot{}/{}"
                   .format(telegram.config["token"], file_["file_path"]))
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
        if message["from"] and not _HiddenSender.has(message["from"]["id"]):
            user = TelegramUser.from_bot_user(telegram, message["from"])
        elif message["sender_chat"] and not _HiddenSender.has(message["sender_chat"]["id"]):
            user = TelegramUser.from_bot_chat(telegram, message["sender_chat"],
                                              message["author_signature"])
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
            if telegram.config["stickers"] or not message["sticker"]["emoji"]:
                attachments.append(await TelegramFile.from_id(telegram,
                                                              message["sticker"]["file_id"],
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
            mime = obj["mime_type"] or ""
            if key == "animation" or mime.startswith("image/"):
                type_ = immp.File.Type.image
            elif key in ("video", "video_note") or mime.startswith("video/"):
                type_ = immp.File.Type.video
            else:
                type_ = immp.File.Type.unknown
            attachments.append(await TelegramFile.from_id(telegram, obj["file_id"], type_,
                                                          obj["file_name"]))
            if message["caption"]:
                text = await TelegramRichText.from_bot_entities(telegram, message["caption"],
                                                                message["caption_entities"])
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
        ignore_forward = False
        if message["from"] and message["from"]["id"] == _HiddenSender.import_user_id:
            # Import bot forwarded the message.  Copy the forward metadata to the main message.
            ignore_forward = True
            if message["forward_date"]:
                at = datetime.fromtimestamp(message["forward_date"], timezone.utc)
            if message["forward_from"]:
                user = TelegramUser.from_bot_user(telegram, message["forward_from"])
            elif message["forward_sender_name"]:
                user = immp.User(real_name=message["forward_sender_name"])
        common = dict(id_=id_,
                      revision=revision,
                      at=at,
                      channel=channel,
                      user=user,
                      raw=message)
        if message["forward_date"] and not ignore_forward:
            # Event is a message containing another message.  Forwarded messages have no ID, so we
            # use a Message instead of a SentMessage here, unless they come from a channel.
            forward_id = forward_channel = forward_date = forward_user = None
            chat = message["forward_from_chat"]
            sender = message["forward_from"]
            if chat and message["forward_from_message_id"]:
                forward_id = "{}:{}".format(chat["id"], message["forward_from_message_id"])
                forward_channel = immp.Channel(telegram, chat["id"])
            if message["forward_date"]:
                at = datetime.fromtimestamp(message["forward_date"], timezone.utc)
            if sender and not _HiddenSender.has(sender["id"]):
                forward_user = TelegramUser.from_bot_user(telegram, sender)
            elif chat and not _HiddenSender.has(chat["id"]):
                forward_user = TelegramUser.from_bot_channel(telegram, chat,
                                                             message["forward_signature"])
            elif message["forward_sender_name"]:
                forward_user = immp.User(real_name=message["forward_sender_name"])
            forward_common = dict(text=text,
                                  user=forward_user,
                                  edited=edited,
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
                                           at=forward_date,
                                           **forward_common)
            else:
                forward = immp.Message(**forward_common)
            # Embed the inner message as an attachment.
            return immp.SentMessage(attachments=[forward], **common)
        else:
            return immp.SentMessage(text=text,
                                    edited=edited,
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
        at = message.date
        channel = immp.Channel(telegram, message.chat_id)
        edited = bool(message.edit_date)
        if edited:
            revision = int(message.edit_date.timestamp())
        elif message.date:
            revision = int(message.date.timestamp())
        else:
            revision = None
        text = await TelegramRichText.from_proto_entities(telegram, message.message,
                                                          message.entities)
        user = None
        if message.sender_id and not _HiddenSender.has(message.sender_id):
            sender = await message.get_sender()
            user = TelegramUser.from_proto_user(telegram, sender)
        elif message.chat_id and not _HiddenSender.has(message.chat_id):
            chat = await message.get_chat()
            user = TelegramUser.from_proto_channel(telegram, chat, message.post_author)
        action = False
        reply_to = None
        joined = []
        left = []
        title = None
        attachments = []
        if message.reply_to_msg_id:
            receipt = immp.Receipt(message.reply_to_msg_id, channel)
            reply_to = await telegram.resolve_message(receipt)
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
            sticker = False
            for attr in message.document.attributes:
                if isinstance(attr, tl.types.DocumentAttributeFilename):
                    name = attr.file_name
                elif isinstance(attr, tl.types.DocumentAttributeSticker):
                    type_ = immp.File.Type.image
                    if attr.alt and not text:
                        text = "sent {} sticker".format(attr.alt)
                        action = True
                        sticker = True
                elif isinstance(attr, tl.types.DocumentAttributeAnimated):
                    type_ = immp.File.Type.image
                elif isinstance(attr, tl.types.DocumentAttributeVideo):
                    type_ = immp.File.Type.video
            if type_ == immp.File.Type.unknown:
                mime = message.document.mime_type or ""
                if mime.startswith("image/"):
                    type_ = immp.File.Type.image
                elif mime.startswith("video/"):
                    type_ = immp.File.Type.video
            if telegram.config["stickers"] or not sticker:
                try:
                    attach = await TelegramFile.from_id(telegram,
                                                        pack_bot_file_id(message.document),
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
        ignore_forward = False
        if message.sender_id == _HiddenSender.import_user_id:
            # Import bot forwarded the message.  Copy the forward metadata to the main message.
            ignore_forward = True
            if message.forward.date:
                at = message.forward.date
            if message.forward.sender_id:
                sender = await message.forward.get_sender()
                user = TelegramUser.from_proto_user(telegram, sender)
            elif message.forward.from_name:
                user = immp.User(real_name=message.forward.from_name)
        common = dict(id_=id_,
                      revision=revision,
                      at=at,
                      channel=channel,
                      user=user,
                      raw=message)
        if message.forward and not ignore_forward:
            # Event is a message containing another message.  Forwarded messages have no ID, so we
            # use a Message instead of a SentMessage here, unless they come from a channel.
            forward_id = forward_channel = forward_date = forward_user = None
            if message.forward.chat_id and message.forward.channel_post:
                forward_id = "{}:{}".format(message.forward.chat_id,
                                            message.forward.channel_post)
                forward_channel = immp.Channel(telegram, message.forward.chat_id)
            if message.forward.date:
                forward_date = message.forward.date
            if message.forward.sender_id and not _HiddenSender.has(message.forward.sender_id):
                sender = await message.forward.get_sender()
                forward_user = TelegramUser.from_proto_user(telegram, sender)
            elif message.forward.chat_id and not _HiddenSender.has(message.forward.chat_id):
                chat = await message.forward.get_chat()
                forward_user = TelegramUser.from_proto_channel(telegram, chat,
                                                               message.forward.post_author)
            elif message.forward.from_name:
                forward_user = immp.User(real_name=message.forward.from_name)
            forward_common = dict(text=text,
                                  user=forward_user,
                                  edited=edited,
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
                                           at=forward_date,
                                           **forward_common)
            else:
                forward = immp.Message(**forward_common)
            # Embed the inner message as an attachment.
            return immp.SentMessage(attachments=[forward], **common)
        else:
            return immp.SentMessage(text=text,
                                    edited=edited,
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

        def get_entity_username(self, username):
            return self._execute("SELECT id, username, name FROM entities WHERE username = ?",
                                 username)


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
        # Caching of user/username lookups to avoid flooding.
        self._users = {}
        self._usernames = {}
        # Blacklist of channels we have an entity for but can't access.  Indexed at startup, with
        # chats removed if we receive a message from that channel.
        self._blacklist = set()
        self._blacklist_task = None
        # Update ID from which to retrieve the next batch.  Should be one higher than the max seen.
        self._offset = 0
        # Private chats and non-super groups have a shared incremental message ID.  Cache the
        # highest we've seen, so that we can attempt to fetch past messages with this as a base.
        self._last_id = None

    async def _api(self, endpoint, type_=None, quiet=False, **kwargs):
        url = "https://api.telegram.org/bot{}/{}".format(self.config["token"], endpoint)
        if not quiet:
            log.debug("Making API request to %r", endpoint)
        try:
            async with self.session.post(url, **kwargs) as resp:
                try:
                    json = await resp.json()
                    schema = _Schema.api(type_)
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
        self._bot_user = await self._api("getMe", _Schema.user)
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
            # Find the most recently received message, and therefore the current value of the shared
            # ID sequence.  Fetch the current state, then subtract one from pts to make it replay
            # the last message, which should appear in new_messages and other_updates.
            state = await self._client(tl.functions.updates.GetStateRequest())
            diff = await self._client(tl.functions.updates.GetDifferenceRequest(
                state.pts - 1, datetime.utcnow(), state.qts))
            if isinstance(diff, tl.types.updates.DifferenceEmpty):
                # Unclear if this will happen with the given parameters.
                pass
            elif diff.new_messages:
                self._last_id = diff.new_messages[-1].id
            self._blacklist = {_HiddenSender.hidden_channel_id}
            self._blacklist_task = ensure_future(wait([self._blacklist_users(),
                                                       self._blacklist_chats()]))

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
        if self._blacklist:
            self._blacklist.clear()
        if self._blacklist_task:
            self._blacklist_task.cancel()
            self._blacklist_task = None
        self._offset = 0
        self._last_id = None
        if self._migrations:
            log.warning("Chat migrations require a config update before next run")

    async def user_from_id(self, id_):
        id_ = int(id_)
        if not self._client:
            log.debug("Client auth required to look up users")
            return None
        entity = self._client.session.get_entity(id_)
        if entity:
            return TelegramUser.from_entity(self, entity)
        elif id_ in self._users:
            return self._users[id_]
        try:
            data = await self._client(tl.functions.users.GetFullUserRequest(id_))
        except ValueError:
            log.warning("Missing entity for user %d", id_)
            return None
        except BadRequestError:
            return None
        user = TelegramUser.from_proto_user(self, data.user)
        self._users[id_] = user
        return user

    async def user_from_username(self, username):
        if not self._client:
            log.debug("Client auth required to look up users")
            return None
        entity = self._client.session.get_entity_username(username)
        if entity:
            return TelegramUser.from_entity(self, entity)
        elif username in self._usernames:
            return self._usernames[username]
        try:
            data = await self._client(tl.functions.contacts.ResolveUsernameRequest(username))
        except BadRequestError:
            return None
        if not data.users:
            return None
        user = TelegramUser.from_proto_user(self, data.users[0])
        self._usernames[username] = user
        return user

    async def user_is_system(self, user):
        return user.id == str(self._bot_user["id"])

    async def _blacklist_users(self):
        # For each user in the entity table, check the bot API for a corresponding chat, and
        # blacklist those who haven't started a conversation with us yet.
        log.debug("Finding users to blacklist")
        count = 0
        for user in self._client.session.get_user_entities():
            try:
                await self._api("getChat", _Schema.chat, quiet=True, params={"chat_id": user[0]})
            except TelegramAPIRequestError:
                count += 1
                self._blacklist.add(user[0])
        log.debug("Blacklisted %d users", count)

    async def _blacklist_chats(self):
        # The entity cache is polluted with channels we've seen outside of participation (e.g.
        # mentions and forwards).  Narrow down the list by excluding chats we can't access.
        log.debug("Finding chats to blacklist")
        count = 0
        lookup = []
        for chat in self._client.session.get_chat_entities():
            if chat[0] in self._blacklist:
                continue
            if str(chat[0]).startswith("-100"):
                try:
                    await self._client(tl.functions.channels.GetChannelsRequest([abs(chat[0])]))
                except ChannelPrivateError:
                    count += 1
                    self._blacklist.add(chat[0])
            else:
                lookup.append(abs(chat[0]))
        if lookup:
            chats = await self._client(tl.functions.messages.GetChatsRequest(lookup))
            gone = [-chat.id for chat in chats.chats if isinstance(chat, tl.types.ChatForbidden)]
            if gone:
                count += len(gone)
                self._blacklist.update(gone)
        log.debug("Blacklisted %d chats", count)

    async def public_channels(self):
        if not self._client:
            log.debug("Client auth required to look up channels")
            return None
        # Use the session cache to find all "seen" chats -- not guaranteed to be a complete list.
        # Filter out chats we're no longer a member of, or otherwise can't access.
        ids = set(chat[0] for chat in self._client.session.get_chat_entities())
        return [immp.Channel(self, chat) for chat in ids - self._blacklist]

    async def private_channels(self):
        if not self._client:
            log.debug("Client auth required to look up channels")
            return None
        # Private channels just use user IDs, so return all users we know about, filtered to those
        # we also have a valid chat for.
        ids = set(chat[0] for chat in self._client.session.get_user_entities())
        return [immp.Channel(self, chat) for chat in ids - self._blacklist]

    async def channel_for_user(self, user):
        if not isinstance(user, TelegramUser):
            return None
        try:
            await self._api("getChat", _Schema.chat, params={"chat_id": user.id})
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
            entity = self._client.session.get_entity(channel.source)
            if entity:
                return entity[2]
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
        # Private channels should just contain the bot and the corresponding user.
        if await channel.is_private():
            if channel.source == str(self._bot_user["id"]):
                return [TelegramUser.from_bot_user(self, self._bot_user)]
            elif int(channel.source) > 0:
                entity = self._client.session.get_entity(channel.source)
                if entity:
                    return [TelegramUser.from_bot_user(self, self._bot_user),
                            await self.user_from_id(channel.source)]
        # Channel and supergroup chat IDs have a bot-API-only prefix to distinguish them.
        if channel.source.startswith("-100"):
            chat = int(channel.source[4:])
            users = []
            try:
                while True:
                    data = await self._client(tl.functions.channels.GetParticipantsRequest(
                        chat, tl.types.ChannelParticipantsRecent(), len(users), 1000, 0))
                    if data.users:
                        users += [TelegramUser.from_proto_user(self, user) for user in data.users
                                  if not _HiddenSender.has(user.id)]
                    else:
                        break
            except ValueError:
                log.warning("Missing entity for channel %d", chat)
                return None
            except BadRequestError:
                return None
            else:
                return users
        else:
            chat = abs(int(channel.source))
            try:
                data = await self._client(tl.functions.messages.GetFullChatRequest(chat))
            except ValueError:
                log.warning("Missing entity for channel %d", chat)
                return None
            except BadRequestError:
                return None
            else:
                return [TelegramUser.from_proto_user(self, user) for user in data.users]

    async def channel_remove(self, channel, user):
        if user.id == self._bot_user["id"]:
            await self._api("leaveChat", params={"chat_id": channel.source})
        else:
            await self._api("kickChatMember", params={"chat_id": channel.source,
                                                      "user_id": user.id})

    async def channel_history(self, channel, before=None):
        if not self._client:
            log.debug("Client auth required to retrieve messages")
            return []
        # Telegram channels (including supergroups) have their own message ID sequence starting from
        # 1.  Each user has a shared ID sequence used for non-super groups and private chats.
        private_seq = channel.source.startswith("-100")
        if not before:
            if private_seq:
                request = tl.functions.channels.GetFullChannelRequest(int(channel.source))
                chat = await self._client(request)
                before = immp.Receipt("{}:{}".format(channel.source, chat.full_chat.pts), channel)
            elif self._last_id:
                before = immp.Receipt("{}:{}".format(channel.source, self._last_id + 1), channel)
            else:
                log.debug("Before message required to retrieve messages")
                return []
        chat, message = (int(field) for field in before.id.split(":", 1))
        # For a channel-private sequence, we can just retrieve the last batch of messages.  For the
        # shared sequence, we can't lookup for a specific chat, so we instead fetch a larger batch
        # (maxes out at 200) and filter to messages from the target chat.
        limit = 50 if private_seq else 200
        ids = list(range(max(message - limit, 1), message))
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

    async def get_message(self, receipt):
        if not self._client:
            log.debug("Client auth required to retrieve messages")
            return None
        id_ = int(receipt.id.split(":", 1)[1])
        chat = int(receipt.channel.source)
        message = await self._client.get_messages(chat, ids=id_)
        if not message:
            log.debug("Failed to find message %d in chat %d", id_, chat)
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

    async def _upload_attachment(self, chat, msg, attach, reply_to=None, caption=None):
        # Upload a file to Telegram in its own message.
        # Prefer a source URL if available, else fall back to re-uploading the file.
        base = {"chat_id": str(chat)}
        if reply_to:
            base.update({"reply_to_message_id": reply_to,
                         "allow_sending_without_reply": "true"})
        if msg.user:
            if not caption:
                if attach.type == immp.File.Type.image:
                    what = "an image"
                elif attach.type == immp.File.Type.video:
                    what = "a video"
                else:
                    what = "a file"
                caption = immp.Message(text=immp.RichText([immp.Segment("sent {}".format(what))]),
                                       user=msg.user, action=True).render()
            text = "".join(TelegramSegment.to_html(self, segment) for segment in caption)
            base["caption"] = text
            base["parse_mode"] = "HTML"
        if attach.type == immp.File.Type.image:
            data = await self._form_data(base, "photo", attach)
            try:
                return await self._api("sendPhoto", _Schema.message, data=data)
            except (TelegramAPIConnectError, TelegramAPIRequestError):
                log.debug("Failed to upload image, falling back to document upload")
        elif attach.type == immp.File.Type.video:
            data = await self._form_data(base, "video", attach)
            try:
                return await self._api("sendVideo", _Schema.message, data=data)
            except (TelegramAPIConnectError, TelegramAPIRequestError):
                log.debug("Failed to upload video, falling back to document upload")
        data = await self._form_data(base, "document", attach)
        try:
            return await self._api("sendDocument", _Schema.message, data=data)
        except TelegramAPIConnectError as e:
            log.warning("Failed to upload file", exc_info=e)
            return None

    def _requests(self, chat, msg):
        reply_to = ""
        quote = False
        rich = None
        if isinstance(msg.reply_to, immp.Receipt):
            reply_to = int(msg.reply_to.id.split(":", 1)[1])
        elif isinstance(msg.reply_to, immp.Message):
            quote = True
        if msg.text or quote:
            rich = msg.render(edit=msg.edited, quote_reply=quote)
        # If there's exactly one file attachment that we can attach a caption to, use that for the
        # message text, otherwise send the text first in its own message.
        parts = []
        captionable = []
        for attach in msg.attachments:
            if isinstance(attach, immp.File):
                parts.append(attach)
                captionable.append(attach)
            elif isinstance(attach, immp.Location):
                parts.append(attach)
        requests = []
        # Send the primary attachment, or the leading text-only message, with any reply metadata.
        primary = None
        if len(captionable) == 1:
            primary = captionable[0]
            requests.append(self._upload_attachment(chat, msg, primary, reply_to, rich))
        elif rich:
            text = "".join(TelegramSegment.to_html(self, segment) for segment in rich)
            # Prevent linked user names generating link previews.
            no_link_preview = "true" if msg.user and msg.user.link else "false"
            requests.append(self._api("sendMessage", _Schema.message,
                                      params={"chat_id": chat,
                                              "text": text,
                                              "parse_mode": "HTML",
                                              "disable_web_page_preview": no_link_preview,
                                              "reply_to_message_id": reply_to,
                                              "allow_sending_without_reply": "true"}))
        # Send any remaining attachments as auxilary messages.
        for attach in msg.attachments:
            if attach is primary:
                continue
            elif isinstance(attach, immp.File):
                requests.append(self._upload_attachment(chat, msg, attach))
            elif isinstance(attach, immp.Location):
                requests.append(self._api("sendLocation", _Schema.message,
                                          params={"chat_id": chat,
                                                  "latitude": str(attach.latitude),
                                                  "longitude": str(attach.longitude)}))
                if msg.user:
                    caption = immp.Message(user=msg.user, text="sent a location", action=True)
                    text = "".join(TelegramSegment.to_html(self, segment)
                                   for segment in caption.render())
                    requests.append(self._api("sendMessage", _Schema.message,
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
                forward_chat, forward_id = map(int, attach.id.split(":", 1))
                requests.append(self._api("forwardMessage", _Schema.message,
                                          params={"chat_id": chat,
                                                  "from_chat_id": forward_chat,
                                                  "message_id": forward_id}))
            elif isinstance(attach, immp.Message):
                requests += self._requests(chat, attach)
        own_requests = self._requests(chat, msg)
        if requests and not own_requests and msg.user:
            # Forwarding a message but no content to show who forwarded it.
            info = immp.Message(user=msg.user, action=True, text="forwarded a message")
            own_requests = self._requests(chat, info)
        requests += own_requests
        receipts = []
        for request in requests:
            result = await request
            if not result:
                continue
            sent = await TelegramMessage.from_bot_message(self, result)
            receipts.append(sent)
            self._post_recv(sent)
        return receipts

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

    def _post_recv(self, sent):
        self.queue(sent)
        chat, seq = (int(part) for part in sent.id.split(":", 1))
        if self._blacklist:
            self._blacklist.discard(chat)
        if not str(chat).startswith("-100"):
            self._last_id = seq

    async def _poll(self):
        while not self._closing:
            params = {"offset": self._offset,
                      "timeout": 240}
            fetch = ensure_future(self._api("getUpdates", [_Schema.update], params=params))
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
                        self._post_recv(sent)
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
            self._post_recv(sent)
