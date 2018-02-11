from asyncio import ensure_future, Condition
from json import dumps as json_dumps
import logging

from aiohttp import ClientSession, FormData
import discord
from voluptuous import Schema, Optional, ALLOW_EXTRA

import imirror


log = logging.getLogger(__name__)


class _Schema(object):

    config = Schema({"token": str,
                     Optional("webhooks", default={}): dict},
                    extra=ALLOW_EXTRA, required=True)

    webhook = Schema({"id": str}, extra=ALLOW_EXTRA, required=True)


class DiscordAPIError(imirror.TransportError):
    """
    Generic error from the Slack API.
    """


class DiscordUser(imirror.User):
    """
    User present in Discord.
    """

    @classmethod
    def from_user(cls, discord, user):
        """
        Convert a :class:`discord.User` into a :class:`.User`.

        Args:
            discord (.DiscordTransport):
                Related transport instance that provides the user.
            user (discord.User):
                Hangups user object retrieved from the user list.

        Returns:
            .DiscordUser:
                Parsed user object.
        """
        id = user.id
        username = user.name
        real_name = getattr(user, "nick", None)
        avatar = user.avatar_url or None
        return cls(id, username=username, real_name=real_name, avatar=avatar, raw=user)


class DiscordRichText(imirror.RichText):

    tags = {"**": "bold", "_": "italic", "~": "strike", "`": "code", "```": "pre"}

    @classmethod
    def to_markdown(cls, rich):
        text = ""
        active = []
        for segment in rich.normalise():
            for tag in reversed(active):
                # Check all existing tags, and remove any that end at this segment.
                attr = cls.tags[tag]
                if not getattr(segment, attr):
                    text += tag
                    active.remove(tag)
            for tag, attr in cls.tags.items():
                # Add any new tags that start at this segment.
                if getattr(segment, attr) and tag not in active:
                    text += tag
                    active.append(tag)
            text += segment.text
        for tag in reversed(active):
            # Close all remaining tags.
            text += tag
        return text


class DiscordMessage(imirror.Message):
    """
    Message originating from Discord.
    """

    @classmethod
    def from_message(cls, discord, message):
        """
        Convert a :class:`discord.Message` into a :class:`.Message`.

        Args:
            discord (.DiscordTransport):
                Related transport instance that provides the event.
            message (discord.Message):
                Discord message object received from a channel.

        Returns:
            .DiscordMessage:
                Parsed message object.
        """
        text = None
        attachments = []
        if message.content:
            # TODO: Rich text.
            text = message.content
        for attach in message.attachments:
            type = imirror.File.Type.unknown
            if attach.filename.rsplit(".", 1)[1] in ("jpg", "png", "gif"):
                type = imirror.File.Type.image
            attachments.append(imirror.File(title=attach.filename,
                                            type=type,
                                            source=attach.url))
        for embed in message.embeds:
            if embed.image.url and embed.image.url.rsplit(".", 1)[1] in ("jpg", "png", "gif"):
                attachments.append(imirror.File(type=imirror.File.Type.image,
                                                source=embed.image.url))
        return (discord.host.resolve_channel(discord, message.channel.id),
                cls(id=message.id,
                    at=message.created_at,
                    text=text,
                    user=DiscordUser.from_user(discord, message.author),
                    attachments=attachments,
                    raw=message))


class DiscordClient(discord.Client):
    """
    Subclass of the underlying client to bind events.
    """

    def __init__(self, transport, **kwargs):
        super().__init__(**kwargs)
        self._transport = transport

    async def on_ready(self):
        with await self._transport._starting:
            self._transport._starting.notify_all()

    async def on_message(self, message):
        channel, msg = DiscordMessage.from_message(self._transport, message)
        self._transport.queue(channel, msg)


class DiscordTransport(imirror.Transport):
    """
    Transport for a `Discord <https://discordapp.com>`_ server.

    Config:
        token (str):
            Discord token for a bot user.
        webhooks (dict):
            Mapping from Discord channel IDs to webhook URLs, needed for custom message author
            names and avatars.
    """

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        config = _Schema.config(config)
        self._token = config["token"]
        # Connection objects that need to be closed on disconnect.
        self._client = self._task = self._session = None
        self._starting = Condition()

    async def start(self):
        await super().start()
        if self.config["webhooks"]:
            self._session = ClientSession()
        log.debug("Starting client")
        self._client = DiscordClient(self)
        self._task = ensure_future(self._client.start(self._token))
        with await self._starting:
            # Block until the client is ready.
            await self._starting.wait()

    async def stop(self):
        await super().stop()
        if self._client:
            log.debug("Closing client")
            await self._client.close()
            self._client = None
        if self._session:
            log.debug("Closing session")
            await self._session.close()
            self._session = None

    async def private_channel(self, user):
        if not isinstance(user, DiscordUser):
            return None
        if not isinstance(user.raw, (discord.Member, discord.User)):
            return None
        dm = user.raw.dm_channel or (await user.raw.create_dm())
        return imirror.Channel(None, self, dm.id)

    async def put(self, channel, msg):
        dc_channel = self._client.get_channel(channel.source)
        if not dc_channel:
            raise DiscordAPIError("No access to channel {}".format(channel.source))
        if msg.deleted:
            # TODO
            return []
        webhook = self.config["webhooks"].get(channel.name)
        name = image = rich = None
        if msg.user:
            name = msg.user.real_name or msg.user.username
            image = msg.user.avatar
        if msg.text:
            if isinstance(msg.text, imirror.RichText):
                rich = msg.text.clone()
            else:
                # Unformatted text received, make a basic rich text instance out of it.
                rich = imirror.RichText([imirror.Segment(msg.text)])
            if msg.user and not webhook:
                # Can't customise the author name, so put it in the message body.
                prefix = ("{} " if msg.action else "{}: ").format(name)
                rich.prepend(imirror.Segment(prefix, bold=True))
            if msg.action:
                for segment in rich:
                    segment.italic = True
        if webhook:
            log.debug("Sending to {} via webhook".format(repr(channel)))
            data = FormData()
            payload = {}
            embeds = []
            if msg.attachments:
                for i, attach in enumerate(msg.attachments):
                    if isinstance(attach, imirror.File) and attach.type == imirror.File.Type.image:
                        img_resp = await attach.get_content(self._session)
                        filename = attach.title or "image_{}".format(i)
                        embeds.append({"image": {"url": "attachment://{}".format(filename)}})
                        data.add_field("file_{}".format(i), img_resp.content, filename=filename)
            if msg.reply_to:
                quote = {"footer": {"text": "\U0001f4ac"},  # :speech_balloon:
                         "timestamp": msg.reply_to.at.isoformat()}
                if msg.reply_to.user:
                    quote["author"] = {"name": (msg.reply_to.user.real_name or
                                                msg.reply_to.user.username),
                                       "icon_url": msg.reply_to.user.avatar}
                quoted_rich = None
                quoted_action = False
                if msg.reply_to.text:
                    if isinstance(msg.reply_to.text, imirror.RichText):
                        quoted_rich = msg.reply_to.text.clone()
                    else:
                        quoted_rich = imirror.RichText([imirror.Segment(msg.reply_to.text)])
                elif msg.reply_to.attachments:
                    quoted_action = True
                    count = len(msg.reply_to.attachments)
                    what = "{} files".format(count) if count > 1 else "this file"
                    quoted_rich = imirror.RichText([imirror.Segment("sent {}".format(what))])
                if quoted_rich:
                    if quoted_action:
                        for segment in quoted_rich:
                            segment.italic = True
                    quote["description"] = DiscordRichText.to_markdown(quoted_rich)
                embeds.append(quote)
            # Null values aren't accepted, only add name/image to data if they're set.
            if name:
                payload["username"] = name
            if image:
                payload["avatar_url"] = image
            if rich:
                payload["content"] = DiscordRichText.to_markdown(rich.normalise())
            if embeds:
                payload["embeds"] = embeds
            data.add_field("payload_json", json_dumps(payload))
            async with self._session.post("{}?wait=true".format(webhook), data=data) as resp:
                json = await resp.json()
            message = _Schema.webhook(json)
            return [int(message["id"])]
        else:
            log.debug("Sending to {} via API".format(repr(channel)))
            embed = None
            file = None
            if msg.attachments:
                for attach in msg.attachments:
                    if isinstance(attach, imirror.File) and attach.type == imirror.File.Type.image:
                        img_resp = await attach.get_content(self._session)
                        filename = attach.title or "image"
                        embed = discord.Embed()
                        embed.set_image(url="attachment://{}".format(filename))
                        file = discord.File(img_resp.content, filename)
                        # TODO: Handle multiple attachments.
                        break
                if embed and not rich:
                    rich = DiscordRichText([imirror.Segment(name, bold=True, italic=True),
                                            imirror.Segment(" shared an image", italic=True)])
            message = await dc_channel.send(content=DiscordRichText.to_markdown(rich),
                                            embed=embed, file=file)
            return [message.id]
