"""
Connect to `Discord <https://discordapp.com>`_ as a bot.

Config:
    token (str):
        Discord token for the bot user.
    bot (bool):
        Whether the token represents a bot user (true by default).
    webhooks (dict):
        Mapping from Discord channel IDs to webhook URLs, needed for custom message author
        names and avatars.
    playing (str):
        Optional game activity message to show as the bot's presence.

Note that the token is neither a client ID nor client secret -- you need to enable bot features for
your app, and collect the token from there.  New apps can be created from the `My Apps
<https://discordapp.com/developers/applications/me>`_ page in the developer docs.

Because gateway connections can't customise the sender when pushing new messages, you may also want
an `incoming webhook <https://discordapp.com/developers/docs/resources/webhook>`_  configured for
each channel you intend to send messages to.  A new webhook can be created over the API, or in the
UI via Edit Channel > Webhooks.  A fallback style incorporating the user's name in the message text
will be used in lieu of a webhook, e.g. with direct messages.

.. note::
    This plug requires the **new 1.0 release** of the `discord.py
    <https://discordpy.readthedocs.io/en/rewrite/>`_ Python module, which is currently in alpha.
"""

from asyncio import Condition, ensure_future
from collections import defaultdict
from functools import partial
from json import dumps as json_dumps
import logging
import re

from aiohttp import ClientSession, FormData
import discord
from emoji import emojize
from voluptuous import ALLOW_EXTRA, Any, Optional, Schema

import immp


log = logging.getLogger(__name__)


class _Schema:

    config = Schema({"token": str,
                     Optional("bot", default=True): bool,
                     Optional("webhooks", default=dict): dict,
                     Optional("playing", default=None): Any(str, None)},
                    extra=ALLOW_EXTRA, required=True)

    webhook = Schema(Any({"code": int, "message": str}, {"id": str}),
                     extra=ALLOW_EXTRA, required=True)


class DiscordAPIError(immp.PlugError):
    """
    Generic error from the Discord API.
    """


class DiscordUser(immp.User):
    """
    User present in Discord.
    """

    @classmethod
    def from_user(cls, discord, user):
        """
        Convert a :class:`discord.User` into a :class:`.User`.

        Args:
            discord (.DiscordPlug):
                Related plug instance that provides the user.
            user (discord.User):
                Hangups user object retrieved from the user list.

        Returns:
            .DiscordUser:
                Parsed user object.
        """
        username = "{}#{}".format(user.name, user.discriminator)
        real_name = getattr(user, "nick", None) or user.name
        avatar = user.avatar_url or None
        return cls(id=user.id,
                   plug=discord,
                   username=username,
                   real_name=real_name,
                   avatar=avatar,
                   raw=user)


class DiscordRichText(immp.RichText):

    tags = {"**": "bold", "*": "italic", "_": "italic", "__": "underline", "~~": "strike",
            "`": "code", "```": "pre"}
    # A rather complicated expression to match formatting tags according to the following rules:
    # 1) Outside of formatting may not be adjacent to alphanumeric or other formatting characters.
    # 2) Inside of formatting may not be adjacent to whitespace or the current formatting character.
    # 3) Formatting characters may be escaped with a backslash.
    # This still isn't perfect, but provides a good approximation outside of edge cases.
    _outside_chars = r"0-9a-z*_~"
    _tag_chars = r"```|\*\*|__|~~|[*_`]"
    _inside_chars = r"\s\1"
    _format_regex = re.compile(r"(?<![{0}\\])({1})(?![{2}])(.+?)(?<![{2}\\])\1(?![{0}])"
                               .format(_outside_chars, _tag_chars, _inside_chars))

    @classmethod
    def _sub_channel(cls, discord, match):
        return "#{}".format(discord._client.get_channel(match.group(1)).name)

    @classmethod
    def from_markdown(cls, discord, text):
        """
        Convert a string of Markdown into a :class:`.RichText`.

        Args:
            discord (.DiscordPlug):
                Related plug instance that provides the text.
            text (str):
                Markdown formatted text.

        Returns:
            .DiscordRichText:
                Parsed rich text container.
        """
        changes = defaultdict(dict)
        while True:
            match = cls._format_regex.search(text)
            if not match:
                break
            start = match.start()
            end = match.end()
            tag = match.group(1)
            # Strip the tag characters from the message.
            text = text[:start] + match.group(2) + text[end:]
            end -= 2 * len(tag)
            # Record the range where the format is applied.
            field = cls.tags[tag]
            changes[start][field] = True
            changes[end][field] = False
        for match in re.finditer(r"<@!?(\d+)>", text):
            user = discord._client.get_user(int(match.group(1)))
            if user:
                changes[match.start()]["mention"] = DiscordUser.from_user(discord, user)
                changes[match.end()]["mention"] = None
        segments = []
        points = list(changes.keys())
        # Iterate through text in change start/end pairs.
        for start, end in zip([0] + points, points + [len(text)]):
            if start == end:
                # Zero-length segment at the start or end, ignore it.
                continue
            if changes[start].get("mention"):
                user = changes[start]["mention"]
                part = "@{}".format(user.username or user.real_name)
            else:
                part = emojize(text[start:end], use_aliases=True)
                # Strip Discord channel/emoji tags, replace with a plain text representation.
                part = re.sub(r"<#(\d+)>", partial(cls._sub_channel, discord), part)
                part = re.sub(r"<(:[^: ]+:)\d+>", r"\1", part)
            segments.append(immp.Segment(part, **changes[start]))
        return cls(segments)

    @classmethod
    def _sub_emoji(cls, discord, match):
        for emoji in discord._client.emojis:
            if emoji.name == match.group(1):
                return str(emoji)
        return ":{}:".format(match.group(1))

    @classmethod
    def to_markdown(cls, discord, rich):
        """
        Convert a :class:`.RichText` instance into a Markdown string.

        Args:
            discord (.DiscordPlug):
                Related plug instance to cross-reference users.
            rich (.DiscordRichText):
                Parsed rich text container.

        Returns:
            str:
                Markdown formatted text.
        """
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
            if segment.mention and isinstance(segment.mention.plug, DiscordPlug):
                text += "<@{}>".format(segment.mention.id)
            elif segment.link:
                text += "[{}]({})".format(segment.text, segment.link)
            else:
                text += segment.text
        for tag in reversed(active):
            # Close all remaining tags.
            text += tag
        return re.sub(r":([^: ]+):", partial(cls._sub_emoji, discord), text)


class DiscordMessage(immp.Message):
    """
    Message originating from Discord.
    """

    @classmethod
    def from_message(cls, discord, message, edited=False, deleted=False):
        """
        Convert a :class:`discord.Message` into a :class:`.Message`.

        Args:
            discord (.DiscordPlug):
                Related plug instance that provides the event.
            message (discord.Message):
                Discord message object received from a channel.
            edited (bool):
                Whether this message comes from an edit event.
            deleted (bool):
                Whether this message comes from a delete event.

        Returns:
            .DiscordMessage:
                Parsed message object.
        """
        text = None
        user = DiscordUser.from_user(discord, message.author)
        attachments = []
        if message.content:
            text = DiscordRichText.from_markdown(discord, message.content)
        for attach in message.attachments:
            type = immp.File.Type.unknown
            if attach.filename.endswith((".jpg", ".png", ".gif")):
                type = immp.File.Type.image
            attachments.append(immp.File(title=attach.filename,
                                         type=type,
                                         source=attach.url))
        for embed in message.embeds:
            if embed.image.url and embed.image.url.rsplit(".", 1)[1] in ("jpg", "png", "gif"):
                attachments.append(immp.File(type=immp.File.Type.image,
                                             source=embed.image.url))
        return immp.SentMessage(id=message.id,
                                # Edited timestamp is blank for new messages, but updated in
                                # existing objects when the message is later edited.
                                revision=(message.edited_at or message.created_at).timestamp(),
                                at=message.created_at,
                                channel=immp.Channel(discord, message.channel.id),
                                edited=edited,
                                deleted=deleted,
                                text=text,
                                user=user,
                                attachments=attachments,
                                raw=message)

    @classmethod
    async def to_webhook_embed(cls, discord, msg, reply=False):
        """
        Convert a :class:`.Message` to a message embed structure, suitable for embedding within an
        outgoing message.

        Args:
            discord (.DiscordPlug):
                Target plug instance for this attachment.
            msg (.Message):
                Original message from another plug or hook.
            reply (bool):
                Whether to show a reply icon instead of a quote icon.

        Returns.
            dict:
                Discord API `embed <https://discordapp.com/developers/docs/resources/channel>`_
                object.
        """
        icon = "\N{RIGHTWARDS ARROW WITH HOOK}" if reply else "\N{SPEECH BALLOON}"
        quote = {"footer": {"text": icon}}
        if isinstance(msg, immp.SentMessage):
            quote["timestamp"] = msg.at.isoformat()
        if msg.user:
            quote["author"] = {"name": msg.user.real_name or msg.user.username,
                               "icon_url": msg.user.avatar}
        quoted_rich = None
        quoted_action = False
        if msg.text:
            quoted_rich = msg.text.clone()
            quoted_action = msg.action
        elif msg.attachments:
            count = len(msg.attachments)
            what = "{} files".format(count) if count > 1 else "this file"
            quoted_rich = immp.RichText([immp.Segment("sent {}".format(what))])
            quoted_action = True
        if quoted_rich:
            if quoted_action:
                for segment in quoted_rich:
                    segment.italic = True
            quote["description"] = DiscordRichText.to_markdown(discord, quoted_rich)
        return quote


class DiscordClient(discord.Client):
    """
    Subclass of the underlying client to bind events.
    """

    def __init__(self, plug, **kwargs):
        super().__init__(**kwargs)
        self._plug = plug

    async def on_ready(self):
        async with self._plug._starting:
            self._plug._starting.notify_all()
        await self.on_resume()

    async def on_resume(self):
        if self._plug.config["playing"]:
            await self.change_presence(activity=discord.Game(self._plug.config["playing"]))

    async def on_message(self, message):
        log.debug("Received a new message")
        self._plug.queue(DiscordMessage.from_message(self._plug, message))

    async def on_message_edit(self, before, after):
        log.debug("Received an updated message")
        if before.content == after.content:
            # Text content hasn't changed -- maybe just a link unfurl embed added.
            return
        self._plug.queue(DiscordMessage.from_message(self._plug, after, edited=True))

    async def on_message_delete(self, message):
        log.debug("Received a deleted message")
        self._plug.queue(DiscordMessage.from_message(self._plug, message, deleted=True))


class DiscordPlug(immp.Plug):
    """
    Plug for a `Discord <https://discordapp.com>`_ server.
    """

    network_name = "Discord"

    @property
    def network_id(self):
        return "discord:{}".format(self._client.user.id) if self._client else None

    def __init__(self, name, config, host):
        super().__init__(name, _Schema.config(config), host)
        # Connection objects that need to be closed on disconnect.
        self._client = self._task = self._session = None
        self._starting = Condition()

    async def start(self):
        await super().start()
        if self.config["webhooks"]:
            self._session = ClientSession()
        log.debug("Starting client")
        self._client = DiscordClient(self)
        self._task = ensure_future(self._client.start(self.config["token"], bot=self.config["bot"]))
        async with self._starting:
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

    async def user_from_id(self, id):
        user = await self._client.get_user_info(id)
        return DiscordUser.from_user(self, user) if user else None

    async def user_from_username(self, username):
        for guild in self._client.guilds:
            member = guild.get_member_named(username)
            if member:
                return DiscordUser.from_user(self, member)
        return None

    def _get_channel(self, channel):
        return self._client.get_channel(int(channel.source))

    async def channel_for_user(self, user):
        if not isinstance(user, DiscordUser):
            return None
        if not isinstance(user.raw, (discord.Member, discord.User)):
            return None
        dm = user.raw.dm_channel or (await user.raw.create_dm())
        return immp.Channel(self, dm.id)

    async def channel_title(self, channel):
        dc_channel = self._get_channel(channel)
        return dc_channel.name if dc_channel else None

    async def channel_link(self, channel):
        dc_channel = self._get_channel(channel)
        return ("https://discordapp.com/channels/{}/{}".format(dc_channel.guild.id, dc_channel.id)
                if dc_channel else None)

    async def channel_rename(self, channel, title):
        dc_channel = self._get_channel(channel)
        if dc_channel:
            await dc_channel.edit(name=title)

    async def channel_is_private(self, channel):
        dc_channel = self._get_channel(channel)
        return isinstance(dc_channel, discord.DMChannel)

    async def channel_members(self, channel):
        if channel.plug is not self:
            return None
        dc_channel = self._get_channel(channel)
        if dc_channel:
            return [DiscordUser.from_user(self, member) for member in dc_channel.members]
        else:
            return []

    def _resolve_channel(self, channel):
        dc_channel = self._get_channel(channel)
        webhook = None
        for label, host_channel in self.host.channels.items():
            if channel == host_channel:
                webhook = self.config["webhooks"].get(label)
                break
        return dc_channel, webhook

    async def _resolve_message(self, dc_channel, msg):
        if isinstance(msg, immp.SentMessage):
            # Discord offers no reply mechanism, so instead we just fetch the referenced message
            # and render it manually.
            message = await dc_channel.get_message(msg.id)
            return DiscordMessage.from_message(discord, message)
        elif isinstance(msg, immp.Message):
            return msg

    async def _put_webhook(self, dc_channel, webhook, msg):
        name = image = rich = None
        if msg.user:
            name = msg.user.real_name or msg.user.username
            image = msg.user.avatar
        if msg.text:
            rich = msg.text.clone()
            if msg.action:
                for segment in rich:
                    segment.italic = True
        data = FormData()
        payload = {}
        embeds = []
        if msg.attachments:
            for i, attach in enumerate(msg.attachments):
                if isinstance(attach, immp.File) and attach.type == immp.File.Type.image:
                    img_resp = await attach.get_content(self._session)
                    filename = attach.title or "image_{}.png".format(i)
                    data.add_field("file_{}".format(i), img_resp.content, filename=filename)
                elif isinstance(attach, immp.Location):
                    embeds.append({"title": attach.name or "Location",
                                   "url": attach.google_map_url,
                                   "description": attach.address,
                                   "thumbnail": {"url": attach.google_image_url(80)},
                                   "footer": {"text": "{}, {}".format(attach.latitude,
                                                                      attach.longitude)}})
                elif isinstance(attach, immp.Message):
                    resolved = await self._resolve_message(dc_channel, attach)
                    embeds.append(await DiscordMessage.to_webhook_embed(self, resolved))
        if msg.reply_to:
            resolved = await self._resolve_message(dc_channel, msg.reply_to)
            embeds.append(await DiscordMessage.to_webhook_embed(self, resolved, True))
        # Null values aren't accepted, only add name/image to data if they're set.
        if name:
            payload["username"] = name
        if image:
            payload["avatar_url"] = image
        if rich:
            if isinstance(msg, immp.SentMessage) and msg.edited:
                rich.append(immp.Segment(" (edited)", italic=True))
            payload["content"] = DiscordRichText.to_markdown(self, rich.normalise())
        if embeds:
            payload["embeds"] = embeds
        data.add_field("payload_json", json_dumps(payload))
        async with self._session.post("{}?wait=true".format(webhook), data=data) as resp:
            json = await resp.json()
        message = _Schema.webhook(json)
        if "code" in message:
            raise DiscordAPIError("{}: {}".format(message["code"], message["message"]))
        return [message["id"]]

    async def _requests(self, dc_channel, msg):
        embeds = []
        if msg.attachments:
            for i, attach in enumerate(msg.attachments):
                if isinstance(attach, immp.File) and attach.type == immp.File.Type.image:
                    img_resp = await attach.get_content(self._session)
                    filename = attach.title or "image_{}.png".format(i)
                    embeds.append((None, discord.File(img_resp.content, filename), "an image"))
                elif isinstance(attach, immp.Location):
                    embed = discord.Embed()
                    embed.title = attach.name or "Location"
                    embed.url = attach.google_map_url
                    embed.description = attach.address
                    embed.set_thumbnail(url=attach.google_image_url(80))
                    embed.set_footer(text="{}, {}".format(attach.latitude, attach.longitude))
                    embeds.append((embed, None, "a location"))
        requests = []
        if msg.text or msg.reply_to:
            rich = msg.render(quote_reply=True)
            embed = file = None
            if len(embeds) == 1:
                # Attach the only embed to the message text.
                embed, file, _ = embeds.pop()
            requests.append(dc_channel.send(content=DiscordRichText.to_markdown(self, rich),
                                            embed=embed, file=file))
        for embed, file, desc in embeds:
            # Send any additional embeds in their own separate messages.
            content = None
            if msg.user:
                label = immp.Message(user=msg.user, text="sent {}".format(desc), action=True)
                content = DiscordRichText.to_markdown(self, label.render())
            requests.append(dc_channel.send(content=content, embed=embed, file=file))
        return requests

    async def _put_client(self, dc_channel, msg):
        requests = []
        for attach in msg.attachments:
            # Generate requests for attached messages first.
            if isinstance(attach, immp.Message):
                resolved = await self._resolve_message(dc_channel, attach)
                requests += await self._requests(dc_channel, resolved)
        own_requests = await self._requests(dc_channel, msg)
        if requests and not own_requests:
            # Forwarding a message but no content to show who forwarded it.
            info = immp.Message(user=msg.user, action=True, text="forwarded a message")
            own_requests = await self._requests(dc_channel, info)
        requests += own_requests
        sent = []
        for request in requests:
            sent.append(await request)
        return [str(resp.id) for resp in sent]

    async def put(self, channel, msg):
        dc_channel, webhook = self._resolve_channel(channel)
        webhook = None
        for label, host_channel in self.host.channels.items():
            if channel == host_channel:
                webhook = self.config["webhooks"].get(label)
                break
        if webhook:
            log.debug("Sending to {} via webhook".format(repr(channel)))
            return await self._put_webhook(dc_channel, webhook, msg)
        elif dc_channel:
            log.debug("Sending to {} via client".format(repr(channel)))
            return await self._put_client(dc_channel, msg)
        else:
            raise DiscordAPIError("No access to channel {}".format(channel.source))

    async def delete(self, sent):
        dc_channel, webhook = self._resolve_channel(sent.channel)
        if not dc_channel:
            raise DiscordAPIError("No access to channel {}".format(sent.channel.source))
        message = await dc_channel.get_message(sent.id)
        # If not self-posted (including webhooks), the Manage Messages permission is required.
        await message.delete()
