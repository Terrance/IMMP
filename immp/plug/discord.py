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
    This plug requires the `discord.py <https://discordpy.readthedocs.io>`_ Python module.
"""

from asyncio import Condition, ensure_future
from collections import defaultdict
from datetime import timezone
from functools import partial
from io import BytesIO
import logging
import re
from textwrap import wrap

import discord
from emoji import emojize

import immp


log = logging.getLogger(__name__)


class _Schema:

    config = immp.Schema({"token": str,
                          immp.Optional("bot", True): bool,
                          immp.Optional("webhooks", dict): {str: str},
                          immp.Optional("playing"): immp.Nullable(str)})

    webhook = immp.Schema(immp.Any({"code": int, "message": str}, {"id": str}))


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
        # Avatar URL is an Asset object, URL only available via __str__.
        avatar = str(user.avatar_url) if user.avatar_url else None
        return cls(id_=user.id,
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

    _mention_regex = re.compile(r"<@!?(\d+)>")
    _channel_regex = re.compile(r"<#(\d+)>")
    _emoji_regex = re.compile(r"<(:[^: ]+?:)\d+>")

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
        for match in cls._mention_regex.finditer(text):
            user = discord._client.get_user(int(match.group(1)))
            if user:
                changes[match.start()]["mention"] = DiscordUser.from_user(discord, user)
                changes[match.end()]["mention"] = None
        segments = []
        points = list(sorted(changes.keys()))
        formatting = {}
        # Iterate through text in change start/end pairs.
        for start, end in zip([0] + points, points + [len(text)]):
            formatting.update(changes[start])
            if start == end:
                # Zero-length segment at the start or end, ignore it.
                continue
            if formatting.get("mention"):
                user = formatting["mention"]
                part = "@{}".format(user.username or user.real_name)
            else:
                part = emojize(text[start:end], use_aliases=True)
                # Strip Discord channel/emoji tags, replace with a plain text representation.
                part = cls._channel_regex.sub(partial(cls._sub_channel, discord), part)
                part = cls._emoji_regex.sub(r"\1", part)
            segments.append(immp.Segment(part, **formatting))
        return cls(segments)

    @classmethod
    def _sub_emoji(cls, discord, match):
        for emoji in discord._client.emojis:
            if emoji.name == match.group(1):
                return str(emoji)
        return ":{}:".format(match.group(1))

    @classmethod
    def to_markdown(cls, discord, rich, webhook=False):
        """
        Convert a :class:`.RichText` instance into a Markdown string.

        Args:
            discord (.DiscordPlug):
                Related plug instance to cross-reference users.
            rich (.DiscordRichText):
                Parsed rich text container.
            webhook (bool):
                ``True`` if being sent via a webhook, which allows use of hyperlinks.

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
                if webhook:
                    text += "[{}]({})".format(segment.text, segment.link)
                elif segment.text == segment.link:
                    text += segment.text
                else:
                    text += "{} [{}]".format(segment.text, segment.link)
            else:
                text += segment.text
        for tag in reversed(active):
            # Close all remaining tags.
            text += tag
        return re.sub(r":([^: ]+):", partial(cls._sub_emoji, discord), text)

    @classmethod
    def chunk_split(cls, text):
        """
        Split long messages into parts of at most 2000 characters.

        Args:
            text (str):
                Markdown-formatted message to be sent.

        Returns:
            str list:
                Chunked message text.
        """
        parts = []
        current = []
        for line in text.splitlines():
            size = sum(len(part) + 1 for part in current)
            extra = len(line)
            if size + extra >= 2000:
                if current:
                    # The message is full, split here.
                    parts.append("\n".join(current))
                    current.clear()
                if extra >= 2000:
                    # The line itself is too long, split on whitespace instead.
                    *lines, line = wrap(line, 2000, expand_tabs=False, replace_whitespace=False)
                    parts.extend(lines)
            current.append(line)
        if current:
            parts.append("\n".join(current))
        return parts


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
            type_ = immp.File.Type.unknown
            if attach.filename.endswith((".jpg", ".png", ".gif")):
                type_ = immp.File.Type.image
            attachments.append(immp.File(title=attach.filename,
                                         type_=type_,
                                         source=attach.url))
        for embed in message.embeds:
            if embed.image.url and embed.image.url.rsplit(".", 1)[1] in ("jpg", "png", "gif"):
                attachments.append(immp.File(type_=immp.File.Type.image,
                                             source=embed.image.url))
        return immp.SentMessage(id_=message.id,
                                channel=immp.Channel(discord, message.channel.id),
                                # Timestamps are naive but in UTC.
                                at=message.created_at.replace(tzinfo=timezone.utc),
                                # Edited timestamp is blank for new messages, but updated in
                                # existing objects when the message is later edited.
                                revision=(message.edited_at or message.created_at).timestamp(),
                                edited=edited,
                                deleted=deleted,
                                text=text,
                                user=user,
                                attachments=attachments,
                                raw=message)

    @classmethod
    async def to_embed(cls, discord_, msg, reply=False):
        """
        Convert a :class:`.Message` to a message embed structure, suitable for embedding within an
        outgoing message.

        Args:
            discord_ (.DiscordPlug):
                Target plug instance for this attachment.
            msg (.Message):
                Original message from another plug or hook.
            reply (bool):
                Whether to show a reply icon instead of a quote icon.

        Returns.
            discord.Embed:
                Discord API `embed <https://discordapp.com/developers/docs/resources/channel>`_
                object.
        """
        icon = "\N{RIGHTWARDS ARROW WITH HOOK}" if reply else "\N{SPEECH BALLOON}"
        embed = discord.Embed()
        embed.set_footer(text=icon)
        if isinstance(msg, immp.Receipt):
            embed.timestamp = msg.at
        if msg.user:
            link = discord.Embed.Empty
            # Exclude platform-specific join protocol URLs.
            if (msg.user.link or "").startswith("http"):
                link = msg.user.link
            embed.set_author(name=(msg.user.real_name or msg.user.username),
                             url=link, icon_url=msg.user.avatar or discord.Embed.Empty)
        quote = None
        action = False
        if msg.text:
            quote = msg.text.clone()
            action = msg.action
        elif msg.attachments:
            count = len(msg.attachments)
            what = "{} attachment".format(count) if count > 1 else "this attachment"
            quote = immp.RichText([immp.Segment("sent {}".format(what))])
            action = True
        if quote:
            if action:
                for segment in quote:
                    segment.italic = True
            embed.description = DiscordRichText.to_markdown(discord_, quote)
        return embed


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


class DiscordPlug(immp.HTTPOpenable, immp.Plug):
    """
    Plug for a `Discord <https://discordapp.com>`_ server.
    """

    schema = _Schema.config

    network_name = "Discord"

    @property
    def network_id(self):
        return "discord:{}".format(self._client.user.id) if self._client else None

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        # Connection objects that need to be closed on disconnect.
        self._client = self._task = None
        self._starting = Condition()

    async def start(self):
        await super().start()
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

    async def user_from_id(self, id_):
        user = await self._client.fetch_user(id_)
        return DiscordUser.from_user(self, user) if user else None

    async def user_from_username(self, username):
        for guild in self._client.guilds:
            member = guild.get_member_named(username)
            if member:
                return DiscordUser.from_user(self, member)
        return None

    async def user_is_system(self, user):
        return user.id == str(self._client.user.id)

    async def public_channels(self):
        return [immp.Channel(self, channel.id) for channel in self._client.get_all_channels()
                if isinstance(channel, discord.TextChannel)]

    async def private_channels(self):
        return [immp.Channel(self, channel.id) for channel in self._client.private_channels]

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
        return dc_channel.name if isinstance(dc_channel, discord.TextChannel) else None

    async def channel_link(self, channel):
        dc_channel = self._get_channel(channel)
        if isinstance(dc_channel, discord.TextChannel):
            guild = dc_channel.guild.id
        elif isinstance(dc_channel, (discord.DMChannel, discord.GroupChannel)):
            guild = "@me"
        else:
            return None
        return "https://discordapp.com/channels/{}/{}".format(guild, dc_channel.id)

    async def channel_rename(self, channel, title):
        dc_channel = self._get_channel(channel)
        if isinstance(dc_channel, discord.TextChannel):
            await dc_channel.edit(name=title)

    async def channel_is_private(self, channel):
        dc_channel = self._get_channel(channel)
        return isinstance(dc_channel, discord.DMChannel)

    async def channel_members(self, channel):
        dc_channel = self._get_channel(channel)
        if isinstance(dc_channel, discord.TextChannel):
            return [DiscordUser.from_user(self, member) for member in dc_channel.members]
        elif isinstance(dc_channel, discord.GroupChannel):
            return [DiscordUser.from_user(self, member) for member in dc_channel.recipients]
        elif isinstance(dc_channel, discord.DMChannel):
            return [DiscordUser.from_user(self, dc_channel.me),
                    DiscordUser.from_user(self, dc_channel.recipient)]
        else:
            return []

    async def channel_history(self, channel, before=None):
        dc_channel = self._get_channel(channel)
        dc_before = await dc_channel.fetch_message(before.id) if before else None
        history = dc_channel.history(before=dc_before, oldest_first=False)
        messages = [DiscordMessage.from_message(self, message) async for message in history]
        return list(reversed(messages))

    def _resolve_channel(self, channel):
        dc_channel = self._get_channel(channel)
        webhook = None
        for label, host_channel in self.host.channels.items():
            if channel == host_channel and label in self.config["webhooks"]:
                adapter = discord.AsyncWebhookAdapter(self.session)
                webhook = discord.Webhook.from_url(self.config["webhooks"][label], adapter=adapter)
                break
        return dc_channel, webhook

    async def _resolve_message(self, dc_channel, msg):
        if isinstance(msg, immp.Receipt):
            # Discord offers no reply mechanism, so instead we just fetch the referenced message
            # and render it manually.
            message = await dc_channel.fetch_message(msg.id)
            return DiscordMessage.from_message(self, message)
        elif isinstance(msg, immp.Message):
            return msg

    async def _requests(self, dc_channel, webhook, msg):
        name = image = None
        embeds = []
        files = []
        if msg.user:
            name = msg.user.real_name or msg.user.username
            image = msg.user.avatar
        for i, attach in enumerate(msg.attachments or []):
            if isinstance(attach, immp.File) and attach.type == immp.File.Type.image:
                async with (await attach.get_content(self.session)) as img_content:
                    # discord.py expects a file-like object with a synchronous read() method.
                    # NB. The whole file is read into memory by discord.py anyway.
                    files.append(discord.File(BytesIO(await img_content.read()),
                                              attach.title or "image_{}.png".format(i)))
            elif isinstance(attach, immp.Location):
                embed = discord.Embed()
                embed.title = attach.name or "Location"
                embed.url = attach.google_map_url
                embed.description = attach.address
                embed.set_thumbnail(url=attach.google_image_url(80))
                embed.set_footer(text="{}, {}".format(attach.latitude, attach.longitude))
                embeds.append((embed, "sent a location"))
            elif isinstance(attach, immp.Message):
                resolved = await self._resolve_message(dc_channel, attach)
                embeds.append((await DiscordMessage.to_embed(self, resolved), "sent a message"))
        if msg.reply_to:
            resolved = await self._resolve_message(dc_channel, msg.reply_to)
            embeds.append((await DiscordMessage.to_embed(self, resolved, True), None))
        edited = msg.edited if isinstance(msg, immp.Receipt) else False
        if webhook:
            # Sending via webhook: multiple embeds and files supported.
            requests = []
            text = None
            if msg.text:
                rich = msg.text.clone()
                if msg.action:
                    for segment in rich:
                        segment.italic = True
                if edited:
                    rich.append(immp.Segment(" (edited)", italic=True))
                lines = DiscordRichText.chunk_split(DiscordRichText.to_markdown(self, rich, True))
                if len(lines) > 1:
                    # Multiple messages required to accommodate the text.
                    requests.extend(webhook.send(content=line, wait=True, username=name,
                                                 avatar_url=image) for line in lines)
                else:
                    text = lines[0]
            if text or embeds or files:
                requests.append(webhook.send(content=text, wait=True, username=name,
                                             avatar_url=image, files=files,
                                             embeds=[embed[0] for embed in embeds]))
            return requests
        else:
            # Sending via client: only a single embed per message.
            requests = []
            text = embed = None
            rich = msg.render(link_name=False, edit=edited) or None
            if rich:
                lines = DiscordRichText.chunk_split(DiscordRichText.to_markdown(self, rich))
                if len(lines) > 1:
                    # Multiple messages required to accommodate the text.
                    requests.extend(dc_channel.send(content=line) for line in lines)
                else:
                    text = lines[0]
            if len(embeds) == 1:
                # Attach the only embed to the message text.
                embed, _ = embeds.pop()
            if text or embed or files:
                requests.append(dc_channel.send(content=text, embed=embed, files=files))
            for embed, desc in embeds:
                # Send any additional embeds in their own separate messages.
                content = None
                if msg.user and desc:
                    label = immp.Message(user=msg.user, text="sent {}".format(desc), action=True)
                    content = DiscordRichText.to_markdown(self, label.render())
                requests.append(dc_channel.send(content=content, embed=embed))
            return requests

    async def put(self, channel, msg):
        dc_channel, webhook = self._resolve_channel(channel)
        requests = await self._requests(dc_channel, webhook, msg)
        sent = []
        for request in requests:
            sent.append(await request)
        return [str(resp.id) for resp in sent]

    async def delete(self, sent):
        dc_channel = self._resolve_channel(sent.channel)[0]
        if not dc_channel:
            raise DiscordAPIError("No access to channel {}".format(sent.channel.source))
        message = await dc_channel.fetch_message(sent.id)
        # If not self-posted (including webhooks), the Manage Messages permission is required.
        await message.delete()
