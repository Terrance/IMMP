"""
Connect to `Discord <https://discord.com>`_ as a bot.

Requirements:
    Extra name: ``discord``

    `discord.py <https://discordpy.readthedocs.io>`_

Config:
    token (str):
        Discord token for the bot user.
    message-content (bool):
        Whether to use the privileged message content intent to receive the content of incoming
        messages.  Without this, incoming messages will have no message text or attachments.

        Before enabling here, you must enable the intent in Discord's developer site, otherwise the
        underlying client will fail to connect.
    members (bool):
        Whether to use the privileged members intent to retrieve per-server member information.
        Without this, per-server nicknames and channel member lists will be unavailable.

        Before enabling here, you must enable the intent in Discord's developer site, otherwise the
        underlying client will fail to connect.  For bots in large numbers of servers, this may
        cause a significantly longer startup time whilst members are initially retrieved.
    webhooks ((str, str) dict):
        Mapping from named Discord channels to webhook URLs, needed for custom message author names
        and avatars.
    playing (str):
        Optional game activity message to show as the bot's presence.

Note that the token is neither a client ID nor client secret -- you need to enable bot features for
your app, and collect the token from there.  New apps can be created from the `My Apps
<https://discord.com/developers/applications/me>`_ page in the developer docs.

Channel sources should be Discord's numerical IDs but quoted as strings, e.g. ``"123456789012"``.
You can enable Advanced > Developer Mode in your Discord app settings in order to enable Copy ID
menu options in various places across the UI.

Because gateway connections can't customise the sender when pushing new messages, you may also want
an `incoming webhook <https://discord.com/developers/docs/resources/webhook>`_  configured for each
channel you intend to send messages to.  A new webhook can be created over the API, or in the UI
via Edit Channel > Webhooks.  A fallback style incorporating the user's name in the message text
will be used in lieu of a webhook, e.g. with direct messages.
"""

from asyncio import Condition, ensure_future, gather
from collections import defaultdict
from datetime import timezone
from functools import partial
from io import BytesIO
import logging
import re

import discord as discordpy
from emoji import emojize

import immp


log = logging.getLogger(__name__)


class _Schema:

    config = immp.Schema({"token": str,
                          immp.Optional("message-content", True): bool,
                          immp.Optional("members", False): bool,
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
        real_name = user.display_name
        avatar = user.avatar.url if user.avatar else None
        link = "https://discord.com/users/{}".format(user.id)
        return cls(id_=user.id,
                   plug=discord,
                   username=username,
                   real_name=real_name,
                   avatar=avatar,
                   link=link,
                   raw=user)


class DiscordRichText(immp.RichText):

    base_tags = {"**": "bold", "_": "italic", "__": "underline", "~~": "strike",
                 "`": "code", "```": "pre"}
    all_tags = dict({"*": "italic"}, **base_tags)
    # A rather complicated expression to match formatting tags according to the following rules:
    # 1) Outside of formatting may not be adjacent to alphanumeric or other formatting characters.
    # 2) Inside of formatting may not be adjacent to whitespace or the current formatting character.
    # 3) Formatting characters may be escaped with a backslash.
    # This still isn't perfect, but provides a good approximation outside of edge cases.
    _outside_chars = r"0-9a-z*_~"
    _tag_chars = r"\*\*|__|~~|[*_`]"
    _inside_chars = r"\s\1"
    _format_regex = re.compile(r"(?<![{0}\\])({1})(?![{2}])(.+?)(?<![{2}\\])\1(?![{0}])"
                               .format(_outside_chars, _tag_chars, _inside_chars))
    _pre_regex = re.compile(r"```\n?(.+?)\n?```", re.DOTALL)

    _mention_regex = re.compile(r"<@!?(\d+)>")
    _role_mention_regex = re.compile(r"<@&(\d+)>")
    _channel_regex = re.compile(r"<#(\d+)>")
    _emoji_regex = re.compile(r"<(:[^: ]+?:)\d+>")

    @classmethod
    def _sub_role_mention(cls, roles, match):
        return "@{}".format(roles.get(int(match.group(1)), "&{}".format(match.group(1))))

    @classmethod
    def _sub_channel(cls, discord, match):
        return "#{}".format(discord._client.get_channel(int(match.group(1))).name)

    @classmethod
    def from_message(cls, discord, message):
        """
        Convert a string of Markdown from a Discord message into a :class:`.RichText`.

        Args:
            discord (.DiscordPlug):
                Related plug instance that provides the text.
            message (discord.Message):
                Containing message object, in order to resolve mentions.

        Returns:
            .DiscordRichText:
                Parsed rich text container.
        """
        text = message.content
        changes = defaultdict(dict)
        plain = ""
        done = False
        while not done:
            # Identify pre blocks, parse formatting only outside of them.
            match = cls._pre_regex.search(text)
            if match:
                parse = text[:match.start()]
                pre = match.group(1)
                text = text[match.end():]
            else:
                parse = text
                done = True
            offset = len(plain)
            while True:
                match = cls._format_regex.search(parse)
                if not match:
                    break
                start = match.start()
                end = match.end()
                tag = match.group(1)
                # Strip the tag characters from the message.
                parse = parse[:start] + match.group(2) + parse[end:]
                end -= 2 * len(tag)
                # Record the range where the format is applied.
                field = cls.all_tags[tag]
                changes[offset + start][field] = True
                changes[offset + end][field] = False
                # Shift any future tags back.
                for pos in sorted(changes):
                    if pos > offset + end:
                        changes[pos - 2 * len(tag)].update(changes.pop(pos))
            plain += parse
            if not done:
                changes[len(plain)]["pre"] = True
                changes[len(plain + pre)]["pre"] = False
                plain += pre
        mentioned = {user.id: user for user in message.mentions}
        for match in cls._mention_regex.finditer(plain):
            id_ = int(match.group(1))
            if id_ in mentioned:
                user = mentioned[id_]
            else:
                user = mentioned[id_] = discord._client.get_user(id_)
            if user:
                changes[match.start()]["mention"] = DiscordUser.from_user(discord, user)
                changes[match.end()]["mention"] = None
        segments = []
        points = list(sorted(changes.keys()))
        formatting = {}
        roles = {role.id: role.name for role in message.role_mentions}
        # Iterate through text in change start/end pairs.
        for start, end in zip([0] + points, points + [len(plain)]):
            formatting.update(changes[start])
            if start == end:
                # Zero-length segment at the start or end, ignore it.
                continue
            if formatting.get("mention"):
                user = formatting["mention"]
                part = "@{}".format(user.real_name or user.username)
            else:
                part = emojize(plain[start:end], use_aliases=True)
                # Strip Discord channel/emoji tags, replace with a plain text representation.
                part = cls._role_mention_regex.sub(partial(cls._sub_role_mention, roles), part)
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
                attr = cls.base_tags[tag]
                if not getattr(segment, attr):
                    text += tag
                    active.remove(tag)
            for tag, attr in cls.base_tags.items():
                # Skip duplicate form of italic.
                if tag == "*":
                    continue
                # Add any new tags that start at this segment.
                if getattr(segment, attr) and tag not in active:
                    text += tag
                    if tag == "```":
                        # First line of pre block would set the code language and be hidden.
                        text += "\n"
                    active.append(tag)
            parsed = segment.text
            if not segment.code and not segment.pre:
                link = None
                if not segment.mention:
                    link = segment.link
                elif segment.mention.plug.network_name == discord.network_name:
                    parsed = "<@{}>".format(segment.mention.id)
                else:
                    link = segment.mention.link
                if link:
                    if webhook:
                        parsed = "[{}]({})".format(segment.text, link)
                    elif segment.text == segment.link:
                        pass
                    elif segment.text_is_link:
                        # Implicitly add the protocol to gain automatic linking.
                        parsed = segment.link
                    else:
                        parsed = "{} [{}]".format(segment.text, link)
            text += parsed
        for tag in reversed(active):
            # Close all remaining tags.
            text += tag
        return re.sub(r":([^: ]+):", partial(cls._sub_emoji, discord), text)


class DiscordMessage(immp.Message):
    """
    Message originating from Discord.
    """

    @classmethod
    async def from_message(cls, discord, message, edited=False, deleted=False):
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
        text = reply_to = None
        channel = immp.Channel(discord, message.channel.id)
        user = DiscordUser.from_user(discord, message.author)
        attachments = []
        if message.content:
            text = DiscordRichText.from_message(discord, message)
        if message.reference and not message.flags.is_crossposted:
            receipt = immp.Receipt(message.reference.message_id,
                                   immp.Channel(discord, message.reference.channel_id))
            reply_to = await discord.get_message(receipt)
        for attach in message.attachments:
            if attach.filename.endswith((".jpg", ".png", ".gif")):
                type_ = immp.File.Type.image
            elif attach.filename.endswith((".mp4", ".webm")):
                type_ = immp.File.Type.video
            else:
                type_ = immp.File.Type.unknown
            attachments.append(immp.File(title=attach.filename,
                                         type_=type_,
                                         source=attach.url))
        for embed in message.embeds:
            if embed.image.url and embed.image.url.rsplit(".", 1)[1] in ("jpg", "png", "gif"):
                attachments.append(immp.File(type_=immp.File.Type.image,
                                             source=embed.image.url))
        return immp.SentMessage(id_=message.id,
                                channel=channel,
                                at=message.created_at,
                                # Edited timestamp is blank for new messages, but updated in
                                # existing objects when the message is later edited.
                                revision=(message.edited_at or message.created_at).timestamp(),
                                edited=edited,
                                deleted=deleted,
                                text=text,
                                user=user,
                                reply_to=reply_to,
                                attachments=attachments,
                                raw=message)

    @classmethod
    async def to_embed(cls, discord, msg, reply=False):
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
            discord.Embed:
                Discord API `embed <https://discord.com/developers/docs/resources/channel>`_
                object.
        """
        icon = "\N{RIGHTWARDS ARROW WITH HOOK}" if reply else "\N{SPEECH BALLOON}"
        embed = discordpy.Embed()
        embed.set_footer(text=icon)
        if isinstance(msg, immp.Receipt):
            embed.timestamp = msg.at
        if msg.user:
            name = msg.user.real_name or msg.user.username
            link = icon = None
            # Exclude platform-specific join protocol URLs.
            if (msg.user.link or "").startswith("http"):
                link = msg.user.link
            if msg.user.avatar:
                icon = msg.user.avatar.url
            embed.set_author(name=name, url=link, icon_url=icon)
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
            embed.description = DiscordRichText.to_markdown(discord, quote)
        return embed


class DiscordClient(discordpy.Client):
    """
    Subclass of the underlying client to bind events.
    """

    def __init__(self, plug, **kwargs):
        super().__init__(**kwargs)
        self._plug = plug

    async def on_connect(self):
        async with self._plug._starting:
            self._plug._starting.notify_all()

    on_disconnect = on_connect

    async def on_ready(self):
        await self.on_resume()

    async def on_resume(self):
        if self._plug.config["playing"]:
            await self.change_presence(activity=discordpy.Game(self._plug.config["playing"]))

    async def on_message(self, message):
        log.debug("Received a new message")
        self._plug.queue(await DiscordMessage.from_message(self._plug, message))

    async def on_message_edit(self, before, after):
        log.debug("Received an updated message")
        if before.content == after.content:
            # Text content hasn't changed -- maybe just a link unfurl embed added.
            return
        self._plug.queue(await DiscordMessage.from_message(self._plug, after, edited=True))

    async def on_message_delete(self, message):
        log.debug("Received a deleted message")
        self._plug.queue(await DiscordMessage.from_message(self._plug, message, deleted=True))


class DiscordPlug(immp.Plug, immp.HTTPOpenable):
    """
    Plug for a `Discord <https://discord.com>`_ server.
    """

    schema = _Schema.config

    network_name = "Discord"

    @property
    def network_id(self):
        return ("discord:{}".format(self._client.user.id)
                if self._client and self._client.user else None)

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        # Connection objects that need to be closed on disconnect.
        self._client = self._task = None
        self._starting = Condition()

    async def start(self):
        await super().start()
        log.debug("Starting client")
        intents = discordpy.Intents.default()
        intents.message_content = self.config["message-content"]
        intents.members = self.config["members"]
        self._client = DiscordClient(self, intents=intents)
        self._task = ensure_future(self._client.start(self.config["token"]))
        async with self._starting:
            # Block until the client is ready.
            await self._starting.wait()
        if self._task.done():
            # Raise connection errors (e.g. missing privileged intents).
            self._task.result()

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
                if isinstance(channel, discordpy.TextChannel)]

    async def private_channels(self):
        return [immp.Channel(self, channel.id) for channel in self._client.private_channels]

    def _get_channel(self, channel):
        return self._client.get_channel(int(channel.source))

    async def channel_for_user(self, user):
        if not isinstance(user, DiscordUser):
            return None
        if not isinstance(user.raw, (discordpy.Member, discordpy.User)):
            return None
        dm = user.raw.dm_channel or (await user.raw.create_dm())
        return immp.Channel(self, dm.id)

    async def channel_title(self, channel):
        dc_channel = self._get_channel(channel)
        return dc_channel.name if isinstance(dc_channel, discordpy.TextChannel) else None

    async def channel_link(self, channel):
        dc_channel = self._get_channel(channel)
        if isinstance(dc_channel, discordpy.TextChannel):
            guild = dc_channel.guild.id
        elif isinstance(dc_channel, (discordpy.DMChannel, discordpy.GroupChannel)):
            guild = "@me"
        else:
            return None
        return "https://discord.com/channels/{}/{}".format(guild, dc_channel.id)

    async def channel_rename(self, channel, title):
        dc_channel = self._get_channel(channel)
        if isinstance(dc_channel, discordpy.TextChannel):
            await dc_channel.edit(name=title)

    async def channel_is_private(self, channel):
        dc_channel = self._get_channel(channel)
        return isinstance(dc_channel, discordpy.DMChannel)

    async def channel_members(self, channel):
        dc_channel = self._get_channel(channel)
        if isinstance(dc_channel, discordpy.TextChannel):
            return [DiscordUser.from_user(self, member) for member in dc_channel.members]
        elif isinstance(dc_channel, discordpy.GroupChannel):
            return ([DiscordUser.from_user(self, dc_channel.me)] +
                    [DiscordUser.from_user(self, member) for member in dc_channel.recipients])
        elif isinstance(dc_channel, discordpy.DMChannel):
            return [DiscordUser.from_user(self, dc_channel.me),
                    DiscordUser.from_user(self, dc_channel.recipient)]
        else:
            return None

    async def channel_admins(self, channel):
        dc_channel = self._get_channel(channel)
        if isinstance(dc_channel, discordpy.TextChannel):
            members = dc_channel.members
        elif isinstance(dc_channel, discordpy.GroupChannel):
            members = [dc_channel.me] + dc_channel.recipients
        else:
            return None
        perms = {user: dc_channel.permissions_for(user) for user in members}
        return [DiscordUser.from_user(self, user) for user, perm in perms.items()
                if perm.administrator or perm.manage_messages]

    async def channel_link_create(self, channel, shared=True):
        dc_channel = self._get_channel(channel)
        invite = await dc_channel.create_invite(max_uses=(0 if shared else 1),
                                                unique=(not shared))
        log.debug("Created invite link for %r: %r", channel.source, invite.url)
        return invite.url

    async def channel_link_revoke(self, channel, link=None):
        if not link:
            return
        dc_channel = self._get_channel(channel)
        for invite in await dc_channel.invites():
            if invite.url == link:
                await invite.delete()
                log.debug("Revoked invite link for %r: %r", channel.source, link)
                break

    async def channel_history(self, channel, before=None):
        dc_channel = self._get_channel(channel)
        dc_before = await dc_channel.fetch_message(before.id) if before else None
        history = dc_channel.history(before=dc_before, oldest_first=False)
        messages = await gather(*[DiscordMessage.from_message(self, message)
                                  async for message in history])
        return list(reversed(messages))

    async def get_message(self, receipt):
        dc_channel = self._get_channel(receipt.channel)
        if dc_channel is None:
            log.debug("Channel %r of message not available", receipt.channel)
            return None
        message = await dc_channel.fetch_message(receipt.id)
        return await DiscordMessage.from_message(self, message)

    def _resolve_channel(self, channel):
        dc_channel = self._get_channel(channel)
        webhook = None
        for label, host_channel in self.host.channels.items():
            if channel == host_channel and label in self.config["webhooks"]:
                webhook = discordpy.Webhook.from_url(self.config["webhooks"][label],
                                                     session=self.session,
                                                     bot_token=self.config["token"])
                break
        return dc_channel, webhook

    async def _requests(self, dc_channel, webhook, msg):
        name = image = None
        reply_to = reply_ref = reply_embed = None
        embeds = []
        files = []
        if msg.user:
            name = msg.user.real_name or msg.user.username
            image = msg.user.avatar
        for i, attach in enumerate(msg.attachments or []):
            if isinstance(attach, immp.File):
                if attach.title:
                    title = attach.title
                elif attach.type == immp.File.Type.image:
                    title = "image_{}.png".format(i)
                elif attach.type == immp.File.Type.video:
                    title = "video_{}.mp4".format(i)
                else:
                    title = "file_{}".format(i)
                async with (await attach.get_content(self.session)) as img_content:
                    # discord.py expects a file-like object with a synchronous read() method.
                    # NB. The whole file is read into memory by discord.py anyway.
                    files.append(discordpy.File(BytesIO(await img_content.read()), title))
            elif isinstance(attach, immp.Location):
                embed = discordpy.Embed()
                embed.title = attach.name or "Location"
                embed.url = attach.google_map_url
                embed.description = attach.address
                embed.set_thumbnail(url=attach.google_image_url(80))
                embed.set_footer(text="{}, {}".format(attach.latitude, attach.longitude))
                embeds.append((embed, "sent a location"))
            elif isinstance(attach, immp.Message):
                resolved = await self.resolve_message(attach)
                embeds.append((await DiscordMessage.to_embed(self, resolved), "sent a message"))
        if msg.reply_to:
            if isinstance(msg.reply_to, immp.Receipt):
                if msg.reply_to.channel.plug.network_id == self.network_id:
                    guild_id = dc_channel.guild.id if dc_channel.guild else None
                    reply_ref = discordpy.MessageReference(message_id=int(msg.reply_to.id),
                                                           channel_id=dc_channel.id,
                                                           guild_id=guild_id)
            if not reply_to:
                reply_to = msg.reply_to
            reply_embed = await DiscordMessage.to_embed(self, reply_to, True)
        if webhook and msg.user:
            # Sending via webhook: multiple embeds and files supported.
            requests = []
            rich = None
            if reply_embed:
                # Webhooks can't reply to other messages, quote the target in an embed instead.
                # https://github.com/discord/discord-api-docs/issues/2251
                embeds.append((reply_embed, None))
            if msg.text:
                rich = msg.text.clone()
                if msg.action:
                    for segment in rich:
                        segment.italic = True
            if msg.edited:
                if rich:
                    rich.append(immp.Segment(" "))
                else:
                    rich = immp.RichText()
                rich.append(immp.Segment("(edited)", italic=True))
            text = None
            if rich:
                mark = DiscordRichText.to_markdown(self, rich, True)
                chunks = immp.RichText.chunked_plain(mark, 2000)
                if len(chunks) > 1:
                    # Multiple messages required to accommodate the text.
                    requests.extend(webhook.send(content=chunk, wait=True, username=name,
                                                 avatar_url=image) for chunk in chunks)
                else:
                    text = chunks[0]
            if text or embeds or files:
                requests.append(webhook.send(content=text, wait=True, username=name,
                                             avatar_url=image, files=files,
                                             embeds=[embed[0] for embed in embeds]))
            return requests
        else:
            # Sending via client: only a single embed per message.
            requests = []
            text = embed = desc = None
            chunks = []
            rich = msg.render(link_name=False, edit=msg.edited) or None
            if rich:
                mark = DiscordRichText.to_markdown(self, rich)
                text, *chunks = immp.RichText.chunked_plain(mark, 2000)
            if reply_embed and not reply_ref:
                embeds.append((reply_embed, None))
            if len(embeds) == 1:
                # Attach the only embed to the message text.
                embed, desc = embeds.pop()
            if text or embed or files:
                # Primary message: set reference for reply-to if applicable.
                requests.append(dc_channel.send(content=text or desc, embed=embed, files=files,
                                                reference=reply_ref))
            # Send the remaining text if multiple messages were required to accommodate it.
            requests.extend(dc_channel.send(content=chunk) for chunk in chunks)
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
        receipts = []
        for request in requests:
            message = await request
            if not message.channel:
                # Webhook-sent messages won't have their channel set.
                message.channel = dc_channel
            receipts.append(await DiscordMessage.from_message(self, message))
        return receipts

    async def delete(self, sent):
        dc_channel = self._resolve_channel(sent.channel)[0]
        if not dc_channel:
            raise DiscordAPIError("No access to channel {}".format(sent.channel.source))
        message = await dc_channel.fetch_message(sent.id)
        # If not self-posted (including webhooks), the Manage Messages permission is required.
        await message.delete()
