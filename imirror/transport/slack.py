import asyncio
from collections import defaultdict
from datetime import datetime
import logging
import re

import aiohttp

import imirror


log = logging.getLogger(__name__)


class SlackAPIError(imirror.TransportError):
    """
    Generic error from the Slack API.
    """


class SlackUser(imirror.User):
    """
    User present in Slack.

    Attributes:
        bot_id (str):
            Reference to the Slack integration app for a bot user.
    """

    def __init__(self, id, username=None, real_name=None, avatar=None, bot_id=None, raw=None):
        super().__init__(id, username=username, real_name=real_name, avatar=avatar, raw=raw)
        self.bot_id = bot_id

    @classmethod
    def from_member(cls, slack, member):
        """
        Convert an API member :class:`dict` to a :class:`.User`.

        Args:
            slack (.SlackTransport):
                Related transport instance that provides the user.
            member (dict):
                Slack API `user <https://api.slack.com/types/user>`_ object.

        Returns:
            .SlackUser:
                Parsed user object.
        """
        id = member.get("id")
        username = member.get("name")
        profile = member.get("profile", {})
        real_name = profile.get("real_name")
        avatar = profile.get("image_512")
        bot_id = profile.get("bot_id")
        return cls(id, username=username, real_name=real_name, avatar=avatar, bot_id=bot_id,
                   raw=member)


class SlackRichText(imirror.RichText):
    """
    Wrapper for Slack-specific parsing of formatting.
    """

    tags = {"*": "bold", "_": "italic", "~": "strike", "`": "code", "```": "pre"}
    # A rather complicated expression to match formatting tags according to the following rules:
    # 1) Outside of formatting may not be adjacent to alphanumeric or other formatting characters.
    # 2) Inside of formatting may not be adjacent to whitespace or the current formatting character.
    # 3) Formatting characters may be escaped with a backslash.
    # This still isn't perfect, but provides a good approximation outside of edge cases.
    # Slack only has limited documentation: https://get.slack.help/hc/en-us/articles/202288908
    _outside_chars = r"0-9a-z*_~"
    _tag_chars = r"*_~`"
    _inside_chars = r"\s\1"
    _format_regex = re.compile(r"(?<![{0}\\])(```|[{1}])(?![{2}])(.+?)(?<![{2}\\])\1(?![{0}])"
                               .format(_outside_chars, _tag_chars, _inside_chars))

    @classmethod
    def from_mrkdwn(cls, text):
        """
        Convert a string of Slack's Mrkdwn into a :class:`.RichText`.

        Args:
            text (str):
                Slack-style formatted text.

        Returns:
            .SlackRichText:
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
        segments = []
        points = list(changes.keys())
        # Iterate through text in change start/end pairs.
        for start, end in zip([0] + points, points + [len(text)]):
            if start == end:
                # Zero-length segment at the start or end, ignore it.
                continue
            segments.append(SlackSegment(text[start:end], **changes[start]))
        return cls(segments)


class SlackSegment(imirror.RichText.Segment):
    """
    Transport-friendly representation of Slack message formatting.
    """

    @classmethod
    def to_mrkdwn(cls, segment):
        """
        Convert a :class:`.RichText.Segment` back into a Mrkdwn string.

        Args:
            segment (.RichText.Segment)
                Message segment created by another transport.

        Returns:
            str:
                Unparsed segment string.
        """
        text = segment.text
        if segment.bold:
            text = "*{}*".format(text)
        if segment.italic:
            text = "_{}_".format(text)
        if segment.strike:
            text = "~{}~".format(text)
        if segment.code:
            text = "`{}`".format(text)
        if segment.pre:
            text = "```{}```".format(text)
        return text


class SlackMessage(imirror.Message):
    """
    Message originating from Slack.
    """

    @classmethod
    def from_event(cls, slack, event):
        """
        Convert an API event :class:`dict` to a :class:`.Message`.

        Args:
            slack (.SlackTransport):
                Related transport instance that provides the event.
            event (dict):
                Slack API `message <https://api.slack.com/events/message>`_ event data.

        Returns:
            .SlackMessage:
                Parsed message object.
        """
        id = event.get("ts")
        channel = slack.host.resolve_channel(slack, event.get("channel"))
        at = datetime.fromtimestamp(int(float(id))) if id else None
        original = None
        subtype = event.get("subtype")
        text = event.get("text")
        user = slack.users.get(event.get("user"))
        action = False
        deleted = False
        reply_to = event.get("thread_ts")
        joined = None
        left = None
        if subtype == "bot_message":
            # Event has the bot's app ID, not user ID.
            user = slack.users.get(slack.bot2user.get(event.get("bot_id")))
        elif subtype in ("channel_join", "group_join"):
            joined = [user]
        elif subtype in ("channel_leave", "group_leave"):
            left = [user]
        elif subtype == "message_changed":
            # Original message details are under a nested "message" key.
            msg = event.get("message", {})
            original = msg.get("ts")
            text = msg.get("text")
            # NB: Editing user may be different to the original sender.
            user = slack.users.get(msg.get("edited", {}).get("user"))
        elif subtype == "message_deleted":
            original = event.get("deleted_ts")
            deleted = True
        if text and re.match(r"<@{}|.*?> ".format(user.id), text):
            # Own username at the start of the message, assume it's an action.
            action = True
            text = re.sub(r"^<@{}|.*?> ".format(user.id), "", text)
        text = SlackRichText.from_mrkdwn(text)
        return cls(id, channel, at=at, original=original, text=text, user=user, action=action,
                   deleted=deleted, reply_to=reply_to, joined=joined, left=left, raw=event)


class SlackTransport(imirror.Transport):
    """
    Transport for a `Slack <https://slack.com>`_ team.

    Config
        token (str):
            Slack API token for a bot user (usually starts ``xoxb-``).
    """

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        try:
            self.token = config["token"]
        except KeyError:
            raise imirror.ConfigError("Slack token not specified") from None
        self.team = self.users = self.channels = self.directs = None
        # Connection objects that need to be closed on disconnect.
        self.session = self.socket = None
        # When we send messages asynchronously, we'll receive an RTM event before the HTTP request
        # returns. This lock will block event parsing whilst we're sending, to make sure the caller
        # can finish processing the new message (e.g. storing the ID) before receiving the event.
        self.lock = asyncio.BoundedSemaphore()

    async def connect(self):
        await super().connect()
        self.session = aiohttp.ClientSession()
        log.debug("Requesting RTM session")
        async with self.session.post("https://slack.com/api/rtm.start",
                                     data={"token": self.token}) as resp:
            json = await resp.json()
        if not json.get("ok"):
            raise SlackAPIError(json.get("error"))
        # Cache useful information about users and channels, to save on queries later.
        self.team = json.get("team")
        self.users = {u.get("id"): SlackUser.from_member(self, u) for u in json.get("users", [])}
        log.debug("Users ({}): {}".format(len(self.users), ", ".join(self.users.keys())))
        self.channels = {c.get("id"): c for c in json.get("channels", []) + json.get("groups", [])}
        log.debug("Channels ({}): {}".format(len(self.channels), ", ".join(self.channels.keys())))
        self.directs = {c.get("id"): c for c in json.get("ims", [])}
        log.debug("Directs ({}): {}".format(len(self.directs), ", ".join(self.directs.keys())))
        self.bots = {b.get("id"): b for b in json.get("bots", []) if not b.get("deleted")}
        log.debug("Bots ({}): {}".format(len(self.bots), ", ".join(self.bots.keys())))
        # Create a map of bot IDs to users, as the bot cache doesn't contain references to them.
        self.bot2user = {}
        for user in self.users.values():
            if user.bot_id:
                self.bot2user[user.bot_id] = user.id
        self.socket = await self.session.ws_connect(json["url"])
        log.debug("Connected to websocket")

    async def disconnect(self):
        await super().disconnect()
        if self.socket:
            log.debug("Closing websocket")
            await self.socket.close()
            self.socket = None
        if self.session:
            log.debug("Closing session")
            await self.session.close()
            self.session = None

    async def send(self, channel, msg):
        await super().send(channel, msg)
        log.debug("Sending message")
        with (await self.lock):
            if isinstance(msg.text, imirror.RichText):
                text = "".join(SlackSegment.to_mrkdwn(segment) for segment in msg.text)
            else:
                text = msg.text
            data = {"channel": channel.source,
                    "username": msg.user.username or msg.user.real_name,
                    "icon_url": msg.user.avatar,
                    "text": text}
            # Block event processing whilst we wait for the message to go through. Processing will
            # resume once the caller yields or returns.
            resp = await self.session.post("https://slack.com/api/chat.postMessage",
                                           data=dict(data, token=self.token))
            json = await resp.json()
        if not json.get("ok"):
            raise SlackAPIError(json.get("error"))
        return json.get("ts")

    async def receive(self):
        await super().receive()
        while True:
            event = await self.socket.receive_json()
            with (await self.lock):
                # No critical section here, just wait for any pending messages to be sent.
                pass
            if "type" not in event:
                log.warn("Received strange message with no type")
                continue
            log.debug("Received a '{}' event".format(event["type"]))
            user = event.get("user", {})
            channel = event.get("channel", {})
            if event["type"] in ("team_join", "user_change"):
                # A user appeared or changed, update our cache.
                self.users[user.get("id")] = user
            elif event["type"] in ("channel_joined", "group_joined"):
                # A group or channel appeared, add to our cache.
                self.channels[channel.get("id")] = channel
            elif event["type"] == "im_created":
                # A DM appeared, add to our cache.
                self.directs[channel.get("id")] = channel
            elif event["type"] == "message":
                # A new message arrived, push it back to the host.
                yield SlackMessage.from_event(self, event)
