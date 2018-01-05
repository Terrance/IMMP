import asyncio
from collections import defaultdict
from datetime import datetime
from functools import partial
from json import dumps as json_dumps
import logging
import re

import aiohttp
from emoji import emojize
from voluptuous import Schema, Any, Optional, Match, ALLOW_EXTRA

import imirror


log = logging.getLogger(__name__)


class _Schema(object):

    config = Schema({"token": str,
                     Optional("fallback-name", default="Bridge"): str,
                     Optional("fallback-image", default=None): Any(str, None)},
                    extra=ALLOW_EXTRA, required=True)

    user = Schema({"id": str,
                   "name": str,
                   "profile": {Optional("real_name", default=None): Any(str, None),
                               Optional(Match(r"image_(original|\d+)")): Any(str, None),
                               Optional("bot_id", default=None): Any(str, None)}},
                  extra=ALLOW_EXTRA, required=True)

    file = Schema({"id": str,
                   "name": Any(str, None),
                   "url_private": str},
                  extra=ALLOW_EXTRA, required=True)

    _edit_user = {Optional("user", default=None): Any(str, None)}

    _base_message = Schema({"ts": str,
                            "type": "message",
                            Optional("channel", default=None): Any(str, None),
                            Optional("edited", default={"user": None}): _edit_user,
                            Optional("thread_ts", default=None): Any(str, None)},
                           extra=ALLOW_EXTRA, required=True)

    _plain_message = _base_message.extend({"user": str, "text": str})

    message = Schema(Any(_base_message.extend({"subtype": "bot_message",
                                               "bot_id": str,
                                               "text": str,
                                               Optional("username", default=None): Any(str, None),
                                               Optional("icons", default=dict): Any(dict, None)}),
                         _base_message.extend({"subtype": "message_changed",
                                               "message": lambda v: _Schema.message(v)}),
                         _base_message.extend({"subtype": "message_deleted",
                                               "deleted_ts": str}),
                         _plain_message.extend({"subtype": Any("file_share", "file_mention"),
                                                "file": file}),
                         _plain_message.extend({Optional("subtype", default=None): Any(str, None)})))

    event = Schema(Any(message,
                       {"type": Any("team_join", "user_change"),
                        "user": user},
                       {"type": Any("channel_joined", "group_joined", "im_created"),
                        "channel": {"id": str}},
                       {"type": str},
                       extra=ALLOW_EXTRA, required=True))

    rtm = Schema(Any({"ok": False,
                      "error": str},
                     {"ok": True,
                      "url": str,
                      "team": dict,
                      "users": [user],
                      "channels": [{"id": str}],
                      "groups": [{"id": str}],
                      "ims": [{"id": str}],
                      "bots": [{"id": str}]},
                     extra=ALLOW_EXTRA, required=True))


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
    def _best_image(cls, profile):
        for size in ("original", "512", "192", "72", "48", "32", "24"):
            if "image_{}".format(size) in profile:
                return profile["image_{}".format(size)]
        return None

    @classmethod
    def from_member(cls, slack, json):
        """
        Convert an API member :class:`dict` to a :class:`.User`.

        Args:
            slack (.SlackTransport):
                Related transport instance that provides the user.
            json (dict):
                Slack API `user <https://api.slack.com/types/user>`_ object.

        Returns:
            .SlackUser:
                Parsed user object.
        """
        member = _Schema.user(json)
        return cls(id=member["id"],
                   username=member["name"],
                   real_name=member["profile"]["real_name"],
                   avatar=cls._best_image(member["profile"]),
                   bot_id=member["profile"]["bot_id"],
                   raw=json)


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
    def _sub_user(cls, slack, match):
        return "@{}".format(slack._users[match.group(1)].username)

    @classmethod
    def _sub_channel(cls, slack, match):
        return "#{}".format(slack._channels[match.group(1)]["name"])

    @classmethod
    def from_mrkdwn(cls, slack, text):
        """
        Convert a string of Slack's Mrkdwn into a :class:`.RichText`.

        Args:
            slack (.SlackTransport):
                Related transport instance that provides the text.
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
            # Strip Slack user/channel tags, replace with a plain-text representation.
            part = emojize(text[start:end], use_aliases=True)
            part = re.sub(r"<@([^\|>]+)(\|[^>]+)?>", partial(cls._sub_user, slack), part)
            part = re.sub(r"<#([^\|>]+)(\|[^>]+)?>", partial(cls._sub_channel, slack), part)
            # TODO: Handle links.
            part = re.sub(r"<([^\|>]+)(\|[^>]+)?>", r"\1", part)
            segments.append(imirror.RichText.Segment(part, **changes[start]))
        return cls(segments)

    @classmethod
    def to_mrkdwn(cls, rich):
        """
        Convert a string of Slack's Mrkdwn into a :class:`.RichText`.

        Args:
            rich (.SlackRichText):
                Parsed rich text container.

        Returns:
            str:
                Slack-style formatted text.
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
            text += segment.text
        for tag in reversed(active):
            # Close all remaining tags.
            text += tag
        return text


class SlackFile(imirror.File):

    def __init__(self, slack, title=None, type=None, source=None):
        super().__init__(title=title, type=type)
        self.slack = slack
        # Private source as the URL is not publically accessible.
        self._source = source

    async def get_content(self, sess=None):
        headers = {"Authorization": "Bearer {}".format(self.slack._token)}
        return await sess.get(self._source, headers=headers)

    @classmethod
    def from_file(cls, slack, json):
        """
        Convert an API file :class:`dict` to a :class:`.File`.

        Args:
            slack (.SlackTransport):
                Related transport instance that provides the file.
            json (dict):
                Slack API `file <https://api.slack.com/types/file>`_ data.

        Returns:
            .SlackFile:
                Parsed file object.
        """
        file = _Schema.file(json)
        return cls(slack,
                   title=file["name"],
                   type=imirror.File.Type.image if file["mimetype"].startswith("image/") else None,
                   source=file["url_private"])


class SlackMessage(imirror.Message):
    """
    Message originating from Slack.
    """

    @classmethod
    async def from_event(cls, slack, json):
        """
        Convert an API event :class:`dict` to a :class:`.Message`.

        Args:
            slack (.SlackTransport):
                Related transport instance that provides the event.
            json (dict):
                Slack API `message <https://api.slack.com/events/message>`_ event data.

        Returns:
            .SlackMessage:
                Parsed message object.
        """
        event = _Schema.message(json)
        original = None
        action = False
        deleted = False
        joined = None
        left = None
        attachments = []
        if event["subtype"] == "bot_message":
            # Event has the bot's app ID, not user ID.
            user = slack._bot_to_user.get(event["bot_id"])
            text = event["text"]
        elif event["subtype"] == "message_changed":
            # Original message details are under a nested "message" key.
            original = event["message"]["ts"]
            text = event["message"]["text"]
            # NB: Editing user may be different to the original sender.
            user = event["message"]["edited"]["user"] or event["message"]["user"]
        elif event["subtype"] == "message_deleted":
            original = event["deleted_ts"]
            user = None
            text = None
            deleted = True
        else:
            user = event["user"]
            text = event["text"]
            if event["subtype"] in ("file_share", "file_mention"):
                action = True
                attachments.append(SlackFile.from_file(slack, event["file"]))
            elif event["subtype"] in ("channel_join", "group_join"):
                action = True
                joined = [user]
            elif event["subtype"] in ("channel_leave", "group_leave"):
                action = True
                left = [user]
            elif event["subtype"] == "me_message":
                action = True
        if user and text and re.match(r"<@{}(\|.*?)?> ".format(user), text):
            # Own username at the start of the message, assume it's an action.
            action = True
            text = re.sub(r"^<@{}|.*?> ".format(user), "", text)
        return (slack.host.resolve_channel(slack, event["channel"]),
                cls(id=event["ts"],
                    at=datetime.fromtimestamp(int(float(event["ts"]))),
                    original=original,
                    text=SlackRichText.from_mrkdwn(slack, text) if text else None,
                    user=slack._users.get(user, SlackUser(id=user)) if user else None,
                    action=action,
                    deleted=deleted,
                    reply_to=event["thread_ts"],
                    joined=joined,
                    left=left,
                    attachments=attachments,
                    raw=json))


class SlackTransport(imirror.Transport):
    """
    Transport for a `Slack <https://slack.com>`_ team.

    Config
        token (str):
            Slack API token for a bot user (usually starts ``xoxb-``).
        fallback-name (str):
            Name to display for incoming messages without an attached user (default: ``Bridge``).
        fallback-image (str):
            Avatar to display for incoming messages without a user or image (default: none).
    """

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        config = _Schema.config(config)
        self._token = config["token"]
        self.fallback_name = config["fallback-name"]
        self.fallback_image = config["fallback-image"]
        self._team = self._users = self._channels = self._directs = self._bots = None
        # Connection objects that need to be closed on disconnect.
        self._session = self._socket = None
        # When we send messages asynchronously, we'll receive an RTM event before the HTTP request
        # returns. This lock will block event parsing whilst we're sending, to make sure the caller
        # can finish processing the new message (e.g. storing the ID) before receiving the event.
        self._lock = asyncio.BoundedSemaphore()

    async def connect(self):
        await super().connect()
        self._session = aiohttp.ClientSession()
        log.debug("Requesting RTM session")
        async with self._session.post("https://slack.com/api/rtm.start",
                                      params={"token": self._token}) as resp:
            json = await resp.json()
        rtm = _Schema.rtm(json)
        if not rtm["ok"]:
            raise SlackAPIError(rtm["error"])
        # Cache useful information about users and channels, to save on queries later.
        self._team = rtm["team"]
        self._users = {u.get("id"): SlackUser.from_member(self, u) for u in rtm["users"]}
        log.debug("Users ({}): {}".format(len(self._users), ", ".join(self._users.keys())))
        self._channels = {c.get("id"): c for c in rtm["channels"] + rtm["groups"]}
        log.debug("Channels ({}): {}".format(len(self._channels), ", ".join(self._channels.keys())))
        self._directs = {c.get("id"): c for c in rtm["ims"]}
        log.debug("Directs ({}): {}".format(len(self._directs), ", ".join(self._directs.keys())))
        self._bots = {b.get("id"): b for b in rtm["bots"] if not b.get("deleted")}
        log.debug("Bots ({}): {}".format(len(self._bots), ", ".join(self._bots.keys())))
        # Create a map of bot IDs to users, as the bot cache doesn't contain references to them.
        self._bot_to_user = {user.bot_id: user.id for user in self._users.values() if user.bot_id}
        self._socket = await self._session.ws_connect(rtm["url"])
        log.debug("Connected to websocket")

    async def disconnect(self):
        await super().disconnect()
        if self._socket:
            log.debug("Closing websocket")
            await self._socket.close()
            self._socket = None
        if self._session:
            log.debug("Closing session")
            await self._session.close()
            self._session = None

    async def send(self, channel, msg):
        await super().send(channel, msg)
        if msg.deleted:
            # TODO
            return
        if isinstance(msg.text, imirror.RichText):
            text = SlackRichText.to_mrkdwn(msg.text)
        else:
            text = msg.text
        name = (msg.user.username or msg.user.real_name) if msg.user else self.fallback_name
        image = msg.user.avatar if msg.user else self.fallback_image
        data = {"channel": channel.source,
                "username": name,
                "icon_url": image}
        attachments = []
        for attach in msg.attachments:
            if isinstance(attach, imirror.File) and attach.type == imirror.File.Type.image:
                # TODO: Handle files with no source URL.
                if not attach.source:
                    continue
                attachments.append({"fallback": attach.source,
                                    "title": attach.title,
                                    "image_url": attach.source})
        if text:
            data["text"] = text
        if attachments:
            data["attachments"] = json_dumps(attachments)
        with (await self._lock):
            # Block event processing whilst we wait for the message to go through. Processing will
            # resume once the caller yields or returns.
            resp = await self._session.post("https://slack.com/api/chat.postMessage",
                                            params={"token": self._token}, data=data)
            json = await resp.json()
        if not json.get("ok"):
            raise SlackAPIError(json.get("error"))
        return [json.get("ts")]

    async def receive(self):
        await super().receive()
        while True:
            json = await self._socket.receive_json()
            with (await self._lock):
                # No critical section here, just wait for any pending messages to be sent.
                pass
            event = _Schema.event(json)
            log.debug("Received a '{}' event".format(event["type"]))
            if event["type"] in ("team_join", "user_change"):
                # A user appeared or changed, update our cache.
                self._users[event["user"]["id"]] = user
            elif event["type"] in ("channel_joined", "group_joined"):
                # A group or channel appeared, add to our cache.
                self._channels[event["channel"]["id"]] = channel
            elif event["type"] == "im_created":
                # A DM appeared, add to our cache.
                self._directs[event["channel"]["id"]] = channel
            elif event["type"] == "message":
                # A new message arrived, push it back to the host.
                yield (await SlackMessage.from_event(self, event))
