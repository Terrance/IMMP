"""
Connect to a `Slack <https://slack.com>`_ workspace as a bot.

Config:
    token (str):
        Slack API token for a bot user.
    fallback-name (str):
        Name to display for incoming messages without an attached user (default: ``IMMP``).
    fallback-image (str):
        Avatar to display for incoming messages without a user or image (default: none).
    thread-broadcast (bool):
        ``True`` to always send outgoing thread replies back to the channel.

You'll need to create either a full `Slack App <https://api.slack.com/apps>`_ and add a bot, or
a `Bot Integration <https://my.slack.com/apps/A0F7YS25R-bots>`_.  In either case, you should end up
with a token prefixed ``xoxb-``.

You may alternatively use a full user account, with a token obtained from the `Legacy tokens
<https://api.slack.com/custom-integrations/legacy-tokens>`_ page.  This is required to make use of
adding or removing users in channels.

If multiple Slack workspaces are involved, you will need a separate bot and plug setup per team.
"""

from asyncio import CancelledError, ensure_future, gather, sleep
from copy import copy
from collections import defaultdict
from datetime import datetime, timezone
from functools import partial
from json import dumps as json_dumps
import logging
import re

from aiohttp import ClientResponseError, FormData
from emoji import emojize

import immp


log = logging.getLogger(__name__)


class _Schema:

    image_sizes = ("original", "512", "192", "72", "48", "32", "24")

    _images = {immp.Optional("image_{}".format(size)): immp.Nullable(str)
               for size in image_sizes}

    config = immp.Schema({"token": str,
                          immp.Optional("fallback-name", "IMMP"): str,
                          immp.Optional("fallback-image"): immp.Nullable(str),
                          immp.Optional("thread-broadcast", False): bool})

    user = immp.Schema({"id": str,
                        "name": str,
                        "profile": {immp.Optional("real_name"): immp.Nullable(str),
                                    immp.Optional("bot_id"): immp.Nullable(str),
                                    **_images}})

    bot = immp.Schema({"id": str,
                       "app_id": str,
                       "name": str,
                       "icons": _images})

    channel = immp.Schema({"id": str, "name": str})

    direct = immp.Schema({"id": str, "user": str})

    _shares = {str: [{"ts": str}]}

    file = immp.Schema({"id": str,
                        "name": immp.Nullable(str),
                        "pretty_type": str,
                        "url_private": str,
                        immp.Optional("shares", dict): {immp.Optional("public", dict): _shares,
                                                        immp.Optional("private", dict): _shares}})

    attachment = immp.Schema({immp.Optional("title"): immp.Nullable(str),
                              immp.Optional("image_url"): immp.Nullable(str),
                              immp.Optional("is_msg_unfurl", False): bool})

    msg_unfurl = immp.Schema({"channel_id": str, "ts": str}, attachment)

    _base_msg = immp.Schema({"ts": str,
                             "type": "message",
                             immp.Optional("channel"): immp.Nullable(str),
                             immp.Optional("edited", dict):
                                 {immp.Optional("user"): immp.Nullable(str)},
                             immp.Optional("thread_ts"): immp.Nullable(str),
                             immp.Optional("replies", list): [{"ts": str}],
                             immp.Optional("files", list): [file],
                             immp.Optional("attachments", list): [attachment],
                             immp.Optional("is_ephemeral", False): bool})

    _plain_msg = immp.Schema({immp.Optional("user"): immp.Nullable(str),
                              immp.Optional("bot_id"): immp.Nullable(str),
                              immp.Optional("username"): immp.Nullable(str),
                              immp.Optional("icons", dict): dict,
                              "text": str}, _base_msg)

    message = immp.Schema(immp.Any(immp.Schema({"subtype": "file_comment"}, _base_msg),
                                   immp.Schema({"subtype": "message_changed"}, _base_msg),
                                   immp.Schema({"subtype": "message_deleted",
                                                "deleted_ts": str}, _base_msg),
                                   immp.Schema({"subtype": immp.Any("channel_name", "group_name"),
                                                "name": str}, _plain_msg),
                                   immp.Schema({immp.Optional("subtype"):
                                                    immp.Nullable(str)}, _plain_msg)))

    # Circular references to embedded messages.
    message.raw.choices[1].raw.update({"message": message, "previous_message": message})

    event = immp.Schema(immp.Any(message,
                                 {"type": immp.Any("team_join", "user_change"),
                                  "user": user},
                                 {"type": immp.Any("channel_created", "channel_joined",
                                                   "channel_rename", "group_created",
                                                   "group_joined", "group_rename"),
                                  "channel": {"id": str, "name": str}},
                                 {"type": "im_created",
                                  "channel": {"id": str}},
                                 {"type": immp.Any("member_joined_channel", "member_left_channel"),
                                  "user": str,
                                  "channel": str},
                                 {"type": "message",
                                  immp.Optional("subtype"): immp.Nullable(str)},
                                 {"type": str}))

    def _api(nested={}):
        return immp.Schema(immp.Any({"ok": True,
                                     immp.Optional("response_metadata", dict):
                                         {immp.Optional("next_cursor", ""): str},
                                     **nested},
                                    {"ok": False,
                                     "error": str}))

    rtm = _api({"url": str,
                "self": {"id": str},
                "team": {"id": str, "name": str, "domain": str},
                "users": [user],
                "channels": [channel],
                "groups": [channel],
                "ims": [direct],
                "bots": [{"id": str,
                          "deleted": bool}]})

    im_open = _api({"channel": direct})

    members = _api({"members": [str]})

    post = _api({"ts": str})

    upload = _api({"file": file})

    history = _api({"messages": [message]})

    api = _api()


class SlackAPIError(immp.PlugError):
    """
    Generic error from the Slack API.
    """


class MessageNotFound(Exception):
    # No match for a given channel and ts pair.
    pass


class SlackUser(immp.User):
    """
    User present in Slack.

    Attributes:
        bot_id (str):
            Reference to the Slack integration app for a bot user.
    """

    def __init__(self, id_=None, plug=None, username=None, real_name=None, avatar=None,
                 bot_id=None, app=False, raw=None):
        super().__init__(id_=id_,
                         plug=plug,
                         username=username,
                         real_name=real_name,
                         avatar=avatar,
                         raw=raw)
        self.bot_id = bot_id
        self.app = app

    @property
    def link(self):
        return "https://{}.slack.com/{}/{}".format(self.plug._team["domain"],
                                                   "apps" if self.app else "team",
                                                   self.id)

    @classmethod
    def _best_image(cls, profile):
        for size in _Schema.image_sizes:
            if "image_{}".format(size) in profile:
                return profile["image_{}".format(size)]
        return None

    @classmethod
    def from_member(cls, slack, json):
        """
        Convert an API member :class:`dict` to a :class:`.User`.

        Args:
            slack (.SlackPlug):
                Related plug instance that provides the user.
            json (dict):
                Slack API `user <https://api.slack.com/types/user>`_ object.

        Returns:
            .SlackUser:
                Parsed user object.
        """
        member = _Schema.user(json)
        return cls(id_=member["id"],
                   plug=slack,
                   username=member["name"],
                   real_name=member["profile"]["real_name"],
                   avatar=cls._best_image(member["profile"]),
                   bot_id=member["profile"]["bot_id"],
                   raw=json)

    @classmethod
    def from_bot(cls, slack, json):
        """
        Convert an API bot :class:`dict` to a :class:`.User`.

        Args:
            slack (.SlackPlug):
                Related plug instance that provides the user.
            json (dict):
                Slack API bot object.

        Returns:
            .SlackUser:
                Parsed user object.
        """
        bot = _Schema.bot(json)
        return cls(id_=bot["app_id"],
                   plug=slack,
                   real_name=bot["name"],
                   avatar=cls._best_image(bot["icons"]),
                   bot_id=bot["id"],
                   app=True,
                   raw=json)


class SlackRichText(immp.RichText):
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
    _tag_chars = r"```|[*_~`]"
    _inside_chars = r"\s\1"
    _format_regex = re.compile(r"(?<![{0}\\])({1})(?![{2}])(.+?)(?<![{2}\\])\1(?![{0}])"
                               .format(_outside_chars, _tag_chars, _inside_chars))

    _link_regex = re.compile(r"<([^@#\|][^\|>]*?)(?:\|([^>]+?))?>")
    _mention_regex = re.compile(r"<@([^\|>]+?)(?:\|[^>]+?)?>")
    _channel_regex = re.compile(r"<#([^\|>]+?)(?:\|[^>]+?)?>")

    @classmethod
    def _sub_channel(cls, slack, match):
        return "#{}".format(slack._channels[match.group(1)]["name"])

    @classmethod
    def _sub_link(cls, match):
        # Use a label if we have one, else just show the URL.
        return match.group(2) or match.group(1)

    @classmethod
    def _escape(cls, text):
        return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    @classmethod
    def _unescape(cls, text):
        return text.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")

    @classmethod
    async def from_mrkdwn(cls, slack, text):
        """
        Convert a string of Slack's Mrkdwn into a :class:`.RichText`.

        Args:
            slack (.SlackPlug):
                Related plug instance that provides the text.
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
        for match in cls._link_regex.finditer(text):
            # Store the link target; the link tag will be removed after segmenting.
            changes[match.start()]["link"] = cls._unescape(match.group(1))
            changes[match.end()]["link"] = None
        for match in cls._mention_regex.finditer(text):
            changes[match.start()]["mention"] = await slack.user_from_id(match.group(1))
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
                part = "@{}".format(user.real_name)
            else:
                part = text[start:end]
                # Strip Slack channel tags, replace with a plain-text representation.
                part = cls._channel_regex.sub(partial(cls._sub_channel, slack), part)
                part = cls._link_regex.sub(cls._sub_link, part)
                part = emojize(cls._unescape(part), use_aliases=True)
            segments.append(immp.Segment(part, **formatting))
        return cls(segments)

    @classmethod
    def to_mrkdwn(cls, slack, rich):
        """
        Convert a :class:`.RichText` instance into a string of Slack's Mrkdwn.

        Args:
            slack (.SlackPlug):
                Related plug instance to cross-reference users.
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
            if segment.mention and slack.same_team(segment.mention.plug):
                text += "<@{}>".format(segment.mention.id)
            elif segment.link:
                text += "<{}|{}>".format(segment.link, cls._escape(segment.text))
            else:
                text += cls._escape(segment.text)
        for tag in reversed(active):
            # Close all remaining tags.
            text += tag
        return text


class SlackFile(immp.File):
    """
    File attachment originating from Slack.
    """

    def __init__(self, slack, title=None, type_=None, source=None):
        super().__init__(title=title, type_=type_)
        self.slack = slack
        # Private source as the URL is not publicly accessible.
        self._source = source

    async def get_content(self, sess):
        headers = {"Authorization": "Bearer {}".format(self.slack.config["token"])}
        return await self.slack.session.get(self._source, headers=headers)

    @classmethod
    def from_file(cls, slack, json):
        """
        Convert an API file :class:`dict` to a :class:`.File`.

        Args:
            slack (.SlackPlug):
                Related plug instance that provides the file.
            json (dict):
                Slack API `file <https://api.slack.com/types/file>`_ data.

        Returns:
            .SlackFile:
                Parsed file object.
        """
        file = _Schema.file(json)
        type_ = immp.File.Type.unknown
        if file["mimetype"].startswith("image/"):
            type_ = immp.File.Type.image
        return cls(slack,
                   title=file["name"],
                   type_=type_,
                   source=file["url_private"])


class SlackMessage(immp.Message):
    """
    Message originating from Slack.
    """

    @classmethod
    async def from_event(cls, slack, json, parent=True):
        """
        Convert an API event :class:`dict` to a :class:`.Message`.

        Args:
            slack (.SlackPlug):
                Related plug instance that provides the event.
            json (dict):
                Slack API `message <https://api.slack.com/events/message>`_ event data.
            parent (bool):
                ``True`` (default) to retrieve the thread parent if one exists.

        Returns:
            .SlackMessage:
                Parsed message object.
        """
        event = _Schema.message(json)
        if event["is_ephemeral"]:
            # Ignore user-private messages from Slack (e.g. over quota warnings, link unfurling
            # opt-in prompts etc.) which shouldn't be served to message processors.
            raise NotImplementedError
        id_ = revision = event["ts"]
        at = datetime.fromtimestamp(float(event["ts"]), timezone.utc)
        edited = False
        deleted = False
        author = user = None
        action = False
        reply_to = None
        joined = None
        left = None
        title = None
        attachments = []
        if event["subtype"] == "file_comment":
            raise NotImplementedError
        elif event["subtype"] == "message_changed":
            if event["message"]["text"] == event["previous_message"]["text"]:
                # Message remains unchanged.  Can be caused by link unfurling (adds an attachment)
                # or deleting replies (reply is removed from event.replies in new *and old*).
                raise NotImplementedError
            # Original message details are under a nested "message" key.
            id_ = event["message"]["ts"]
            edited = True
            text = event["message"]["text"]
            if event["message"]["subtype"] == "me_message":
                action = True
            # NB: Editing user may be different to the original sender.
            author = event["message"]["edited"]["user"] or event["message"]["user"]
        elif event["subtype"] == "message_deleted":
            id_ = event["deleted_ts"]
            deleted = True
            author = None
            text = None
        else:
            if event["user"]:
                author = event["user"]
            elif event["bot_id"] in slack._bot_to_user:
                # Event has the bot's app ID, not user ID.
                author = slack._bot_to_user[event["bot_id"]]
            elif event["bot_id"] in slack._bots:
                # Slack app with no bot presence, use the app metadata.
                user = slack._bots[event["bot_id"]]
            elif event["username"]:
                # Bot has no associated user, just create a dummy user with the username.
                user = immp.User(real_name=event["username"])
            text = event["text"]
        if author:
            user = await slack.user_from_id(author) or SlackUser(id_=author, plug=slack)
            if text and re.match(r"<@{}(\|.*?)?> ".format(author), text):
                # Own username at the start of the message, assume it's an action.
                action = True
                text = re.sub(r"^<@{}(\|.*?)?> ".format(author), "", text)
        if event["subtype"] in ("channel_join", "group_join"):
            action = True
            joined = [user]
        elif event["subtype"] in ("channel_leave", "group_leave"):
            action = True
            left = [user]
        elif event["subtype"] in ("channel_name", "group_name"):
            action = True
            title = event["name"]
        elif event["subtype"] == "me_message":
            action = True
        thread = recent = None
        if event["thread_ts"]:
            thread = immp.Receipt(event["thread_ts"], immp.Channel(slack, event["channel"]))
        if thread and parent:
            try:
                thread = await slack.get_message(event["channel"], thread.id, False)
            except MessageNotFound:
                pass
        if isinstance(thread, immp.Message):
            for reply in thread.raw["replies"][::-1]:
                if reply["ts"] not in (event["ts"], event["thread_ts"]):
                    # Reply to a thread with at least one other message, use the next most
                    # recent rather than the parent.
                    recent = immp.Receipt(reply["ts"], immp.Channel(slack, event["channel"]))
                    break
        if recent and parent:
            try:
                recent = await slack.get_reply(event["channel"], event["thread_ts"],
                                               recent.id, False)
            except MessageNotFound:
                pass
        if thread and recent:
            # Don't walk the whole thread, just link to the parent after one step.
            recent.reply_to = thread
            reply_to = recent
        else:
            reply_to = recent or thread
        for file in event["files"]:
            attachments.append(SlackFile.from_file(slack, file))
        for attach in event["attachments"]:
            if attach["is_msg_unfurl"]:
                unfurl = _Schema.msg_unfurl(attach)
                # We have the message ID as the timestamp, fetch the whole message to embed it.
                try:
                    attachments.append(await slack.get_message(unfurl["channel_id"], unfurl["ts"]))
                except MessageNotFound:
                    pass
            elif attach["image_url"]:
                attachments.append(immp.File(title=attach["title"],
                                             type_=immp.File.Type.image,
                                             source=attach["image_url"]))
            elif attach["fallback"]:
                if text:
                    text = "{}\n---\n{}".format(text, attach["fallback"])
                else:
                    text = attach["fallback"]
        if text:
            # Messages can be shared either in the UI, or by pasting an archive link.  The latter
            # unfurls async (it comes through as an edit, which we ignore), so instead we can look
            # up the message ourselves and embed it.
            for channel_id, link in re.findall(r"https://{}.slack.com/archives/([^/]+)/p([0-9]+)"
                                               .format(slack._team["domain"]), text):
                # Archive links are strange and drop the period from the ts value.
                ts = link[:-6] + "." + link[-6:]
                if not any(isinstance(attach, immp.Message) and attach.id == ts
                           for attach in attachments):
                    try:
                        attachments.append(await slack.get_message(channel_id, ts))
                    except MessageNotFound:
                        pass
            text = await SlackRichText.from_mrkdwn(slack, text)
        return immp.SentMessage(id_=id_,
                                channel=immp.Channel(slack, event["channel"]),
                                at=at,
                                revision=revision,
                                edited=edited,
                                deleted=deleted,
                                text=text,
                                user=user,
                                action=action,
                                reply_to=reply_to,
                                joined=joined,
                                left=left,
                                title=title,
                                attachments=attachments,
                                raw=json)

    @classmethod
    def to_attachment(cls, slack, msg, reply=False):
        """
        Convert a :class:`.Message` to a message attachment structure, suitable for embedding
        within an outgoing message.

        Args:
            slack (.SlackPlug):
                Target plug instance for this attachment.
            msg (.Message):
                Original message from another plug or hook.
            reply (bool):
                Whether to show a reply icon instead of a quote icon.

        Returns.
            dict:
                Slack API `attachment <https://api.slack.com/docs/message-attachments>`_ object.
        """
        icon = ":arrow_right_hook:" if reply else ":speech_balloon:"
        quote = {"footer": icon}
        if isinstance(msg, immp.SentMessage):
            quote["ts"] = msg.at.timestamp()
        if msg.user:
            quote["author_name"] = msg.user.real_name or msg.user.username
            quote["author_icon"] = msg.user.avatar
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
            quote["text"] = SlackRichText.to_mrkdwn(slack, quoted_rich)
            quote["mrkdwn_in"] = ["text"]
        return quote


class SlackPlug(immp.HTTPOpenable, immp.Plug):
    """
    Plug for a `Slack <https://slack.com>`_ team.
    """

    schema = _Schema.config

    @property
    def network_name(self):
        return "{} Slack".format(self._team["name"]) if self._team else "Slack"

    @property
    def network_id(self):
        return "slack:{}:{}".format(self._team["id"], self._bot_user["id"]) if self._team else None

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        self._team = self._bot_user = None
        self._users = self._channels = self._directs = None
        self._bots = self._bot_to_user = self._members = None
        # Connection objects that need to be closed on disconnect.
        self._socket = self._receive = None
        self._closing = False

    def same_team(self, other):
        """
        Test if two Slack plugs represent the same team.

        Arguments:
            other (.SlackPlug):
                Second plug instance to compare with.

        Returns:
            bool:
                ``True`` if both plugs are connected to the same team.
        """
        return isinstance(other, self.__class__) and self._team["id"] == other._team["id"]

    async def _api(self, endpoint, schema=_Schema.api, params=None, **kwargs):
        params = params or {}
        params["token"] = self.config["token"]
        log.debug("Making API request to %r", endpoint)
        async with self.session.post("https://slack.com/api/{}".format(endpoint),
                                     params=params, **kwargs) as resp:
            try:
                resp.raise_for_status()
            except ClientResponseError as e:
                raise SlackAPIError("Unexpected response code: {}".format(resp.status)) from e
            else:
                json = await resp.json()
        data = schema(json)
        if not data["ok"]:
            raise SlackAPIError(data["error"])
        return data

    async def _paged(self, endpoint, schema, key, params=None, **kwargs):
        params = params or {}
        params["token"] = self.config["token"]
        items = []
        while True:
            data = await self._api(endpoint, schema, params, **kwargs)
            items += data[key]
            if data["response_metadata"]["next_cursor"]:
                params["cursor"] = data["response_metadata"]["next_cursor"]
            else:
                break
        return items

    async def _rtm(self):
        log.debug("Requesting RTM session")
        rtm = await self._api("rtm.start", _Schema.rtm)
        # Cache useful information about users and channels, to save on queries later.
        self._bot_user = rtm["self"]
        self._team = rtm["team"]
        self._users = {u["id"]: SlackUser.from_member(self, u) for u in rtm["users"]}
        self._channels = {c["id"]: c for c in rtm["channels"] + rtm["groups"]}
        self._directs = {c["id"]: c for c in rtm["ims"]}
        self._bots = {b["id"]: SlackUser.from_bot(self, b)
                      for b in rtm["bots"] if not b["deleted"]}
        log.debug("Cached %d users, %d channels, %d IMs, %d bots",
                  len(self._users), len(self._channels), len(self._directs), len(self._bots))
        self._members = {}
        # Create a map of bot IDs to users, as the bot cache doesn't contain references to them.
        self._bot_to_user = {user.bot_id: user.id for user in self._users.values() if user.bot_id}
        self._socket = await self.session.ws_connect(rtm["url"], heartbeat=60.0)
        log.debug("Connected to websocket")

    async def start(self):
        await super().start()
        self._closing = False
        await self._rtm()
        self._receive = ensure_future(self._poll())

    async def stop(self):
        await super().stop()
        self._closing = True
        if self._receive:
            self._receive.cancel()
            self._receive = None
        if self._socket:
            log.debug("Closing websocket")
            await self._socket.close()
            self._socket = None
        self._team = self._bot_user = None

    async def user_from_id(self, id_):
        if id_ not in self._users:
            try:
                data = await self._api("users.info", params={"user": id_})
            except SlackAPIError:
                return None
            else:
                self._users[id_] = SlackUser.from_member(self, data["user"])
        return self._users[id_]

    async def user_from_username(self, username):
        for user in self._users.values():
            if user.username == username:
                return user
        return None

    async def user_is_system(self, user):
        return user.id == self._bot_user["id"]

    async def public_channels(self):
        return [immp.Channel(self, id_) for id_ in self._channels]

    async def private_channels(self):
        return [immp.Channel(self, id_) for id_ in self._directs]

    async def channel_for_user(self, user):
        if not isinstance(user, SlackUser):
            return
        for direct in self._directs.values():
            if direct["user"] == user.id:
                return immp.Channel(self, direct["id"])
        # Private channel doesn't exist yet or isn't cached.
        params = {"user": user.id,
                  "return_im": "true"}
        opened = await self._api("im.open", _Schema.im_open, params=params)
        return immp.Channel(self, opened["channel"]["id"])

    async def channel_is_private(self, channel):
        return channel.source in self._directs

    async def channel_title(self, channel):
        try:
            sl_channel = self._channels[channel.source]
        except KeyError:
            return None
        else:
            return sl_channel["name"]

    async def channel_link(self, channel):
        return "https://{}.slack.com/messages/{}/".format(self._team["domain"], channel.source)

    async def channel_members(self, channel):
        if channel.plug is not self:
            return None
        if channel.source not in self._members:
            members = await self._paged("conversations.members", _Schema.members, "members",
                                        data={"channel": channel.source})
            self._members[channel.source] = members
        return await gather(*(self.user_from_id(member)
                              for member in self._members[channel.source]))

    async def channel_invite(self, channel, user):
        if user.id == self._bot_user["id"]:
            await self._api("conversations.join", params={"channel": channel.source})
        else:
            await self._api("conversations.invite", params={"channel": channel.source,
                                                            "user": user.id})

    async def channel_remove(self, channel, user):
        if user.id == self._bot_user["id"]:
            await self._api("conversations.leave", params={"channel": channel.source})
        else:
            await self._api("conversations.kick", params={"channel": channel.source,
                                                          "user": user.id})

    async def channel_history(self, channel, before=None):
        params = {"channel": channel.source}
        if before:
            params["latest"] = before.id
        history = await self._api("conversations.history", _Schema.history, params=params)
        messages = list(reversed(history["messages"]))
        for msg in messages:
            msg["channel"] = channel.source
        return await gather(*(SlackMessage.from_event(self, msg) for msg in messages))

    async def get_message(self, channel_id, ts, parent=True):
        params = {"channel": channel_id,
                  "latest": ts,
                  "inclusive": "true",
                  "limit": 1}
        try:
            history = await self._api("conversations.history", _Schema.history, params=params)
        except SlackAPIError as e:
            log.debug("API error retrieving message %r from %r: %r", ts, channel_id, e.args[0])
            raise MessageNotFound from None
        if history["messages"]:
            msg = history["messages"][0]
            if msg["ts"] == ts:
                msg["channel"] = channel_id
                return await SlackMessage.from_event(self, msg, parent)
        log.debug("Failed to find message %r in %r", ts, channel_id)
        raise MessageNotFound

    async def get_reply(self, channel_id, thread_ts, reply_ts, parent=True):
        params = {"channel": channel_id,
                  "ts": thread_ts,
                  "latest": reply_ts,
                  "inclusive": "true",
                  "limit": 1}
        try:
            replies = await self._api("conversations.replies", _Schema.history, params=params)
        except SlackAPIError as e:
            log.debug("API error retrieving reply %r -> %r from %r: %r",
                      thread_ts, reply_ts, channel_id, e.args[0])
            raise MessageNotFound from None
        if replies["messages"]:
            msg = replies["messages"][-1]
            if msg["ts"] == reply_ts:
                msg["channel"] = channel_id
                return await SlackMessage.from_event(self, msg, parent)
        log.debug("Failed to find reply %r -> %r in %r", thread_ts, reply_ts, channel_id)
        raise MessageNotFound

    async def _post(self, channel, parent, msg):
        ids = []
        uploads = 0
        if msg.user:
            name = msg.user.real_name or msg.user.username
            image = msg.user.avatar
        else:
            name = self.config["fallback-name"]
            image = self.config["fallback-image"]
        for attach in msg.attachments:
            if isinstance(attach, immp.File):
                # Upload each file to Slack.
                data = FormData({"channels": channel.source,
                                 "filename": attach.title or ""})
                if isinstance(parent.reply_to, immp.Receipt):
                    # Reply directly to the corresponding thread.  Note that thread_ts can be any
                    # message in the thread, it need not be resolved to the parent.
                    data.add_field("thread_ts", msg.reply_to.id)
                    if self.config["thread-broadcast"]:
                        data.add_field("broadcast", "true")
                if msg.user:
                    comment = immp.RichText([immp.Segment(name, bold=True, italic=True),
                                             immp.Segment(" uploaded this file", italic=True)])
                    data.add_field("initial_comment", SlackRichText.to_mrkdwn(self, comment))
                img_resp = await attach.get_content(self.session)
                data.add_field("file", img_resp.content, filename="file")
                upload = await self._api("files.upload", _Schema.upload, data=data)
                uploads += 1
                for shared in upload["file"]["shares"].values():
                    if channel.source in shared:
                        ids += [share["ts"] for share in shared[channel.source]]
        if len(ids) < uploads:
            log.warning("Missing some file shares: sent %d, got %d", uploads, len(ids))
        data = {"channel": channel.source,
                "as_user": msg.user is None,
                "username": name,
                "icon_url": image}
        rich = None
        if msg.text:
            rich = msg.text.clone()
            if msg.action:
                for segment in rich:
                    segment.italic = True
        if isinstance(msg, immp.Receipt) and msg.edited:
            rich.append(immp.Segment(" (edited)", italic=True))
        attachments = []
        if isinstance(parent.reply_to, immp.Receipt):
            data["thread_ts"] = msg.reply_to.id
            if self.config["thread-broadcast"]:
                data["reply_broadcast"] = "true"
        elif isinstance(msg.reply_to, immp.Message):
            attachments.append(SlackMessage.to_attachment(self, msg.reply_to, True))
        for attach in msg.attachments:
            if isinstance(attach, immp.Location):
                coords = "{}, {}".format(attach.latitude, attach.longitude)
                fallback = "{} ({})".format(attach.address, coords) if attach.address else coords
                attachments.append({"fallback": fallback,
                                    "title": attach.name or "Location",
                                    "title_link": attach.google_map_url,
                                    "text": attach.address,
                                    "footer": "{}, {}".format(attach.latitude, attach.longitude)})
        if rich or attachments:
            if rich:
                data["text"] = SlackRichText.to_mrkdwn(self, rich)
            if attachments:
                data["attachments"] = json_dumps(attachments)
            post = await self._api("chat.postMessage", _Schema.post, data=data)
            ids.append(post["ts"])
        return ids

    async def put(self, channel, msg):
        clone = copy(msg)
        if clone.text:
            clone.text = msg.text.clone()
        forward_ids = []
        for attach in msg.attachments:
            if isinstance(attach, immp.Receipt):
                # No public API to share a message, rely on archive link unfurling instead.
                link = ("https://{}.slack.com/archives/{}/p{}"
                        .format(self._team["domain"], channel.source, attach.id.replace(".", "")))
                if clone.text:
                    clone.text.append(immp.Segment("\n{}".format(link)))
                else:
                    clone.text = immp.RichText([immp.Segment(link)])
            elif isinstance(attach, immp.Message):
                forward_ids += await self._post(channel, msg, attach)
        own_ids = await self._post(channel, msg, msg)
        if forward_ids and not own_ids:
            # Forwarding a message but no content to show who forwarded it.
            info = immp.Message(user=msg.user, action=True, text="forwarded a message")
            own_ids += await self._post(channel, msg, info)
        return forward_ids + own_ids

    async def delete(self, sent):
        await self._api("chat.delete", params={"channel": sent.channel.source, "ts": sent.id})

    async def _poll(self):
        while self.state == immp.OpenState.active and not self._closing:
            fetch = ensure_future(self._socket.receive_json())
            try:
                json = await fetch
            except CancelledError:
                log.debug("Cancelling polling")
                return
            except TypeError as e:
                if self._closing:
                    return
                log.debug("Unexpected socket state: %r", e)
                await self._socket.close()
                self._socket = None
                log.debug("Reconnecting in 3 seconds")
                await sleep(3)
                await self._rtm()
                continue
            event = _Schema.event(json)
            log.debug("Received a %r event", event["type"])
            if event["type"] in ("team_join", "user_change"):
                # A user appeared or changed, update our cache.
                self._users[event["user"]["id"]] = SlackUser.from_member(self, event["user"])
            elif event["type"] in ("channel_created", "channel_joined", "channel_rename",
                                   "group_created", "group_joined", "group_rename"):
                # A group or channel appeared or updated, add to our cache.
                if event["channel"]["id"] in self._channels:
                    self._channels[event["channel"]["id"]].update(event["channel"])
                else:
                    self._channels[event["channel"]["id"]] = event["channel"]
            elif event["type"] == "im_created":
                # A DM appeared, add to our cache.
                self._directs[event["channel"]["id"]] = event["channel"]
            elif (event["type"] in ("channel_deleted", "group_deleted") and
                  event["channel"] in self._channels):
                del self._channels[event["channel"]]
            elif event["type"] == "member_joined_channel" and event["channel"] in self._members:
                self._members[event["channel"]].append(event["user"])
            elif event["type"] == "member_left_channel" and event["channel"] in self._members:
                self._members[event["channel"]].remove(event["user"])
            elif event["type"] == "message" and not event["subtype"] == "message_replied":
                # A new message arrived, push it back to the host.
                try:
                    sent = await SlackMessage.from_event(self, event)
                except NotImplementedError:
                    log.debug("Ignoring message with ts %r", event.get("ts"))
                else:
                    self.queue(sent)
