"""
Connect to a `Slack <https://slack.com>`_ workspace as a bot.

Requirements:
    Extra name: ``slack``

    `aiohttp <https://aiohttp.readthedocs.io/en/latest/>`_

Config:
    token (str):
        Slack API user or bot token (``xoxb`` prefix).
    app-token (str):
        Slack API app token, if using a modern app (``xapp`` prefix).
    fallback-image (str):
        Avatar to display for incoming messages without a user or image (default: none).
    thread-broadcast (bool):
        ``True`` to always send outgoing thread replies back to the channel.
    real-names (bool):
        ``True`` to prefer user real names when sending messages, ``False`` to prefer usernames.

Slack supports multiple types of apps and legacy integrations; this plug supports most of them.  If
you're starting fresh, you should `create a new Slack App <https://api.slack.com/apps>`_ and follow
the modern app steps below.

If you have an existing Slack App created on or after December 2019, then it is a modern app.  Apps
created before then are of classic type, and classic apps with an existing bot user should work
without further setup; just enter the current bot API token in the ``token`` config field.  You can
alternatively use any tokens that have access to the RTM APIs, including those from `legacy bot
integrations <https://slack.com/apps/A0F7YS25R-bots>`_ and `legacy tester tokens
<https://api.slack.com/custom-integrations/legacy-tokens>`_, if you have them.

The events and scopes listed below cover everything that's supported by this plug -- you are free
to grant a subset of these as desired, with the obvious caveat that the corresponding features you
deny access to will not work.  Note that classic app bots do not have any channel management
permissions, so cannot add or remove users in channels.

Modern apps:
  * `Create an app <https://api.slack.com/apps?new_app=1>`_ if you haven't already.
  * Socket Mode page -- enable it for your app.
  * Event Subscriptions page -- enable the following events:
      channel_archive, channel_left, channel_rename, channel_unarchive,
      group_archive, group_deleted, group_left, group_rename, group_unarchive,
      message.channels, message.groups, message.im, message.mpim,
      member_joined_channel, member_left_channel, team_join, team_rename, user_change
  * OAuth & Permissions page -- grant the following scopes:
      channels:read, channels:history, channels:manage, groups:read, groups:history,
      im:read, im:history, im:write, mpim:read, mpim:history, chat:write, chat:write.customize,
      files:read, files:write, team:read, users:read
  * App Home page -- enable the Messages tab and sending of messages, and add a bot user.
  * Install App page -- install the app to your workspace, collect your bot OAuth token, and enter
    that in the ``token`` config field.
  * Basic Information page -- generate or collect an app-level token with the ``connections:write``
    scope, and enter that in the ``app-token`` config field.

Classic apps:
  * `Create a classic app <https://api.slack.com/apps?new_classic_app=1>`_ if you haven't already.
  * OAuth & Permissions page -- grant the ``bot`` scope.
  * App Home page -- enable the Messages tab and sending of messages, and add a bot user.
  * Install App page -- install the app to your workspace, collect your bot OAuth token, and enter
    that in the ``token`` config field.

If multiple Slack workspaces are involved, you will need a separate bot and plug setup per team.
There is no special handling of Enterprise Grid workspaces.

Channel sources should be Slack's channel IDs, typically of the form ``C12345678`` where ``C`` is
the channel type and the rest forms its random identifier.  Note that using channel display names
like ``#channel`` is not supported (it may work for sending, but each message received will only be
attributed to its identifier rather than name).
"""

from asyncio import CancelledError, ensure_future, gather, Lock, sleep
from copy import copy
from collections import defaultdict
from datetime import datetime, timezone
from functools import partial
from json import dumps as json_dumps
import logging
import re
import time

from aiohttp import ClientResponseError, FormData
from emoji import emojize

import immp


log = logging.getLogger(__name__)


class _Schema:

    image_sizes = ("original", "512", "192", "72", "48", "32", "24")

    _images = {immp.Optional("image_{}".format(size)): immp.Nullable(str)
               for size in image_sizes}

    config = immp.Schema({"token": str,
                          immp.Optional("app-token"): immp.Nullable(str),
                          immp.Optional("fallback-image"): immp.Nullable(str),
                          immp.Optional("thread-broadcast", False): bool,
                          immp.Optional("real-names", True): bool})

    user = immp.Schema({"id": str,
                        "name": str,
                        "profile": {immp.Optional("real_name"): immp.Nullable(str),
                                    immp.Optional("bot_id"): immp.Nullable(str),
                                    **_images}})

    bot = immp.Schema({"id": str,
                       immp.Optional("app_id"): immp.Nullable(str),
                       "name": str,
                       "icons": _images})

    _channel = {"id": str,
                immp.Optional("name"): immp.Nullable(str),
                immp.Optional("is_im", False): bool}

    direct = immp.Schema({"id": str, "user": str})

    _shares = {str: [{"ts": str}]}

    file = immp.Schema(immp.Any({"id": str,
                                 "name": immp.Nullable(str),
                                 "pretty_type": str,
                                 "url_private": str,
                                 immp.Optional("mode"): immp.Nullable(str),
                                 immp.Optional("shares", dict):
                                     {immp.Optional("public", dict): _shares,
                                      immp.Optional("private", dict): _shares}},
                                {"id": str,
                                 "mode": "tombstone"}))

    attachment = immp.Schema({immp.Optional("fallback"): immp.Nullable(str),
                              immp.Optional("title"): immp.Nullable(str),
                              immp.Optional("image_url"): immp.Nullable(str),
                              immp.Optional("is_msg_unfurl", False): bool})

    msg_unfurl = immp.Schema({"channel_id": str, "ts": str}, attachment)

    _base_msg = {"ts": str,
                 "type": "message",
                 immp.Optional("hidden", False): bool,
                 immp.Optional("channel"): immp.Nullable(str),
                 immp.Optional("edited", dict):
                     {immp.Optional("ts"): immp.Nullable(str),
                      immp.Optional("user"): immp.Nullable(str)},
                 immp.Optional("thread_ts"): immp.Nullable(str),
                 immp.Optional("files", list): [file],
                 immp.Optional("attachments", list): [attachment],
                 immp.Optional("is_ephemeral", False): bool}

    _plain_msg = {immp.Optional("user"): immp.Nullable(str),
                  immp.Optional("bot_id"): immp.Nullable(str),
                  immp.Optional("username"): immp.Nullable(str),
                  immp.Optional("icons", dict): dict,
                  "text": str,
                  **_base_msg}

    message = immp.Schema(immp.Any({"subtype": "file_comment", **_base_msg},
                                   {"subtype": "message_changed", **_base_msg},
                                   {"subtype": "message_deleted", "deleted_ts": str, **_base_msg},
                                   {"subtype": immp.Any("channel_name", "group_name"),
                                    "name": str, **_plain_msg},
                                   {immp.Optional("subtype"): immp.Nullable(str), **_plain_msg}))

    # Circular references to embedded messages.
    message.raw.choices[1].update({"message": message, "previous_message": message})

    event = immp.Schema(immp.Any(message,
                                 {"type": "team_pref_change",
                                  "name": str,
                                  "value": immp.Any()},
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

    socket_event = immp.Schema(immp.Any({"type": "events_api",
                                         immp.Optional("envelope_id"): immp.Nullable(str),
                                         "payload": {"type": "event_callback",
                                                     "event": event}},
                                        {"type": str,
                                         immp.Optional("envelope_id"): immp.Nullable(str)}))

    def _api(nested={}):
        return immp.Schema(immp.Any({"ok": True,
                                     immp.Optional("response_metadata", dict):
                                         {immp.Optional("next_cursor", ""): str},
                                     **nested},
                                    {"ok": False,
                                     "error": str}))

    socket_open = _api({"url": str})
    auth_test = _api({"user_id": str})
    team_info = _api({"team": {"id": str, "name": str, "domain": str}})
    users_list = _api({"members": [user]})
    bot_info = _api({"bot": bot})
    convs_list = _api({"channels": [_channel]})
    conv_open = _api({"channel": direct})
    conv_members = _api({"members": [str]})
    conv_history = _api({"messages": [message]})
    chat_post = _api({"channel": str, "message": message})
    file_upload = _api({"file": file})

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

    def __init__(self, id_=None, plug=None, display_name=None, real_name=None, avatar=None,
                 bot_id=None, app=False, raw=None):
        super().__init__(id_=id_,
                         plug=plug,
                         avatar=avatar,
                         raw=raw)
        self._display_name = display_name
        self._real_name = real_name
        self._real_name_override = None
        self.bot_id = bot_id
        self.app = app

    @property
    def real_name(self):
        if self._real_name_override:
            return self._real_name_override
        elif self.plug.config["real-names"]:
            return self._real_name or self._display_name
        else:
            return self._display_name or self._real_name

    @real_name.setter
    def real_name(self, value):
        self._real_name_override = value

    @property
    def link(self):
        return "https://{}.slack.com/{}/{}".format(self.plug._team["domain"],
                                                   "apps" if self.app else "team", self.id)

    @link.setter
    def link(self, value):
        pass

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
                   display_name=member["profile"]["display_name"],
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
        return cls(id_=(bot["app_id"] or bot["id"]),
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

    # If bold comes before italic here, "<b,i,l=http://example.com>B+I+L</> <i>just I</>" becomes
    # "*_<http://example.com|B+I+L>_* _just I_" and the first segment's italic breaks.  For some
    # reason this doesn't happen with bold and italic swapped here and in the text.
    tags = {"_": "italic", "*": "bold", "~": "strike", "`": "code", "```": "pre"}
    # A rather complicated expression to match formatting tags according to the following rules:
    # 1) Outside of formatting may not be adjacent to alphanumeric or other formatting characters.
    # 2) Inside of formatting may not be adjacent to whitespace or the current formatting character.
    # 3) Formatting characters may be escaped with a backslash.
    # This still isn't perfect, but provides a good approximation outside of edge cases.
    # Slack only has limited documentation: https://get.slack.help/hc/en-us/articles/202288908
    _outside_chars = r"0-9a-z*_~"
    _tag_chars = r"[*_~`]"
    _inside_chars = r"\s\1"
    _format_regex = re.compile(r"(?<![{0}\\])({1})(?![{2}])(.+?)(?<![{2}\\])\1(?![{0}])"
                               .format(_outside_chars, _tag_chars, _inside_chars))
    _pre_regex = re.compile(r"```\n?(.+?)\n?```", re.DOTALL)

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
                field = cls.tags[tag]
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
        for match in cls._link_regex.finditer(plain):
            # Store the link target; the link tag will be removed after segmenting.
            changes[match.start()]["link"] = cls._unescape(match.group(1))
            changes[match.end()]["link"] = None
        for match in cls._mention_regex.finditer(plain):
            changes[match.start()]["mention"] = await slack.user_from_id(match.group(1))
            changes[match.end()]["mention"] = None
        segments = []
        points = list(sorted(changes.keys()))
        formatting = {}
        # Iterate through text in change start/end pairs.
        for start, end in zip([0] + points, points + [len(plain)]):
            formatting.update(changes[start])
            if start == end:
                # Zero-length segment at the start or end, ignore it.
                continue
            if formatting.get("mention"):
                user = formatting["mention"]
                part = "@{}".format(user.real_name)
            else:
                part = plain[start:end]
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
            parsed = cls._escape(segment.text)
            if not segment.code and not segment.pre:
                link = None
                if not segment.mention:
                    link = segment.link
                elif slack.same_team(segment.mention.plug):
                    parsed = "<@{}>".format(segment.mention.id)
                else:
                    link = segment.mention.link
                if link:
                    parsed = "<{}|{}>".format(link, cls._escape(segment.text))
            text += parsed
        for tag in reversed(active):
            # Close all remaining tags.
            text += tag
        return text


class SlackFile(immp.File):
    """
    File attachment originating from Slack.
    """

    def __init__(self, slack, title=None, type_=immp.File.Type.unknown, source=None):
        super().__init__(title=title, type_=type_)
        self.slack = slack
        # Private source as the URL is not publicly accessible.
        self._source = source

    async def get_content(self, sess):
        headers = {"Authorization": "Bearer {}".format(self.slack.config["token"])}
        return await sess.get(self._source, headers=headers)

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
        if file["mode"] == "tombstone":
            # File has been deleted, but the container still references it.
            raise MessageNotFound
        if file["mimetype"].startswith("image/"):
            type_ = immp.File.Type.image
        elif file["mimetype"].startswith("video/"):
            type_ = immp.File.Type.video
        else:
            type_ = immp.File.Type.unknown
        return cls(slack,
                   title=file["name"],
                   type_=type_,
                   source=file["url_private"])


class SlackMessage(immp.Message):
    """
    Message originating from Slack.
    """

    _bot_lookup = Lock()

    @classmethod
    def from_unfurl(cls, slack, attach):
        unfurl = _Schema.msg_unfurl(attach)
        return immp.Receipt(unfurl["ts"], immp.Channel(slack, unfurl["channel_id"]))

    @classmethod
    def _parse_meta(cls, slack, event):
        id_ = event["ts"]
        at = datetime.fromtimestamp(float(event["ts"]), timezone.utc)
        return id_, at

    @classmethod
    async def _parse_author(cls, slack, event=None, author=None):
        user = None
        if author:
            pass
        elif not event:
            raise TypeError("Need either event or author")
        elif event["user"]:
            author = event["user"]
        elif event["bot_id"]:
            # Fetching a slice of channel history may call this method many times in parallel, with
            # many parallel lookups of the same bot ID -- force serial lookups here to try and avoid
            # unnecessary API requests if several of them refer to the same legacy bot user.
            async with cls._bot_lookup:
                bot_id = event["bot_id"]
                if bot_id in slack._bot_to_user:
                    # Event has the bot's app ID, not user ID.
                    author = slack._bot_to_user[bot_id]
                elif bot_id in slack._bot_to_app:
                    # Slack app with no bot presence, use the app metadata.
                    user = slack._bot_to_app[bot_id]
                else:
                    # Legacy bot (owned by the Slack API Tester app), or a bot from an uninstalled
                    # app, do a lookup and cache the resulting user.
                    try:
                        bot = await slack._api("bots.info", _Schema.bot_info, params={"bot": bot_id})
                    except SlackAPIError:
                        log.warning("Failed to resolve bot ID %r", bot_id, exc_info=True)
                    else:
                        user = SlackUser.from_bot(slack, bot["bot"])
                        slack._bot_to_app[bot_id] = user
        if author:
            user = await slack.user_from_id(author) or SlackUser(id_=author, plug=slack)
        if event["username"]:
            if user:
                user = copy(user)
                user.real_name = event["username"]
            else:
                user = immp.User(real_name=event["username"])
            icon = SlackUser._best_image(event["icons"])
            if icon:
                user.avatar = icon
        return user

    @classmethod
    async def _parse_main(cls, slack, json, event, channel, parent=True):
        id_, at = cls._parse_meta(slack, event)
        revision = event["edited"]["ts"]
        edited = bool(revision)
        deleted = False
        text = event["text"]
        user = await cls._parse_author(slack, event)
        action = False
        reply_to = joined = left = title = None
        attachments = []
        if user and text and re.match(r"<@{}(\|.*?)?> ".format(user.id), text):
            # Own username at the start of the message, assume it's an action.
            action = True
            text = re.sub(r"^<@{}(\|.*?)?> ".format(user.id), "", text)
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
        elif event["subtype"] == "reminder_add":
            action = True
            # Slack leaves a leading space in the message text: " set up a reminder..."
            text = text.lstrip()
        if event["thread_ts"] and event["ts"] != event["thread_ts"] and parent:
            thread = immp.Receipt(event["thread_ts"], channel)
            # Look for the current reply in the thread, and take the previous message as reply-to.
            last = None
            for entry in await slack.get_replies(thread):
                if entry.id == event["ts"]:
                    break
                last = entry
            try:
                if last:
                    reply_to = await slack.get_reply(thread, last.id, False)
                else:
                    # No previous reply, take the thread parent message as reply-to instead.
                    reply_to = await slack.get_message(thread, False)
            except MessageNotFound:
                reply_to = last or thread
        for file in event["files"]:
            try:
                attachments.append(SlackFile.from_file(slack, file))
            except MessageNotFound:
                pass
        for attach in event["attachments"]:
            if attach["is_msg_unfurl"]:
                # We have the message ID as the timestamp, fetch the whole message to embed it.
                try:
                    unfurl = cls.from_unfurl(slack, attach)
                    attachments.append(await slack.resolve_message(unfurl))
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
            regex = r"https://{}.slack.com/archives/([^/]+)/p([0-9]+)".format(slack._team["domain"])
            for channel_id, link in re.findall(regex, text):
                # Archive links are strange and drop the period from the ts value.
                ts = link[:-6] + "." + link[-6:]
                refs = [attach.id for attach in attachments if isinstance(attach, immp.Receipt)]
                if ts not in refs:
                    try:
                        receipt = immp.Receipt(ts, immp.Channel(slack, channel_id))
                        attachments.append(await slack.resolve_message(receipt))
                    except MessageNotFound:
                        pass
            if re.match("^<{}>$".format(regex), text):
                # Strip the message text if the entire body was just a link.
                text = None
            else:
                text = await SlackRichText.from_mrkdwn(slack, text)
        return immp.SentMessage(id_=id_,
                                channel=channel,
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
        if event["hidden"] and event["subtype"] != "message_changed":
            # Ignore most UI-hidden events (e.g. tombstones of deleted files).
            raise NotImplementedError("hidden")
        if event["is_ephemeral"]:
            # Ignore user-private messages from Slack (e.g. over quota warnings, link unfurling
            # opt-in prompts etc.) which shouldn't be served to message processors.
            raise NotImplementedError("ephemeral")
        if event["subtype"] == "file_comment":
            # Deprecated in favour of file threads, but Slack may still emit these.
            raise NotImplementedError("deprecated")
        channel = immp.Channel(slack, event["channel"])
        if event["subtype"] == "message_deleted":
            id_, at = cls._parse_meta(slack, event)
            return immp.SentMessage(id_=id_,
                                    channel=channel,
                                    at=at,
                                    revision=event["ts"],
                                    deleted=True,
                                    raw=json)
        elif event["subtype"] == "message_changed":
            if event["message"]["hidden"]:
                # We might get updates to messages that are themselves hidden.
                raise NotImplementedError("hidden")
            if event["message"]["text"] == event["previous_message"]["text"]:
                # Message remains unchanged.  Can be caused by link unfurling (adds an attachment)
                # or deleting replies (reply is removed from event.replies in new *and old*).
                raise NotImplementedError("unchanged")
            # Original message details are under a nested "message" key.
            return await cls._parse_main(slack, json, event["message"], channel, parent)
        else:
            return await cls._parse_main(slack, json, event, channel, parent)

    @classmethod
    async def from_post(cls, slack, json):
        """
        Convert an API post response :class:`dict` to a :class:`.Message`.

        Args:
            slack (.SlackPlug):
                Related plug instance that provides the event.
            json (dict):
                Slack API response payload from
                `postMessage <https://api.slack.com/methods/chat.postMessage#response>`_.

        Returns:
            .SlackMessage:
                Parsed message object.
        """
        post = _Schema.chat_post(json)
        channel = immp.Channel(slack, post["channel"])
        return await cls._parse_main(slack, json, post["message"], channel, True)

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


class SlackPlug(immp.Plug, immp.HTTPOpenable):
    """
    Plug for a `Slack <https://slack.com>`_ team.
    """

    schema = _Schema.config

    @property
    def network_name(self):
        return "{} Slack".format(self._team["name"]) if self._team else "Slack"

    @property
    def network_id(self):
        return "slack:{}:{}".format(self._team["id"], self._bot_user) if self._team else None

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        self._team = self._bot_user = None
        self._users = self._channels = self._directs = None
        self._bot_to_app = self._bot_to_user = self._members = None
        # Connection objects that need to be closed on disconnect.
        self._socket = self._receive = None
        self._app_socket = False
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

    async def _api(self, endpoint, schema=_Schema.api, app=False, *, headers=None, **kwargs):
        headers = headers or {}
        token = app and self.config["app-token"] or self.config["token"]
        headers["Authorization"] = "Bearer {}".format(token)
        log.debug("User %r making API request to %r", self._bot_user, endpoint)
        async with self.session.post("https://slack.com/api/{}".format(endpoint),
                                     headers=headers, **kwargs) as resp:
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

    async def _paged(self, endpoint, schema, key, app=False, *, params=None, **kwargs):
        params = params or {}
        items = []
        while True:
            data = await self._api(endpoint, schema, app, params=params, **kwargs)
            items += data[key]
            if data["response_metadata"]["next_cursor"]:
                params["cursor"] = data["response_metadata"]["next_cursor"]
            else:
                break
        return items

    async def _rtm(self):
        auth = await self._api("auth.test", _Schema.auth_test)
        self._bot_user = auth["user_id"]
        # Cache useful information about users and channels, to save on queries later.
        reqs = (self._api("team.info", _Schema.team_info),
                self._paged("users.list", _Schema.users_list, "members"),
                self._paged("conversations.list", _Schema.convs_list, "channels",
                            params={"types": "public_channel,private_channel,mpim,im"}))
        team, users, convs = await gather(*reqs)
        self._team = team["team"]
        self._users = {u["id"]: SlackUser.from_member(self, u) for u in users}
        self._directs = {c["id"]: c for c in convs if c["is_im"]}
        self._channels = {c["id"]: c for c in convs if c["id"] not in self._directs}
        log.debug("User %r cached %d users, %d channels, %d IMs", self._bot_user,
                  len(self._users), len(self._channels), len(self._directs))
        self._members = {}
        self._bot_to_app = {}
        # Create a map of bot IDs to users, as the bot cache doesn't contain references to them.
        self._bot_to_user = {user.bot_id: user.id for user in self._users.values() if user.bot_id}
        log.debug("User %r requesting websocket session", self._bot_user)
        if self.config["app-token"]:
            rtm = await self._api("apps.connections.open", _Schema.socket_open, True)
            self._app_socket = True
        else:
            rtm = await self._api("rtm.connect", _Schema.socket_open)
            self._app_socket = False
        self._socket = await self.session.ws_connect(rtm["url"], heartbeat=60.0)
        log.debug("User %r connected to websocket", self._bot_user)

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
        return user.id == self._bot_user

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
        params = {"users": user.id,
                  "return_im": "true"}
        opened = await self._api("conversations.open", _Schema.conv_open, params=params)
        channel = opened["channel"]
        self._directs[channel["id"]] = channel
        return immp.Channel(self, channel["id"])

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
            members = await self._paged("conversations.members", _Schema.conv_members, "members",
                                        data={"channel": channel.source})
            self._members[channel.source] = members
        return await gather(*(self.user_from_id(member)
                              for member in self._members[channel.source]))

    async def channel_invite(self, channel, user):
        if user.id == self._bot_user:
            await self._api("conversations.join", params={"channel": channel.source})
        else:
            await self._api("conversations.invite", params={"channel": channel.source,
                                                            "user": user.id})

    async def channel_remove(self, channel, user):
        if user.id == self._bot_user:
            await self._api("conversations.leave", params={"channel": channel.source})
        else:
            await self._api("conversations.kick", params={"channel": channel.source,
                                                          "user": user.id})

    async def channel_history(self, channel, before=None):
        params = {"channel": channel.source}
        if before:
            params["latest"] = before.id
        history = await self._api("conversations.history", _Schema.conv_history, params=params)
        messages = list(reversed(history["messages"]))
        for msg in messages:
            msg["channel"] = channel.source
        return await gather(*(SlackMessage.from_event(self, msg) for msg in messages))

    async def get_message(self, receipt, parent=True):
        params = {"channel": receipt.channel.source,
                  "latest": receipt.id,
                  "inclusive": "true",
                  "limit": 1}
        try:
            history = await self._api("conversations.history", _Schema.conv_history, params=params)
        except SlackAPIError:
            log.debug("API error retrieving message %r from %r", receipt.id, receipt.channel.source,
                      exc_info=True)
            raise MessageNotFound from None
        if history["messages"]:
            msg = history["messages"][0]
            if msg["ts"] == receipt.id:
                msg["channel"] = receipt.channel.source
                return await SlackMessage.from_event(self, msg, parent)
        log.debug("Failed to find message %r in %r", receipt.id, receipt.channel.source)
        raise MessageNotFound

    async def get_replies(self, receipt):
        params = {"channel": receipt.channel.source, "ts": receipt.id}
        try:
            replies = await self._api("conversations.replies", _Schema.conv_history, params=params)
        except SlackAPIError as e:
            log.debug("API error retrieving replies %r from %r: %r",
                      receipt.id, receipt.channel.source, e.args[0])
            raise MessageNotFound from None
        else:
            return [immp.Receipt(msg["ts"], receipt.channel,
                                 at=datetime.fromtimestamp(float(msg["ts"]), timezone.utc))
                    for msg in replies["messages"] if msg["ts"] != receipt.id]

    async def get_reply(self, receipt, reply_ts, parent=True):
        params = {"channel": receipt.channel.source,
                  "ts": receipt.id,
                  "latest": reply_ts,
                  "inclusive": "true",
                  "limit": 1}
        try:
            replies = await self._api("conversations.replies", _Schema.conv_history, params=params)
        except SlackAPIError as e:
            log.debug("API error retrieving reply %r -> %r from %r: %r",
                      receipt.id, reply_ts, receipt.channel.source, e.args[0])
            raise MessageNotFound from None
        if replies["messages"]:
            msg = replies["messages"][-1]
            if msg["ts"] == reply_ts:
                msg["channel"] = receipt.channel.source
                return await SlackMessage.from_event(self, msg, parent)
        log.debug("Reply %r -> %r not found in %r", receipt.id, reply_ts, receipt.channel.source)
        raise MessageNotFound

    async def _post(self, channel, parent, msg):
        receipts = []
        uploads = 0
        name = None
        data = {"channel": channel.source}
        if msg.user:
            data["username"] = name = msg.user.real_name or msg.user.username
            if msg.user.avatar:
                # Slack permanently caches user icon URLs; add monotonic hashes to keep them fresh.
                data["icon_url"] = "{}#{}".format(msg.user.avatar, int(time.time()))
            elif self.config["fallback-image"]:
                data["icon_url"] = self.config["fallback-image"]
        if not self.config["app-token"]:
            data["as_user"] = False if msg.user else True
        for attach in msg.attachments:
            if isinstance(attach, immp.File):
                # Upload each file to Slack.
                form = FormData({"channels": channel.source,
                                 "filename": attach.title or ""})
                if isinstance(parent.reply_to, immp.Receipt):
                    # Reply directly to the corresponding thread.  Note that thread_ts can be any
                    # message in the thread, it need not be resolved to the parent.
                    form.add_field("thread_ts", msg.reply_to.id)
                    if self.config["thread-broadcast"]:
                        form.add_field("broadcast", "true")
                if name:
                    comment = immp.RichText([immp.Segment(name, bold=True, italic=True,
                                                          link=msg.user.link),
                                             immp.Segment(" uploaded this file", italic=True)])
                    form.add_field("initial_comment", SlackRichText.to_mrkdwn(self, comment))
                img_resp = await attach.get_content(self.session)
                form.add_field("file", img_resp.content, filename="file")
                upload = await self._api("files.upload", _Schema.file_upload, data=form)
                uploads += 1
                for shared in upload["file"]["shares"].values():
                    if channel.source in shared:
                        ids = [share["ts"] for share in shared[channel.source]]
                        receipts += [immp.Receipt(id_, channel) for id_ in ids]
        if len(receipts) < uploads:
            log.warning("Missing some file shares: sent %d, got %d", uploads, len(receipts))
        rich = None
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
            chunks = []
            if rich:
                text = SlackRichText.to_mrkdwn(self, rich)
                chunks = immp.RichText.chunked_plain(text, 4000)
            items = []
            if attachments:
                item = dict(data)
                if len(chunks) == 1:
                    # Attach the only embed to the message text.
                    item["text"] = chunks.pop()
                item["attachments"] = json_dumps(attachments)
                items.append(item)
            for chunk in chunks:
                item = dict(data)
                item["text"] = chunk
                items.append(item)
            for item in items:
                post = await self._api("chat.postMessage", _Schema.chat_post, data=item)
                receipts.append(await SlackMessage.from_post(self, post))
        return receipts

    async def put(self, channel, msg):
        clone = copy(msg)
        if clone.text:
            clone.text = msg.text.clone()
        forwards = []
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
                forwards += await self._post(channel, clone, attach)
        own = await self._post(channel, clone, clone)
        if forwards and not own and msg.user:
            # Forwarding a message but no content to show who forwarded it.
            info = immp.Message(user=msg.user, action=True, text="forwarded a message")
            own += await self._post(channel, msg, info)
        return forwards + own

    async def delete(self, sent):
        await self._api("chat.delete", params={"channel": sent.channel.source, "ts": sent.id})

    async def _poll(self):
        while self.state == immp.OpenState.active and not self._closing:
            try:
                json = await self._socket.receive_json()
            except CancelledError:
                log.debug("User %r cancelling polling", self._bot_user)
                return
            except TypeError as e:
                if self._closing:
                    return
                log.debug("User %r in unexpected socket state: %r", self._bot_user, e)
                await self._socket.close()
                self._socket = None
                log.debug("Reconnecting in 3 seconds")
                await sleep(3)
                await self._rtm()
                continue
            if self._app_socket:
                wrapper = _Schema.socket_event(json)
                if "envelope_id" in wrapper:
                    await self._socket.send_json({"envelope_id": wrapper["envelope_id"]})
                if wrapper["type"] != "events_api" or not wrapper["payload"]:
                    log.debug("User %r ignoring unknown Socket Mode event %r",
                              self._bot_user, wrapper["type"])
                    continue
                payload = wrapper["payload"]
                if payload["type"] != "event_callback":
                    log.debug("User %r ignoring unknown Events API callback %r",
                              self._bot_user, payload["type"])
                    continue
                event = payload["event"]
            else:
                event = payload = _Schema.event(json)
            log.debug("User %r received a %r event", self._bot_user, event["type"])
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
                except NotImplementedError as e:
                    log.debug("Ignoring message with ts %r (%s)", event.get("ts"), e.args[0])
                else:
                    self.queue(sent)
