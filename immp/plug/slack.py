"""
Connect to a `Slack <https://slack.com>`_ workspace as a bot.

Config:
    token (str):
        Slack API token for a bot user.
    fallback-name (str):
        Name to display for incoming messages without an attached user (default: ``IMMP``).
    fallback-image (str):
        Avatar to display for incoming messages without a user or image (default: none).

You'll need to create either a full `Slack App <https://api.slack.com/apps>`_ and add a bot, or
a `Bot Integration <https://my.slack.com/apps/A0F7YS25R-bots>`_.  In either case, you should end up
with a token prefixed ``xoxb-``.

If multiple Slack workspaces are involved, you will need a separate bot and plug setup per team.
Enterprise Grid support has not been tested, and will likely have issues if plugs are configured
for two workspaces in the same grid.
"""

from asyncio import CancelledError, ensure_future, sleep
from collections import defaultdict
from datetime import datetime
from functools import partial
from json import dumps as json_dumps
import logging
import re

from aiohttp import ClientResponseError, ClientSession, FormData
from emoji import emojize
from voluptuous import ALLOW_EXTRA, Any, Match, Optional, Schema

import immp


log = logging.getLogger(__name__)


class _Schema(object):

    config = Schema({"token": str,
                     Optional("fallback-name", default="IMMP"): str,
                     Optional("fallback-image", default=None): Any(str, None)},
                    extra=ALLOW_EXTRA, required=True)

    user = Schema({"id": str,
                   "name": str,
                   "profile": {Optional("real_name", default=None): Any(str, None),
                               Optional(Match(r"image_(original|\d+)")): Any(str, None),
                               Optional("bot_id", default=None): Any(str, None)}},
                  extra=ALLOW_EXTRA, required=True)

    channel = Schema({"id": str,
                      "name": str},
                     extra=ALLOW_EXTRA, required=True)

    direct = Schema({"id": str,
                     "user": str},
                    extra=ALLOW_EXTRA, required=True)

    file = Schema({"id": str,
                   "name": Any(str, None),
                   "pretty_type": str,
                   "url_private": str},
                  extra=ALLOW_EXTRA, required=True)

    _base_msg = Schema({"ts": str,
                        "type": "message",
                        Optional("channel", default=None): Any(str, None),
                        Optional("edited", default={"user": None}):
                            {Optional("user", default=None): Any(str, None)},
                        Optional("thread_ts", default=None): Any(str, None)},
                       extra=ALLOW_EXTRA, required=True)

    _plain_msg = _base_msg.extend({"user": str, "text": str})

    message = Schema(Any(_base_msg.extend({"subtype": "bot_message",
                                           "bot_id": str,
                                           "text": str,
                                           Optional("username", default=None): Any(str, None),
                                           Optional("icons", default=dict): Any(dict, None)}),
                         _base_msg.extend({"subtype": "message_changed",
                                           "message": lambda v: _Schema.message(v)}),
                         _base_msg.extend({"subtype": "message_deleted",
                                           "deleted_ts": str}),
                         _plain_msg.extend({"subtype": Any("file_share", "file_mention"),
                                            "file": file}),
                         _plain_msg.extend({Optional("subtype", default=None): Any(str, None)})))

    event = Schema(Any(message,
                       {"type": Any("team_join", "user_change"),
                        "user": user},
                       {"type": Any("channel_joined", "group_joined", "im_created"),
                        "channel": {"id": str}},
                       {"type": str,
                        Optional("subtype", default=None): Any(str, None)},
                       extra=ALLOW_EXTRA, required=True))

    def _api(nested={}):
        return Schema(Any({"ok": True,
                           Optional("response_metadata", default={"next_cursor": ""}):
                               {Optional("next_cursor", default=""): str},
                           **nested},
                          {"ok": False,
                           "error": str},
                          extra=ALLOW_EXTRA, required=True))

    rtm = _api({"url": str,
                "team": {"domain": str},
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


class SlackAPIError(immp.PlugError):
    """
    Generic error from the Slack API.
    """


class SlackUser(immp.User):
    """
    User present in Slack.

    Attributes:
        bot_id (str):
            Reference to the Slack integration app for a bot user.
    """

    def __init__(self, id=None, plug=None, username=None, real_name=None, avatar=None,
                 bot_id=None, raw=None):
        super().__init__(id=id,
                         plug=plug,
                         username=username,
                         real_name=real_name,
                         avatar=avatar,
                         raw=raw)
        self.bot_id = bot_id
        self._workspace = plug._team["domain"]

    @property
    def link(self):
        if self.id and self._workspace:
            return "https://{}.slack.com/team/{}".format(self._workspace, self.id)

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
            slack (.SlackPlug):
                Related plug instance that provides the user.
            json (dict):
                Slack API `user <https://api.slack.com/types/user>`_ object.

        Returns:
            .SlackUser:
                Parsed user object.
        """
        member = _Schema.user(json)
        return cls(id=member["id"],
                   plug=slack,
                   username=member["name"],
                   real_name=member["profile"]["real_name"],
                   avatar=cls._best_image(member["profile"]),
                   bot_id=member["profile"]["bot_id"],
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
    _tag_chars = r"*_~`"
    _inside_chars = r"\s\1"
    _format_regex = re.compile(r"(?<![{0}\\])(```|[{1}])(?![{2}])(.+?)(?<![{2}\\])\1(?![{0}])"
                               .format(_outside_chars, _tag_chars, _inside_chars))

    @classmethod
    def _sub_channel(cls, slack, match):
        return "#{}".format(slack._channels[match.group(1)]["name"])

    @classmethod
    def _sub_link(cls, match):
        # Use a label if we have one, else just show the URL.
        return match.group(2) or match.group(1)

    @classmethod
    def from_mrkdwn(cls, slack, text):
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
        for match in re.finditer(r"<([^@#\|][^\|>]*)(?:\|([^>]+))?>", text):
            # Store the link target; the link tag will be removed after segmenting.
            changes[match.start()]["link"] = match.group(1)
            changes[match.end()]["link"] = None
        for match in re.finditer(r"<@([^\|>]+)(?:\|[^>]+)?>", text):
            changes[match.start()]["mention"] = slack._users[match.group(1)]
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
                # Strip Slack channel tags, replace with a plain-text representation.
                part = re.sub(r"<#([^\|>]+)(?:\|[^>]+)?>", partial(cls._sub_channel, slack), part)
                part = re.sub(r"<([^\|>]+)(?:\|([^>]+))?>", cls._sub_link, part)
            segments.append(immp.Segment(part, **changes[start]))
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
                text += "<{}|{}>".format(segment.link, segment.text)
            else:
                text += segment.text
        for tag in reversed(active):
            # Close all remaining tags.
            text += tag
        return text


class SlackFile(immp.File):

    def __init__(self, slack, title=None, type=None, source=None):
        super().__init__(title=title, type=type)
        self.slack = slack
        # Private source as the URL is not publicly accessible.
        self._source = source

    async def get_content(self, sess=None):
        sess = sess or self.slack._session
        headers = {"Authorization": "Bearer {}".format(self.slack._token)}
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
        type = immp.File.Type.unknown
        if file["mimetype"].startswith("image/"):
            type = immp.File.Type.image
        return cls(slack,
                   title=file["name"],
                   type=type,
                   source=file["url_private"])


class SlackMessage(immp.Message):
    """
    Message originating from Slack.
    """

    @classmethod
    async def from_event(cls, slack, json):
        """
        Convert an API event :class:`dict` to a :class:`.Message`.

        Args:
            slack (.SlackPlug):
                Related plug instance that provides the event.
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
        reply_to = None
        joined = None
        left = None
        attachments = []
        if event["subtype"] == "bot_message":
            # Event has the bot's app ID, not user ID.
            author = slack._bot_to_user.get(event["bot_id"])
            text = event["text"]
        elif event["subtype"] == "message_changed":
            # Original message details are under a nested "message" key.
            original = event["message"]["ts"]
            text = event["message"]["text"]
            # NB: Editing user may be different to the original sender.
            author = event["message"]["edited"]["user"] or event["message"]["user"]
        elif event["subtype"] == "message_deleted":
            original = event["deleted_ts"]
            author = None
            text = None
            deleted = True
        else:
            author = event["user"]
            text = event["text"]
        user = None
        if author:
            user = slack._users.get(author, SlackUser(id=author, plug=slack))
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
        if author and text and re.match(r"<@{}(\|.*?)?> ".format(author), text):
            # Own username at the start of the message, assume it's an action.
            action = True
            text = re.sub(r"^<@{}|.*?> ".format(author), "", text)
        if event["thread_ts"] and event["thread_ts"] != event["ts"]:
            # We have the parent ID, fetch the rest of the message to embed it.
            params = {"channel": event["channel"],
                      "latest": event["thread_ts"],
                      "inclusive": "true",
                      "limit": 1}
            history = await slack._api("conversations.history", _Schema.history, params=params)
            if history["messages"] and history["messages"][0]["ts"] == event["thread_ts"]:
                reply_to = (await cls.from_event(slack, history["messages"][0]))[1]
        return (slack.host.resolve_channel(slack, event["channel"]),
                cls(id=event["ts"],
                    at=datetime.fromtimestamp(int(float(event["ts"]))),
                    original=original,
                    text=SlackRichText.from_mrkdwn(slack, text) if text else None,
                    user=user,
                    action=action,
                    deleted=deleted,
                    reply_to=reply_to,
                    joined=joined,
                    left=left,
                    attachments=attachments,
                    raw=json))


class SlackPlug(immp.Plug):
    """
    Plug for a `Slack <https://slack.com>`_ team.
    """

    class Meta(immp.Plug.Meta):
        network = "Slack"

    def __init__(self, name, config, host):
        super().__init__(name, _Schema.config(config), host)
        self._team = self._users = self._channels = self._directs = self._bots = None
        # Connection objects that need to be closed on disconnect.
        self._session = self._socket = self._receive = None
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

    async def _api(self, endpoint, schema, params=None, **kwargs):
        params = params or {}
        params["token"] = self.config["token"]
        log.debug("Making API request to '{}'".format(endpoint))
        async with self._session.post("https://slack.com/api/{}".format(endpoint),
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
        self._team = rtm["team"]
        self._users = {u["id"]: SlackUser.from_member(self, u) for u in rtm["users"]}
        log.debug("Users ({}): {}".format(len(self._users), ", ".join(self._users.keys())))
        self._channels = {c["id"]: c for c in rtm["channels"] + rtm["groups"]}
        log.debug("Channels ({}): {}".format(len(self._channels), ", ".join(self._channels.keys())))
        self._directs = {c["id"]: c for c in rtm["ims"]}
        log.debug("Directs ({}): {}".format(len(self._directs), ", ".join(self._directs.keys())))
        self._bots = {b["id"]: b for b in rtm["bots"] if not b["deleted"]}
        log.debug("Bots ({}): {}".format(len(self._bots), ", ".join(self._bots.keys())))
        # Create a map of bot IDs to users, as the bot cache doesn't contain references to them.
        self._bot_to_user = {user.bot_id: user.id for user in self._users.values() if user.bot_id}
        self._socket = await self._session.ws_connect(rtm["url"])
        log.debug("Connected to websocket")

    async def start(self):
        await super().start()
        self._closing = False
        self._session = ClientSession()
        await self._rtm()

    async def stop(self):
        await super().stop()
        self._closing = True
        if self._receive:
            self._receive.cancel()
        if self._socket:
            log.debug("Closing websocket")
            await self._socket.close()
            self._socket = None
        if self._session:
            log.debug("Closing session")
            await self._session.close()
            self._session = None

    async def user_from_id(self, id):
        return self._users.get(id)

    async def user_from_username(self, username):
        for id, user in self._users.items():
            if user.username == username:
                return user
        return None

    async def private_channel(self, user):
        if not isinstance(user, SlackUser):
            return
        for direct in self._directs.values():
            if direct["user"] == user.id:
                return immp.Channel(None, self, direct["id"])
        # Private channel doesn't exist yet or isn't cached.
        params = {"user": user.id,
                  "return_im": "true"}
        opened = await self._api("im.open", _Schema.im_open, params=params)
        return immp.Channel(None, self, opened["channel"]["id"])

    async def channel_members(self, channel):
        if channel.plug is not self:
            return None
        members = await self._paged("conversations.members", _Schema.members, "members",
                                    data={"channel": channel.source})
        return [self._users[member] for member in members]

    async def put(self, channel, msg):
        if msg.deleted:
            # TODO
            return []
        uploads = []
        sent = []
        for attach in msg.attachments:
            if isinstance(attach, immp.File):
                # Upload each file to Slack.
                data = FormData({"channels": channel.source,
                                 "filename": attach.title or ""})
                img_resp = await attach.get_content(self._session)
                data.add_field("file", img_resp.content, filename="file")
                upload = await self._api("files.upload", _Schema.upload, data=data)
                uploads.append(upload["file"]["id"])
        for upload in uploads:
            # Slack doesn't provide us with a message ID, so we have to find it ourselves.
            params = {"channel": channel.source,
                      "limit": 100}
            history = await self._api("conversations.history", _Schema.history, params=params)
            for message in history["messages"]:
                if message["subtype"] in ("file_share", "file_mention"):
                    if message["file"]["id"] in uploads:
                        sent.append(message["ts"])
            if len(sent) < len(uploads):
                # Of the 100 messages we just looked at, at least one file wasn't found.
                log.debug("Missing some file upload messages")
        if msg.user:
            name = msg.user.real_name or msg.user.username
            image = msg.user.avatar
        else:
            name = self.config["fallback-name"]
            image = self.config["fallback-image"]
        data = {"channel": channel.source,
                "as_user": False,
                "username": name,
                "icon_url": image}
        if msg.text:
            if isinstance(msg.text, immp.RichText):
                rich = msg.text.clone()
            else:
                rich = immp.RichText([immp.Segment(msg.text)])
            if msg.action:
                for segment in rich:
                    segment.italic = True
            data["text"] = SlackRichText.to_mrkdwn(self, rich)
        elif uploads:
            what = "{} files".format(len(uploads)) if len(uploads) > 1 else "this file"
            data["text"] = "_shared {}_".format(what)
        attachments = []
        if msg.reply_to:
            quote = {"footer": ":speech_balloon:",
                     "ts": msg.reply_to.at.timestamp()}
            if msg.reply_to.user:
                quote["author_name"] = msg.reply_to.user.real_name or msg.reply_to.user.username
                quote["author_icon"] = msg.reply_to.user.avatar
            quoted_rich = None
            quoted_action = False
            if msg.reply_to.text:
                if isinstance(msg.reply_to.text, immp.RichText):
                    quoted_rich = msg.reply_to.text.clone()
                else:
                    quoted_rich = immp.RichText([immp.Segment(msg.reply_to.text)])
            elif msg.reply_to.attachments:
                quoted_action = True
                count = len(msg.reply_to.attachments)
                what = "{} files".format(count) if count > 1 else "this file"
                quoted_rich = immp.RichText([immp.Segment("sent {}".format(what))])
            if quoted_rich:
                if quoted_action:
                    for segment in quoted_rich:
                        segment.italic = True
                quote["text"] = SlackRichText.to_mrkdwn(self, quoted_rich)
                quote["mrkdwn_in"] = ["text"]
            attachments.append(quote)
        for attach in msg.attachments:
            if isinstance(attach, immp.Location):
                coords = "{}, {}".format(attach.latitude, attach.longitude)
                fallback = "{} ({})".format(attach.address, coords) if attach.address else coords
                attachments.append({"fallback": fallback,
                                    "title": attach.name or "Location",
                                    "title_link": attach.google_map_url,
                                    "text": attach.address,
                                    "footer": "{}, {}".format(attach.latitude, attach.longitude)})
        data["attachments"] = json_dumps(attachments)
        post = await self._api("chat.postMessage", _Schema.post, data=data)
        sent.append(post["ts"])
        return sent

    async def get(self):
        while self.state == immp.OpenState.active and not self._closing:
            self._receive = ensure_future(self._socket.receive_json())
            try:
                json = await self._receive
            except CancelledError:
                log.debug("Cancel request for plug '{}' getter".format(self.name))
                return
            except TypeError as e:
                if self._closing:
                    return
                log.debug("Unexpected socket state: {}".format(e))
                await self._socket.close()
                self._socket = None
                log.debug("Reconnecting in 3 seconds")
                await sleep(3)
                await self._rtm()
                continue
            finally:
                self._receive = None
            event = _Schema.event(json)
            log.debug("Received a '{}' event".format(event["type"]))
            if event["type"] in ("team_join", "user_change"):
                # A user appeared or changed, update our cache.
                self._users[event["user"]["id"]] = SlackUser.from_member(self, event["user"])
            elif event["type"] in ("channel_joined", "group_joined"):
                # A group or channel appeared, add to our cache.
                self._channels[event["channel"]["id"]] = event["channel"]
            elif event["type"] == "im_created":
                # A DM appeared, add to our cache.
                self._directs[event["channel"]["id"]] = event["channel"]
            elif event["type"] == "message" and not event["subtype"] == "message_replied":
                # A new message arrived, push it back to the host.
                yield (await SlackMessage.from_event(self, event))