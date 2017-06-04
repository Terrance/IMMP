import asyncio
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
    """

    @classmethod
    def from_member(cls, slack, member):
        """
        Convert an API member :class:`dict` to an :class:`imirror.User`.

        Args:
            slack (.SlackTransport):
                Related transport instance that provides the user.
            member (dict):
                Slack API `user <https://api.slack.com/types/user>`_ object.

        Returns:
            .SlackUser:
                Parsed user object.
        """
        id = member["id"]
        username = member["name"]
        real_name = member["profile"].get("real_name")
        avatar = member["profile"].get("image_512")
        return cls(id, username=username, real_name=real_name, avatar=avatar, raw=member)


class SlackMessage(imirror.Message):
    """
    Message originating from Slack.
    """

    @classmethod
    def from_event(cls, slack, event):
        """
        Convert an API event :class:`dict` to an :class:`imirror.Message`.

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
        return cls(id, channel, at=at, original=original, text=text, user=user, action=action,
                   deleted=deleted, reply_to=reply_to, joined=joined, left=left, raw=event)


class SlackTransport(imirror.Transport):
    """
    Transport for a `Slack <https://slack.com>`_ team.

    Config:
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
        resp = await self.session.post("https://slack.com/api/rtm.start",
                                       data={"token": self.token})
        json = await resp.json()
        if not json["ok"]:
            raise SlackAPIError(json["error"])
        # Cache useful information about users and channels, to save on queries later.
        self.team = json["team"]
        self.users = {u["id"]: SlackUser.from_member(self, u) for u in json["users"]}
        log.debug("Users ({}): {}".format(len(self.users), ", ".join(self.users.keys())))
        self.channels = {c["id"]: c for c in json["channels"] + json["groups"]}
        log.debug("Channels ({}): {}".format(len(self.channels), ", ".join(self.channels.keys())))
        self.directs = {c["id"]: c for c in json["ims"]}
        log.debug("Directs ({}): {}".format(len(self.directs), ", ".join(self.directs.keys())))
        self.bots = {b["id"]: b for b in json["bots"] if not b["deleted"]}
        log.debug("Bots ({}): {}".format(len(self.bots), ", ".join(self.bots.keys())))
        # Create a map of bot IDs to users, as the bot cache doesn't contain references to them.
        self.bot2user = {}
        for user in self.users.values():
            if user.raw.get("profile", {}).get("bot_id"):
                self.bot2user[user.raw["profile"]["bot_id"]] = user.id
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
            data = {"channel": channel.source,
                    "username": msg.user.username or msg.user.real_name,
                    "icon_url": msg.user.avatar,
                    # TODO: Handle rich text.
                    "text": str(msg.text)}
            # Block event processing whilst we wait for the message to go through. Processing will
            # resume once the caller yields or returns.
            resp = await self.session.post("https://slack.com/api/chat.postMessage",
                                           data=dict(data, token=self.token))
            json = await resp.json()
        if not json["ok"]:
            raise SlackAPIError(json["error"])
        return json["ts"]

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
            if event["type"] in ("team_join", "user_change"):
                # A user appeared or changed, update our cache.
                self.users[event["user"]["id"]] = event["user"]
            elif event["type"] in ("channel_joined", "group_joined"):
                # A group or channel appeared, add to our cache.
                self.channels[event["channel"]["id"]] = event["channel"]
            elif event["type"] == "im_created":
                # A DM appeared, add to our cache.
                self.directs[event["channel"]["id"]] = event["channel"]
            elif event["type"] == "message":
                # A new message arrived, push it back to the host.
                yield SlackMessage.from_event(self, event)
