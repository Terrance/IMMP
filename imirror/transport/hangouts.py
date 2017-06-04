import asyncio
import logging
import re

import hangups
from hangups import hangouts_pb2

import imirror


log = logging.getLogger(__name__)
logging.getLogger("hangups").setLevel(logging.WARNING)


class HangoutsUser(imirror.User):
    """
    User present in Hangouts.
    """

    @classmethod
    def from_user(cls, hangouts, user):
        """
        Convert a :class:`hangups.user.User` into a :class:`.User`.

        Args:
            hangouts (.HangoutsTransport):
                Related transport instance that provides the user.
            user (hangups.user.User):
                Hangups user object retrieved from the user list.

        Returns:
            .HangoutsUser:
                Parsed user object.
        """
        id = user.id_.chat_id
        # No usernames here, just the ID.
        real_name = user.full_name
        avatar = re.sub("^//", "https://", user.photo_url)
        return cls(id, real_name=real_name, avatar=avatar, raw=user)


class HangoutsMessage(imirror.Message):
    """
    Message originating from Hangouts.
    """

    @classmethod
    def from_event(cls, hangouts, event):
        """
        Convert a :class:`hangups.ChatMessageEvent` into a :class:`.Message`.

        Args:
            hangouts (.HangoutsTransport):
                Related transport instance that provides the event.
            event (hangups.ChatMessageEvent):
                Hangups message event emitted from a conversation.

        Returns:
            .HangoutsMessage:
                Parsed message object.
        """
        id = event.id_
        channel = hangouts.host.resolve_channel(hangouts, event.conversation_id)
        text = event.text
        user = HangoutsUser.from_user(hangouts, hangouts.users.get_user(event.user_id))
        action = False
        if any(a.type == 4 for a in event._event.chat_message.annotation):
            # This is a /me message sent from desktop Hangouts.
            action = True
            # The user's first name prefixes the message text, so try to strip that.
            if user.real_name:
                # We don't have a clear-cut first name, so try to match parts of names.
                # Try the full name first, then split successive words off the end.
                parts = user.real_name.split()
                for pos in range(len(parts), 0, -1):
                    sub_name = " ".join(parts[:pos])
                    if text.startswith(sub_name):
                        text = text[len(sub_name) + 1:]
                        break
                else:
                    # Couldn't match the user's name to the message text.
                    pass
        return cls(id, channel, text=text, user=user, action=action, raw=event)


class HangoutsTransport(imirror.Transport):
    """
    Transport for `Google Hangouts <https://hangouts.google.com>`_.

    Config:
        cookie (str):
            Path to a cookie text file read/written by :func:`hangups.get_auth_stdin`.
    """

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        try:
            self.cookie = config["cookie"]
        except KeyError:
            raise imirror.ConfigError("Hangouts cookie file not specified") from None
        self.client = None
        # Message queue, to move processing from the event stream to the generator.
        self.queue = asyncio.Queue()

    async def connect(self):
        await super().connect()
        self.client = hangups.Client(hangups.get_auth_stdin(self.cookie))
        self.client.on_connect.add_observer(self._connect)
        log.debug("Connecting client")
        asyncio.ensure_future(self.client.connect())
    
    async def _connect(self):
        log.debug("Retrieving users and conversations")
        self.users, self.convs = await hangups.build_user_conversation_list(self.client)
        self.convs.on_event.add_observer(self._event)
        log.debug("Listening for events")

    async def _event(self, event):
        log.debug("Queued new message event")
        await self.queue.put(event)

    async def disconnect(self):
        await super().disconnect()
        if self.client:
            log.debug("Requesting client disconnect")
            await self.client.disconnect()

    async def send(self, channel, msg):
        await super().send(channel, msg)
        conv = self.convs.get(channel.source)
        name = msg.user.real_name or msg.user.username
        if msg.action:
            segments = [hangups.ChatMessageSegment("{} ".format(name), is_bold=True,
                                                   is_italic=True),
                        hangups.ChatMessageSegment(msg.text, is_italic=True)]
        else:
            segments = [hangups.ChatMessageSegment("{}: ".format(name), is_bold=True),
                        hangups.ChatMessageSegment(msg.text)]
        content = [seg.serialize() for seg in segments]
        request = hangouts_pb2.SendChatMessageRequest(
                      request_header=self.client.get_request_header(),
                      event_request_header=conv._get_event_request_header(),
                      message_content=hangouts_pb2.MessageContent(segment=content))
        sent = await self.client.send_chat_message(request)
        return sent.created_event.event_id

    async def receive(self):
        while True:
            event = await self.queue.get()
            log.debug("Retrieved message event")
            yield HangoutsMessage.from_event(self, event)
