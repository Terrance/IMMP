"""
Connect to `Google Hangouts <https://hangouts.google.com>`_ as a regular user.

Config:
    cookie (str):
        Path to a cookie text file read/written by :func:`hangups.get_auth_stdin`.

The cookie file is used by hangups to store the access token.  If the file doesn't exist or is
invalid, you will be prompted for Google account credentials at startup.  You can generate a new
cookie file manually by interacting with hangups directly::

    $ python -m hangups.auth

This will generate a cookie file called *refresh_token.txt* in the current directory.

.. note::
    This plug requires the `hangups <https://hangups.readthedocs.io>`_ Python module.
"""

from asyncio import Condition, ensure_future
from copy import copy
from io import BytesIO
import logging
import re
from urllib.parse import unquote

import hangups
from hangups import hangouts_pb2
from voluptuous import ALLOW_EXTRA, Schema

import immp


log = logging.getLogger(__name__)


class _Schema(object):

    config = Schema({"cookie": str}, extra=ALLOW_EXTRA, required=True)


class HangoutsUser(immp.User):
    """
    User present in Hangouts.
    """

    @classmethod
    def from_user(cls, hangouts, user):
        """
        Convert a :class:`hangups.user.User` into a :class:`.User`.

        Args:
            hangouts (.HangoutsPlug):
                Related plug instance that provides the user.
            user (hangups.user.User):
                Hangups user object retrieved from the user list.

        Returns:
            .HangoutsUser:
                Parsed user object.
        """
        # No usernames here, just the ID.
        avatar = re.sub("^//", "https://", user.photo_url) if user.photo_url else None
        return cls(id=user.id_.chat_id,
                   plug=hangouts,
                   real_name=user.full_name,
                   avatar=avatar,
                   raw=user)

    @property
    def link(self):
        if self.id:
            return "https://hangouts.google.com/chat/person/{}".format(self.id)


class HangoutsSegment(immp.Segment):
    """
    Plug-friendly representation of Hangouts message formatting.
    """

    @classmethod
    def from_segment(cls, segment):
        """
        Convert a :class:`hangups.ChatMessageSegment` into a :class:`.Segment`.

        Args:
            segment (hangups.ChatMessageSegment):
                Hangups message segment from the conversation event.

        Returns:
            .HangoutsSegment:
                Parsed segment object.
        """
        link = segment.link_target
        if segment.link_target:
            match = re.match(r"https://www.google.com/url\?(?:[^&]+&)*q=([^&]+)",
                             segment.link_target)
            if match:
                # Strip Google link redirect.
                link = unquote(match[1])
        return cls(segment.text,
                   bold=segment.is_bold,
                   italic=segment.is_italic,
                   underline=segment.is_underline,
                   strike=segment.is_strikethrough,
                   link=link)

    @classmethod
    def _hangups_segments(cls, text, segment):
        if segment.link and not text == segment.link:
            # Hangouts won't render links unless the text matches.
            # Fall back to multiple segments showing the text and the link separately.
            clone = copy(segment)
            clone.link = None
            return (cls._hangups_segments("{} [".format(text), clone) +
                    cls._hangups_segments(segment.link, segment) +
                    cls._hangups_segments("]", clone))
        else:
            return [hangups.ChatMessageSegment(text,
                                               is_bold=segment.bold,
                                               is_italic=segment.italic,
                                               is_underline=segment.underline,
                                               is_strikethrough=segment.strike,
                                               link_target=segment.link)]

    @classmethod
    def to_segments(cls, segment):
        """
        Convert a :class:`.Segment` into one or more :class:`hangups.ChatMessageSegment` instances.

        Args:
            segment (.Segment)
                Message segment created by another plug.

        Returns:
            hangups.ChatMessageSegment list:
                Unparsed segment objects.
        """
        parts = segment.text.split("\n")
        segments = cls._hangups_segments(parts[0], segment)
        for part in parts[1:]:
            segments.append(hangups.ChatMessageSegment("\n", hangouts_pb2.SEGMENT_TYPE_LINE_BREAK))
            segments += cls._hangups_segments(part, segment)
        return [segment for segment in segments if segment.text]


class HangoutsMessage(immp.Message):
    """
    Message originating from Hangouts.
    """

    @classmethod
    def from_event(cls, hangouts, event):
        """
        Convert a :class:`hangups.ChatMessageEvent` into a :class:`.Message`.

        Args:
            hangouts (.HangoutsPlug):
                Related plug instance that provides the event.
            event (hangups.ChatMessageEvent):
                Hangups message event emitted from a conversation.

        Returns:
            .HangoutsMessage:
                Parsed message object.
        """
        user = HangoutsUser.from_user(hangouts, hangouts._users.get_user(event.user_id))
        action = False
        joined = None
        left = None
        attachments = []
        if isinstance(event, hangups.ChatMessageEvent):
            segments = [HangoutsSegment.from_segment(segment) for segment in event.segments]
            text = immp.RichText(segments)
            if any(a.type == 4 for a in event._event.chat_message.annotation):
                # This is a /me message sent from desktop Hangouts.
                action = True
                # The user's first name prefixes the message text, so try to strip that.
                if user.real_name:
                    # We don't have a clear-cut first name, so try to match parts of names.
                    # Try the full name first, then split successive words off the end.
                    parts = user.real_name.split()
                    start = text[0].text
                    for pos in range(len(parts), 0, -1):
                        sub_name = " ".join(parts[:pos])
                        if start.startswith(sub_name):
                            text[0].text = start[len(sub_name) + 1:]
                            break
                    else:
                        # Couldn't match the user's name to the message text.
                        pass
            for attach in event.attachments:
                attachments.append(immp.File(type=immp.File.Type.image, source=attach))
        elif isinstance(event, hangups.MembershipChangeEvent):
            action = True
            is_join = event.type_ == hangups.hangouts_pb2.MEMBERSHIP_CHANGE_TYPE_JOIN
            parts = [HangoutsUser.from_user(hangouts, hangouts._users.get_user(part_id))
                     for part_id in event.participant_ids]
            if len(parts) == 1 and parts[0].id == user.id:
                # Membership event is a user acting on themselves.
                segments = [HangoutsSegment("{} the hangout"
                                            .format("joined" if is_join else "left"))]
            else:
                segments = [HangoutsSegment("added " if is_join else "removed ")]
                for part in parts:
                    link = "https://hangouts.google.com/chat/person/{}".format(part.id)
                    segments.append(HangoutsSegment(part.real_name, bold=True, link=link))
                    segments.append(HangoutsSegment(", "))
                # Replace trailing comma.
                segments[-1].text = " {} the hangout".format("to" if is_join else "from")
            if is_join:
                joined = parts
            else:
                left = parts
        elif isinstance(event, hangups.OTREvent):
            action = True
            is_history = (event.new_otr_status ==
                          hangups.hangouts_pb2.OFF_THE_RECORD_STATUS_ON_THE_RECORD)
            segments = [HangoutsSegment("{}abled hangout message history"
                                        .format("en" if is_history else "dis"))]
        elif isinstance(event, hangups.RenameEvent):
            action = True
            segments = [HangoutsSegment("renamed the hangout to "),
                        HangoutsSegment(event.new_name, bold=True)]
        elif isinstance(event, hangups.GroupLinkSharingModificationEvent):
            action = True
            is_shared = event.new_status == hangups.hangouts_pb2.GROUP_LINK_SHARING_STATUS_ON
            segments = [HangoutsSegment("{}abled joining the hangout by link"
                                        .format("en" if is_shared else "dis"))]
        else:
            raise NotImplementedError
        text = immp.RichText(segments)
        return (hangouts.host.resolve_channel(hangouts, event.conversation_id),
                cls(id=event.id_,
                    text=text,
                    user=user,
                    action=action,
                    joined=joined,
                    left=left,
                    attachments=attachments,
                    raw=event))


class HangoutsPlug(immp.Plug):
    """
    Plug for `Google Hangouts <https://hangouts.google.com>`_.
    """

    class Meta(immp.Plug.Meta):
        network = "Hangouts"

    def __init__(self, name, config, host):
        super().__init__(name, _Schema.config(config), host)
        self._client = None
        self._starting = Condition()

    async def start(self):
        await super().start()
        self._client = hangups.Client(hangups.get_auth_stdin(self.config["cookie"]))
        self._client.on_connect.add_observer(self._connect)
        log.debug("Connecting client")
        ensure_future(self._client.connect())
        with await self._starting:
            # Block until users and conversations are loaded.
            await self._starting.wait()
        log.debug("Listening for events")

    async def _connect(self):
        log.debug("Retrieving users and conversations")
        self._users, self._convs = await hangups.build_user_conversation_list(self._client)
        self._convs.on_event.add_observer(self._event)
        with await self._starting:
            self._starting.notify_all()

    async def _event(self, event):
        try:
            channel, msg = HangoutsMessage.from_event(self, event)
        except NotImplementedError:
            log.warn("Skipping unimplemented message event")
        else:
            log.debug("Queueing new message event")
            self.queue(channel, msg)

    async def stop(self):
        await super().stop()
        if self._client:
            log.debug("Requesting client disconnect")
            await self._client.disconnect()

    async def user_from_id(self, id):
        user = self._users.get_user(hangups.user.UserID(chat_id=id, gaia_id=id))
        return HangoutsUser.from_user(self, user) if user else None

    async def private_channel(self, user):
        if not isinstance(user, HangoutsUser) or not isinstance(user.raw, hangups.user.User):
            return None
        for conv in self._convs.get_all(include_archived=True):
            if conv._conversation.type == hangouts_pb2.CONVERSATION_TYPE_ONE_TO_ONE:
                if any(part.id_.chat_id == user.id for part in conv.users):
                    return immp.Channel(None, self, conv.id_)
        # TODO: Create conversation.
        return None

    async def channel_members(self, channel):
        if channel.plug is not self:
            return None
        try:
            conv = self._convs.get(channel.source)
        except KeyError:
            return []
        else:
            return [HangoutsUser.from_user(self, user) for user in conv.users]

    async def channel_invite(self, channel, user):
        try:
            conv = self._convs.get(channel.source)
        except KeyError:
            return
        request = hangouts_pb2.AddUserRequest(
            request_header=self._client.get_request_header(),
            event_request_header=conv._get_event_request_header(),
            invitee_id=[hangouts_pb2.InviteeID(gaia_id=user.id)])
        await self._client.add_user(request)

    async def channel_remove(self, channel, user):
        try:
            conv = self._convs.get(channel.source)
        except KeyError:
            return
        request = hangouts_pb2.RemoveUserRequest(
            request_header=self._client.get_request_header(),
            event_request_header=conv._get_event_request_header(),
            participant_id=hangouts_pb2.ParticipantId(gaia_id=user.id))
        await self._client.remove_user(request)

    async def put(self, channel, msg):
        if msg.deleted:
            # We can't delete messages on this side.
            return []
        conv = self._convs.get(channel.source)
        media = None
        for attach in msg.attachments:
            if isinstance(attach, immp.File) and attach.type == immp.File.Type.image:
                # Upload an image file to Hangouts.
                async with (await attach.get_content()) as img_content:
                    # Hangups expects a file-like object with a synchronous read() method.
                    # NB. The whole files is read into memory by Hangups anyway.
                    # Filename must be present, else Hangups will try (and fail) to read the path.
                    photo = await self._client.upload_image(BytesIO(await img_content.read()),
                                                            filename=attach.title or "image.png")
                media = hangouts_pb2.ExistingMedia(photo=hangouts_pb2.Photo(photo_id=photo))
                # TODO: Handle more than one image attachment.
                break
        segments = []
        for segment in msg.render(quote_reply=True):
            segments += HangoutsSegment.to_segments(segment)
        content = [segment.serialize() for segment in segments]
        request = hangouts_pb2.SendChatMessageRequest(
            request_header=self._client.get_request_header(),
            event_request_header=conv._get_event_request_header(),
            message_content=hangouts_pb2.MessageContent(segment=content),
            existing_media=media)
        sent = await self._client.send_chat_message(request)
        return [sent.created_event.event_id]
