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

from asyncio import CancelledError, Condition, ensure_future, gather, sleep
from copy import copy
from datetime import datetime
from io import BytesIO
import logging
import re
from urllib.parse import unquote

import hangups
from hangups import hangouts_pb2
from voluptuous import ALLOW_EXTRA, Schema

import immp


log = logging.getLogger(__name__)


class _Schema:

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

    @classmethod
    def from_entity(cls, hangouts, entity):
        """
        Convert a :class:`hangups.hangouts_pb2.Entity` into a :class:`.User`.

        Args:
            hangouts (.HangoutsPlug):
                Related plug instance that provides the user.
            user (hangups.hangouts_pb2.Entity):
                Hangouts entity response retrieved from a
                :class:`hangups.hangouts_pb2.GetEntityByIdRequest`.

        Returns:
            .HangoutsUser:
                Parsed user object.
        """
        avatar = (re.sub("^//", "https://", entity.properties.photo_url)
                  if entity.properties.photo_url else None)
        return cls(id=entity.id_.chat_id,
                   plug=hangouts,
                   real_name=entity.properties.display_name,
                   avatar=avatar,
                   raw=entity)

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
    def _make_segment(cls, text, segment):
        return hangups.ChatMessageSegment(text,
                                          (hangouts_pb2.SEGMENT_TYPE_LINK
                                           if segment.link else
                                           hangouts_pb2.SEGMENT_TYPE_TEXT),
                                          is_bold=segment.bold,
                                          is_italic=segment.italic,
                                          is_underline=segment.underline,
                                          is_strikethrough=segment.strike,
                                          link_target=segment.link)

    @classmethod
    def to_segments(cls, segment):
        """
        Convert a :class:`.Segment` into one or more :class:`hangups.ChatMessageSegment` instances.
        This will also attempt to create missing link segments for plain URLs within the text.

        Args:
            segment (.Segment)
                Message segment created by another plug.

        Returns:
            hangups.ChatMessageSegment list:
                Unparsed segment objects.
        """
        unlinked = copy(segment)
        unlinked.link = None
        if segment.link:
            if segment.text == segment.link:
                return [cls._make_segment(segment.text, segment)]
            else:
                return [cls._make_segment("{} [".format(segment.text), unlinked),
                        cls._make_segment(segment.link, segment),
                        cls._make_segment("]", unlinked)]
        segments = []
        for i, line in enumerate(segment.text.split("\n")):
            if i:
                segments.append(hangups.ChatMessageSegment(
                    "\n", hangouts_pb2.SEGMENT_TYPE_LINE_BREAK))
            while True:
                match = re.search(r"\b([a-z-]+://\S+)\b", line)
                if not match:
                    break
                left, right = match.span()
                linked = copy(segment)
                linked.link = line[left:right]
                segments += [cls._make_segment(line[:left], segment),
                             cls._make_segment(linked.link, linked)]
                line = line[right:]
            segments.append(cls._make_segment(line, unlinked))
        return [segment for segment in segments if segment.text]


class HangoutsLocation(immp.Location):

    @classmethod
    def from_embed(cls, embed):
        """
        Convert a :class:`hangouts_pb2.EmbedItem` into a :class:`.Location`.

        Args:
            embed (hangups.hangouts_pb2.EmbedItem):
                Location inside a Hangups message event.

        Returns:
            .HangoutsLocation:
                Parsed location object.
        """
        latitude = longitude = name = address = None
        if embed.place:
            # Chains of elements may be defined but empty at the end.
            if all((embed.place.geo,
                    embed.place.geo.geo_coordinates,
                    embed.place.geo.geo_coordinates.latitude,
                    embed.place.geo.geo_coordinates.longitude)):
                latitude = embed.place.geo.geo_coordinates.latitude
                longitude = embed.place.geo.geo_coordinates.longitude
            if embed.place.name:
                name = embed.place.name
            if all((embed.place.address,
                    embed.place.address.postal_address,
                    embed.place.address.postal_address.street_address)):
                address = embed.place.address.postal_address.street_address
        return cls(latitude=latitude,
                   longitude=longitude,
                   name=name,
                   address=address)

    @classmethod
    def to_place(cls, location):
        """
        Convert a :class:`.Location` into a :class:`hangouts_pb2.EmbedItem`.

        Args:
            location (.Location):
                Location created by another plug.

        Returns:
            hangups.hangouts_pb2.EmbedItem:
                Formatted embed item, suitable for sending inside a
                :class:`hangups.hangouts_pb2.Place` object.
        """
        address = hangouts_pb2.EmbedItem.PostalAddress(street_address=location.address)
        geo = hangouts_pb2.EmbedItem.GeoCoordinates(latitude=location.latitude,
                                                    longitude=location.longitude)
        image_url = ("https://maps.googleapis.com/maps/api/staticmap?"
                     "center={0},{1}&markers=color:red%7C{0},{1}&size=400x400"
                     .format(location.latitude, location.longitude))
        return hangouts_pb2.EmbedItem(
                type=[hangouts_pb2.ITEM_TYPE_PLACE_V2,
                      hangouts_pb2.ITEM_TYPE_PLACE,
                      hangouts_pb2.ITEM_TYPE_THING],
                id="and0",
                place=hangouts_pb2.Place(
                    name=location.name,
                    address=hangouts_pb2.EmbedItem(postal_address=address),
                    geo=hangouts_pb2.EmbedItem(geo_coordinates=geo),
                    representative_image=hangouts_pb2.EmbedItem(
                        type=[hangouts_pb2.ITEM_TYPE_PLACE,
                              hangouts_pb2.ITEM_TYPE_THING],
                        id=image_url,
                        image=hangouts_pb2.EmbedItem.Image(url=image_url)))).place


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
        version = event._event.event_version
        user = HangoutsUser.from_user(hangouts, hangouts._users.get_user(event.user_id))
        action = False
        joined = None
        left = None
        title = None
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
            for attach in event._event.chat_message.message_content.attachment:
                embed = attach.embed_item
                if any(place in embed.type for place in
                       (hangouts_pb2.ITEM_TYPE_PLACE, hangouts_pb2.ITEM_TYPE_PLACE_V2)):
                    location = HangoutsLocation.from_embed(embed)
                    if str(text) == ("https://maps.google.com/maps?q={0},{1}"
                                     .format(location.latitude, location.longitude)):
                        text = None
                    attachments.append(location)
        elif isinstance(event, hangups.MembershipChangeEvent):
            action = True
            is_join = event.type_ == hangouts_pb2.MEMBERSHIP_CHANGE_TYPE_JOIN
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
                          hangouts_pb2.OFF_THE_RECORD_STATUS_ON_THE_RECORD)
            segments = [HangoutsSegment("{}abled hangout message history"
                                        .format("en" if is_history else "dis"))]
        elif isinstance(event, hangups.RenameEvent):
            action = True
            title = event.new_name
            segments = [HangoutsSegment("renamed the hangout to "),
                        HangoutsSegment(event.new_name, bold=True)]
        elif isinstance(event, hangups.GroupLinkSharingModificationEvent):
            action = True
            is_shared = event.new_status == hangouts_pb2.GROUP_LINK_SHARING_STATUS_ON
            segments = [HangoutsSegment("{}abled joining the hangout by link"
                                        .format("en" if is_shared else "dis"))]
        elif isinstance(event, hangups.HangoutEvent):
            action = True
            texts = {hangouts_pb2.HANGOUT_EVENT_TYPE_START: "started a call",
                     hangouts_pb2.HANGOUT_EVENT_TYPE_END: "ended the call",
                     hangouts_pb2.HANGOUT_EVENT_TYPE_JOIN: "joined the call",
                     hangouts_pb2.HANGOUT_EVENT_TYPE_LEAVE: "left the call"}
            try:
                segments = [HangoutsSegment(texts[event.event_type])]
            except KeyError:
                raise NotImplementedError
        else:
            raise NotImplementedError
        if not isinstance(event, hangups.ChatMessageEvent):
            text = immp.RichText(segments)
        return immp.SentMessage(id=event.id_,
                                revision=version,
                                at=datetime.fromtimestamp(version / 1000000),
                                channel=immp.Channel(hangouts, event.conversation_id),
                                text=text,
                                user=user,
                                action=action,
                                joined=joined,
                                left=left,
                                title=title,
                                attachments=attachments,
                                raw=event)


class HangoutsPlug(immp.Plug):
    """
    Plug for `Google Hangouts <https://hangouts.google.com>`_.
    """

    network_name = "Hangouts"

    @property
    def network_id(self):
        return "hangouts:{}".format(self._bot_user) if self._bot_user else None

    def __init__(self, name, config, host):
        super().__init__(name, _Schema.config(config), host)
        self._client = None
        self._looped = None
        self._starting = Condition()
        self._closing = False
        self._bot_user = None

    async def _loop(self):
        while True:
            try:
                await self._client.connect()
            except CancelledError:
                log.debug("Cancel request for plug '{}' loop".format(self.name))
                return
            except Exception as e:
                log.debug("Unexpected client disconnect: {}".format(e))
            if self._closing:
                return
            log.debug("Reconnecting in 3 seconds")
            await sleep(3)

    async def _connect(self):
        log.debug("Retrieving users and conversations")
        self._users, self._convs = await hangups.build_user_conversation_list(self._client)
        self._convs.on_event.add_observer(self._event)
        resp = await self._client.get_self_info(hangouts_pb2.GetSelfInfoRequest(
            request_header=self._client.get_request_header()))
        self._bot_user = resp.self_entity.id.chat_id
        async with self._starting:
            self._starting.notify_all()

    async def _event(self, event):
        try:
            sent = HangoutsMessage.from_event(self, event)
        except NotImplementedError:
            log.warn("Skipping unimplemented '{}' event type".format(event.__class__.__name__))
        else:
            log.debug("Queueing new message event")
            self.queue(sent)
        await self._convs.get(event.conversation_id).update_read_timestamp()

    async def start(self):
        await super().start()
        self._closing = False
        self._client = hangups.Client(hangups.get_auth_stdin(self.config["cookie"]))
        self._client.on_connect.add_observer(self._connect)
        log.debug("Connecting client")
        self._looped = ensure_future(self._loop())
        async with self._starting:
            # Block until users and conversations are loaded.
            await self._starting.wait()
        log.debug("Listening for events")

    async def stop(self):
        await super().stop()
        self._closing = True
        if self._client:
            log.debug("Requesting client disconnect")
            await self._client.disconnect()
            self._client = None
        if self._looped:
            self._looped.cancel()
            self._looped = None
        self._bot_user = None

    async def user_from_id(self, id):
        user = self._users.get_user(hangups.user.UserID(chat_id=id, gaia_id=id))
        if user:
            return HangoutsUser.from_user(self, user)
        request = hangouts_pb2.GetEntityByIdRequest(
            request_header=self._client.get_request_header(),
            batch_lookup_spec=[hangouts_pb2.EntityLookupSpec(gaia_id=id)])
        response = await self._client.get_entity_by_id(request)
        if response.entity:
            return HangoutsUser.from_entity(self, response.entity)
        else:
            return None

    async def channel_for_user(self, user):
        if not isinstance(user, HangoutsUser) or not isinstance(user.raw, hangups.user.User):
            return None
        for conv in self._convs.get_all(include_archived=True):
            if conv._conversation.type == hangouts_pb2.CONVERSATION_TYPE_ONE_TO_ONE:
                if any(part.id_.chat_id == user.id for part in conv.users):
                    return immp.Channel(self, conv.id_)
        request = hangouts_pb2.CreateConversationRequest(
            request_header=self._client.get_request_header(),
            type=hangouts_pb2.CONVERSATION_TYPE_ONE_TO_ONE,
            client_generated_id=self._client.get_client_generated_id(),
            invitee_id=[hangouts_pb2.InviteeID(gaia_id=user.id)])
        response = await self._client.create_conversation(request)
        return immp.Channel(self, response.conversation.conversation_id.id)

    async def channel_is_private(self, channel):
        try:
            conv = self._convs.get(channel.source)
        except KeyError:
            return False
        else:
            return conv._conversation.type == hangouts_pb2.CONVERSATION_TYPE_ONE_TO_ONE

    async def channel_title(self, channel):
        try:
            return self._convs.get(channel.source).name
        except KeyError:
            return None

    async def channel_link(self, channel):
        return "https://hangouts.google.com/chat/{}".format(channel.source)

    async def channel_rename(self, channel, title):
        try:
            conv = self._convs.get(channel.source)
        except KeyError:
            return None
        else:
            if not conv.name == title:
                await conv.rename(title)

    async def channel_members(self, channel):
        try:
            conv = self._convs.get(channel.source)
        except KeyError:
            return None
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

    async def _get_event(self, conv, event_id):
        # Check first if we've already got the event cached.
        for event in conv.events:
            if event.id_ == event_id:
                return event
        # Hangouts has no way to query for an event by ID, only by timestamp.  Instead, we'll fetch
        # the last X (in this case 500) events and hope to find it.
        for event in await conv.get_events(conv.events[0].id_, 500):
            if event.id_ == event_id:
                return event
        return None

    async def _resolve_msg(self, conv, msg):
        if isinstance(msg, immp.SentMessage):
            event = await self._get_event(conv, msg.id)
            if event:
                return HangoutsMessage.from_event(self, event)
            elif not msg.empty:
                return msg
        elif isinstance(msg, immp.Message):
            return msg
        return None

    async def _upload(self, attach):
        async with (await attach.get_content()) as img_content:
            # Hangups expects a file-like object with a synchronous read() method.
            # NB. The whole file is read into memory by Hangups anyway.
            # Filename must be present, else Hangups will try (and fail) to read the path.
            photo = await self._client.upload_image(BytesIO(await img_content.read()),
                                                    filename=attach.title or "image.png")
        return hangouts_pb2.ExistingMedia(photo=hangouts_pb2.Photo(photo_id=photo))

    @classmethod
    def _serialise(cls, segments):
        output = []
        for segment in segments:
            output += HangoutsSegment.to_segments(segment)
        return [segment.serialize() for segment in output]

    def _request(self, conv, segments=None, media=None, place=None):
        return hangouts_pb2.SendChatMessageRequest(
            request_header=self._client.get_request_header(),
            event_request_header=conv._get_event_request_header(),
            message_content=hangouts_pb2.MessageContent(segment=segments) if segments else None,
            existing_media=media,
            location=hangouts_pb2.Location(place=place) if place else None)

    async def _requests(self, conv, msg):
        uploads = []
        images = []
        locations = []
        for attach in msg.attachments:
            if isinstance(attach, immp.File) and attach.type == immp.File.Type.image:
                uploads.append(self._upload(attach))
            elif isinstance(attach, immp.Location):
                locations.append((attach, HangoutsLocation.to_place(attach)))
        if uploads:
            images = await gather(*uploads)
        requests = []
        if msg.text or msg.reply_to:
            edited = msg.edited if isinstance(msg, immp.SentMessage) else False
            segments = self._serialise(msg.render(link_name=False, edit=edited, quote_reply=True))
            media = None
            if len(images) == 1:
                # Attach the only image to the message text.
                media = images.pop()
            requests.append(self._request(conv, segments, media))
        if images:
            segments = []
            if msg.user:
                label = immp.Message(user=msg.user, text="sent an image", action=True)
                segments = self._serialise(label.render(link_name=False))
            # Send any additional media items in their own separate messages.
            for media in images:
                requests.append(self._request(conv, segments, media))
        if locations:
            # Send each location separately.
            for location, place in locations:
                requests.append(self._request(conv, place=place))
            # Include a label only if we haven't sent a text message earlier.
            if msg.user and not msg.text:
                label = immp.Message(user=msg.user, text="sent a location", action=True)
                segments = self._serialise(label.render(link_name=False))
                requests.append(self._request(conv, segments))
        return requests

    async def put(self, channel, msg):
        conv = self._convs.get(channel.source)
        # Attempt to find sources for referenced messages.
        clone = copy(msg)
        clone.reply_to = await self._resolve_msg(conv, clone.reply_to)
        for i, attach in enumerate(clone.attachments):
            if isinstance(attach, immp.Message):
                clone.attachments[i] = await self._resolve_msg(conv, attach.reply_to)
        requests = []
        for i, attach in enumerate(clone.attachments):
            # Generate requests for attached messages first.
            if isinstance(attach, immp.SentMessage):
                event = await self._get_event(conv, attach.id)
                if event:
                    requests += await self._requests(conv, HangoutsMessage.from_event(self, event))
                elif not attach.empty:
                    requests += await self._requests(conv, attach)
            elif isinstance(attach, immp.Message):
                requests += await self._requests(conv, attach)
        own_requests = await self._requests(conv, clone)
        if requests and not own_requests:
            # Forwarding a message but no content to show who forwarded it.
            info = immp.Message(user=clone.user, action=True, text="forwarded a message")
            own_requests = await self._requests(conv, info)
        requests += own_requests
        events = []
        for request in requests:
            events.append(await self._client.send_chat_message(request))
        return [event.created_event.event_id for event in events]
