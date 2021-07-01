"""
Connect to `Google Hangouts <https://hangouts.google.com>`_ as a regular user.

Requirements:
    Extra name: ``hangouts``

    `hangups <https://hangups.readthedocs.io>`_

Config:
    cookie (str):
        Path to a cookie text file read/written by :func:`hangups.get_auth_stdin`.
    read (bool):
        ``True`` (default) to update conversation watermarks on every message, thus marking each
        message as read once processed.

The cookie file is used by hangups to store the access token.  If the file doesn't exist or is
invalid, you will be prompted for Google account credentials at startup.  You can generate a new
cookie file manually by interacting with hangups directly::

    $ python -m hangups.auth

This will generate a cookie file called *refresh_token.txt* in the current directory.
"""

from asyncio import CancelledError, Condition, ensure_future, gather, sleep
from copy import copy
from io import BytesIO
from itertools import chain
import logging
import re
from urllib.parse import unquote

import hangups
from hangups import hangouts_pb2

import immp


log = logging.getLogger(__name__)


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
        return cls(id_=user.id_.chat_id,
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
        return cls(id_=entity.id_.chat_id,
                   plug=hangouts,
                   real_name=entity.properties.display_name,
                   avatar=avatar,
                   raw=entity)

    @property
    def link(self):
        return "https://hangouts.google.com/chat/person/{}".format(self.id)

    @link.setter
    def link(self, value):
        pass


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
        if segment.type_ == hangouts_pb2.SEGMENT_TYPE_LINE_BREAK:
            return cls("\n")
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
    def _make_segments(cls, segment):
        text = re.sub("^ ", "\N{NBSP}", segment.text).replace("  ", " \N{NBSP}")
        for i, line in enumerate(text.split("\n")):
            if i:
                yield hangups.ChatMessageSegment("\n", hangouts_pb2.SEGMENT_TYPE_LINE_BREAK)
            yield hangups.ChatMessageSegment(line,
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

        Args:
            segment (.Segment)
                Message segment created by another plug.

        Returns:
            hangups.ChatMessageSegment list:
                Unparsed segment objects.
        """
        if segment.link and not segment.text_is_link:
            # The server rejects segments whose link targets don't look like their text, including
            # any free text as well as masquerading of links (e.g. text "a.com" and link "b.org").
            parts = [copy(segment) for _ in range(3)]
            for part, text in zip(parts, ("{} [".format(segment.text), segment.link, "]")):
                part.text = text
            parts[0].link = parts[2].link = None
        else:
            parts = [segment]
        return list(chain(*(cls._make_segments(part) for part in parts)))


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


class HangoutsFile(immp.File):
    """
    File attachment originating from Hangouts.
    """

    def __init__(self, hangouts, title=None, type_=None, source=None):
        super().__init__(title=title, type_=type_)
        self._hangouts = hangouts
        # Private source as the URL is not publicly accessible.
        self._source = source

    async def get_content(self, sess):
        return await self._hangouts.session.get(self._source, allow_redirects=True,
                                                cookies=self._hangouts._client._cookies)

    @classmethod
    async def from_embed(cls, hangouts, embed):
        """
        Convert a :class:`hangouts_pb2.PlusPhoto` into a :class:`.File`.

        Args:
            hangouts (.HangoutsPlug):
                Related plug instance that provides the file.
            embed (hangups.hangouts_pb2.EmbedItem):
                Photo or video inside a Hangups message event.

        Returns:
            .SlackFile:
                Parsed file object.
        """
        source = None
        if embed.plus_photo:
            source = embed.plus_photo.url
            resp = await hangouts.session.head(source, allow_redirects=True,
                                               cookies=hangouts._client._cookies)
            title = str(resp.url).rsplit("/", 1)[-1]
            mime = resp.headers["Content-Type"]
            if mime.startswith("image/"):
                type_ = immp.File.Type.image
            elif mime.startswith("video/"):
                type_ = immp.File.Type.video
            else:
                type_ = immp.File.Type.unknown
        return cls(hangouts, title=title, type_=type_, source=source)


class HangoutsMessage(immp.Message):
    """
    Message originating from Hangouts.
    """

    @classmethod
    async def from_event(cls, hangouts, event):
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
            for attach in event._event.chat_message.message_content.attachment:
                embed = attach.embed_item
                if any(place in embed.type for place in
                       (hangouts_pb2.ITEM_TYPE_PLACE, hangouts_pb2.ITEM_TYPE_PLACE_V2)):
                    location = HangoutsLocation.from_embed(embed)
                    if str(text) == ("https://maps.google.com/maps?q={0},{1}"
                                     .format(location.latitude, location.longitude)):
                        text = None
                    attachments.append(location)
                elif hangouts_pb2.ITEM_TYPE_PLUS_PHOTO in embed.type:
                    attachments.append(await HangoutsFile.from_embed(hangouts, embed))
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
        return immp.SentMessage(id_=event.id_,
                                channel=immp.Channel(hangouts, event.conversation_id),
                                at=event.timestamp,
                                text=text,
                                user=user,
                                action=action,
                                joined=joined,
                                left=left,
                                title=title,
                                attachments=attachments,
                                raw=event)


class HangoutsPlug(immp.Plug, immp.HTTPOpenable):
    """
    Plug for `Google Hangouts <https://hangouts.google.com>`_.
    """

    schema = immp.Schema({"cookie": str,
                          immp.Optional("read", True): bool})

    network_name = "Hangouts"

    @property
    def network_id(self):
        return "hangouts:{}".format(self._bot_user) if self._bot_user else None

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        self._client = self._looped = None
        self._starting = Condition()
        self._closing = False
        self._users = self._convs = self._bot_user = None

    async def _loop(self):
        while True:
            try:
                await self._client.connect()
            except CancelledError:
                log.debug("Cancel request for plug %r loop", self.name)
                return
            except Exception as e:
                log.debug("Unexpected client disconnect: %r", e)
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
            sent = await HangoutsMessage.from_event(self, event)
        except NotImplementedError:
            log.warn("Skipping unimplemented %r event type", event.__class__.__name__)
        else:
            log.debug("Queueing new message event")
            self.queue(sent)
        if self.config["read"]:
            await self._convs.get(event.conversation_id).update_read_timestamp()

    async def start(self):
        await super().start()
        self._closing = False
        self._client = hangups.Client(hangups.get_auth_stdin(self.config["cookie"], True))
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

    async def user_from_id(self, id_):
        user = self._users.get_user(hangups.user.UserID(chat_id=id_, gaia_id=id_))
        if user:
            return HangoutsUser.from_user(self, user)
        request = hangouts_pb2.GetEntityByIdRequest(
            request_header=self._client.get_request_header(),
            batch_lookup_spec=[hangouts_pb2.EntityLookupSpec(gaia_id=id_)])
        response = await self._client.get_entity_by_id(request)
        if response.entity:
            return HangoutsUser.from_entity(self, response.entity)
        else:
            return None

    async def user_is_system(self, user):
        return user.id == self._bot_user

    def _filter_channels(self, type_):
        convs = self._convs.get_all(include_archived=True)
        return (immp.Channel(self, conv.id_) for conv in convs if conv._conversation.type == type_)

    async def public_channels(self):
        return list(self._filter_channels(hangouts_pb2.CONVERSATION_TYPE_GROUP))

    async def private_channels(self):
        return list(self._filter_channels(hangouts_pb2.CONVERSATION_TYPE_ONE_TO_ONE))

    async def channel_for_user(self, user):
        for channel in self._filter_channels(hangouts_pb2.CONVERSATION_TYPE_ONE_TO_ONE):
            if any(part.id == user.id for part in await channel.members()):
                return channel
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

    async def channel_link_create(self, channel, shared=True):
        try:
            conv = self._convs.get(channel.source)
        except KeyError:
            return None
        # Hangouts has no concept of private invite links.
        if not shared:
            return None
        # Enable joining via link for the conversation.
        if not conv.is_group_link_sharing_enabled:
            await conv.set_group_link_sharing_enabled(True)
            log.debug("Enabled join-by-link for %r", channel.source)
        # Request a new invite link (this won't revoke any existing ones).
        request = hangouts_pb2.GetGroupConversationUrlRequest(
            request_header=self._client.get_request_header(),
            conversation_id=hangouts_pb2.ConversationId(id=channel.source))
        response = await self._client.get_group_conversation_url(request)
        return response.group_conversation_url

    async def channel_link_revoke(self, channel, link=None):
        # Hangouts has no concept of revoking links -- any previously issued links will continue
        # to work forever.  Instead, just disable joining via link for the default revocation.
        if link:
            return
        try:
            conv = self._convs.get(channel.source)
        except KeyError:
            return
        if conv.is_group_link_sharing_enabled:
            await conv.set_group_link_sharing_enabled(False)
            log.debug("Disabled join-by-link for %r", channel.source)

    async def _next_batch(self, conv, before_id):
        # Conversation.get_events() should, if the target is the oldest message in the current
        # batch, fetch the next whole batch and return that, or else return everything before the
        # target.  However, at the end of the message history, it sometimes returns an arbitrary
        # batch instead.  Return fetched messages from Conversation.events directly instead.
        ids = [event.id_ for event in conv.events]
        if before_id not in ids:
            return None
        if ids[0] == before_id:
            # Target is the oldest message cached, so there may be more -- try for another batch.
            await conv.get_events(before_id)
            ids = [event.id_ for event in conv.events]
        # Take all events older than the target.
        events = conv.events[:ids.index(before_id)]
        return [await HangoutsMessage.from_event(self, event) for event in events]

    async def channel_history(self, channel, before=None):
        try:
            conv = self._convs.get(channel.source)
        except KeyError:
            return []
        if not conv.events:
            return []
        if not before:
            if len(conv.events) == 1:
                # Only the initial message cached, try to fetch a first batch.
                await conv.get_events(conv.events[0].id_)
            # Return all cached events.
            return [await HangoutsMessage.from_event(self, event) for event in conv.events]
        ids = [event.id_ for event in conv.events]
        if before.id in ids:
            return await self._next_batch(conv, before.id)
        # Hangouts has no way to query for an event by ID, only by timestamp.  Instead, we'll try a
        # few times to retrieve it further down the message history.
        for i in range(10):
            log.debug("Fetching batch %i of events to find %r", i + 1, before.id)
            events = await conv.get_events(conv.events[0].id_)
            ids = [event.id_ for event in events]
            if not ids:
                # No further messages, we've hit the end of the message history.
                return []
            elif before.id in ids:
                return await self._next_batch(conv, before.id)
        # Maxed out on attempts but didn't find the requested message.
        return []

    async def _get_event(self, receipt):
        try:
            conv = self._convs.get(receipt.channel.source)
        except KeyError:
            return None
        ids = [event.id_ for event in conv.events]
        try:
            return conv.get_event(receipt.id)
        except KeyError:
            pass
        # Hangouts has no way to query for an event by ID, only by timestamp.  Instead, we'll try a
        # few times to retrieve it further down the message history.
        for i in range(10):
            log.debug("Fetching batch %i of events to find %r", i + 1, receipt.id)
            events = await conv.get_events(conv.events[0].id_)
            ids = [event.id_ for event in events]
            if not ids:
                # No further messages, we've hit the end of the message history.
                return []
            elif receipt.id in ids:
                return events[ids.index(receipt.id)]
        # Maxed out on attempts but didn't find the requested message.
        return None

    async def get_message(self, receipt):
        # We have the message reference but not the content.
        event = await self._get_event(receipt)
        if not event:
            return None
        sent = await HangoutsMessage.from_event(self, event)
        # As we only use this for rendering the message again, we shouldn't add a second
        # layer of authorship if we originally sent the message being retrieved.
        if sent.user.id == self._bot_user:
            sent.user = None
        return sent

    async def _upload(self, attach):
        async with (await attach.get_content(self.session)) as img_content:
            # Hangups expects a file-like object with a synchronous read() method.
            # NB. The whole file is read into memory by Hangups anyway.
            # Filename must be present, else Hangups will try (and fail) to read the path.
            photo = await self._client.upload_image(BytesIO(await img_content.read()),
                                                    filename=attach.title or "image.png")
        return hangouts_pb2.ExistingMedia(photo=hangouts_pb2.Photo(photo_id=photo))

    @classmethod
    def _serialise(cls, rich):
        output = []
        for chunk in rich.chunked(4096):
            line = []
            for segment in chunk:
                line += HangoutsSegment.to_segments(segment)
            output.append([segment.serialize() for segment in line])
        return output

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
        places = []
        for attach in msg.attachments:
            if isinstance(attach, immp.File) and attach.type in (immp.File.Type.image,
                                                                 immp.File.Type.video):
                uploads.append(self._upload(attach))
            elif isinstance(attach, immp.Location):
                places.append(HangoutsLocation.to_place(attach))
        if uploads:
            images = await gather(*uploads)
        requests = []
        if msg.text or msg.reply_to:
            render = msg.render(link_name=False, edit=msg.edited, quote_reply=True)
            parts = self._serialise(render)
            media = None
            if len(images) == 1 and len(parts) == 1:
                # Attach the only image to the message text.
                media = images.pop()
            for segments in parts:
                requests.append(self._request(conv, segments, media))
        if images:
            segments = []
            if msg.user:
                label = immp.Message(user=msg.user, text="sent an image", action=True)
                segments = self._serialise(label.render(link_name=False))[0]
            # Send any additional media items in their own separate messages.
            for media in images:
                requests.append(self._request(conv, segments, media))
        if places:
            # Send each location separately.
            for place in places:
                requests.append(self._request(conv, place=place))
            # Include a label only if we haven't sent a text message earlier.
            if msg.user and not msg.text:
                label = immp.Message(user=msg.user, text="sent a location", action=True)
                segments = self._serialise(label.render(link_name=False))[0]
                requests.append(self._request(conv, segments))
        return requests

    async def put(self, channel, msg):
        conv = self._convs.get(channel.source)
        # Attempt to find sources for referenced messages.
        clone = copy(msg)
        clone.reply_to = await self.resolve_message(clone.reply_to)
        requests = []
        for attach in clone.attachments:
            # Generate requests for attached messages first.
            if isinstance(attach, immp.Message):
                requests += await self._requests(conv, await self.resolve_message(attach))
        own_requests = await self._requests(conv, clone)
        if requests and not own_requests:
            # Forwarding a message but no content to show who forwarded it.
            info = immp.Message(user=clone.user, action=True, text="forwarded a message")
            own_requests = await self._requests(conv, info)
        requests += own_requests
        receipts = []
        for request in requests:
            response = await self._client.send_chat_message(request)
            event = hangups.conversation.Conversation._wrap_event(response.created_event)
            receipts.append(await HangoutsMessage.from_event(self, event))
        return receipts
