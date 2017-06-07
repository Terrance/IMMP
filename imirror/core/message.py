from copy import deepcopy
from datetime import datetime

from .util import Base


class User(Base):
    """
    Generic class to represent senders of messages.

    Attributes:
        id (str):
            Transport-specific user identifier.
        username (str):
            User's chosen or allocated display name.
        real_name (str):
            User's preferred and/or family name.
        avatar (str):
            URL of the user's profile picture.
        raw:
            Optional transport-specific underlying user object.
    """

    def __init__(self, id, username=None, real_name=None, avatar=None, raw=None):
        self.id = id
        self.username = username
        self.real_name = real_name
        self.avatar = avatar
        self.raw = raw


class RichText(list, Base):
    """
    Common standard for formatted message text, akin to Hangouts' message segments.

    This is a specialised subclass of :class:`list`, designed to hold instances of
    :class:`.RichText.Segment`.
    """

    class Segment(Base):
        """
        Substring of message text with consistent formatting.

        Attributes:
            text (str):
                Plain segment text.
            bold (bool):
                Whether this segment should be formatted bold.
            italic (bool):
                Whether this segment should be emphasised.
            underline (bool):
                Whether this segment should be underlined.
            strike (bool):
                Whether this segment should be struck through.
            code (bool):
                Whether this segment should be monospaced.
            pre (bool):
                Whether this segment should be preformatted.
            link (str):
                Anchor URL if this segment represents a clickable link.
        """

        def __init__(self, text, bold=False, italic=False, underline=False, strike=False,
                     code=False, pre=False, link=None):
            self.text = text
            self.bold = bold
            self.italic = italic
            self.underline = underline
            self.strike = strike
            self.code = code
            self.pre = pre
            self.link = link

        def __str__(self):
            # Fallback implementation: just return the segment text without formatting.
            return self.text

    def clone(self):
        """
        Make a copy of this message text and all its segments.

        Returns:
            .RichText:
                Cloned message text instance.
        """
        return deepcopy(self)

    def __str__(self):
        # Fallback implementation: just return the message text without formatting.
        return "".join(str(segment) for segment in self)

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, super().__repr__())


class Message(Base):
    """
    Base message object, understood by all transports.

    Attributes:
        id (str):
            Unique (to the transport) message identifier.
        channel (.Channel):
            Original source of this message.
        at (datetime.datetime):
            Timestamp of the message according to the external server.
        original (str):
            ID of the original message, which this message is an update of.
        text (str):
            Plain text representation of the message.
        user (.User):
            User profile that sent the message.
        action (bool):
            Whether this message should be presented as an action involving its user.
        deleted (bool):
            Whether the message was deleted from its source.
        reply_to (str):
            ID of the parent message, which this message replies to.
        joined (.User list):
            Collection of users that just joined the channel.
        left (.User list):
            Collection of users that just parted the channel.
        raw:
            Optional transport-specific underlying message or event object.
    """

    def __init__(self, id, channel, at=None, original=None, text=None, user=None, action=False,
                 deleted=False, reply_to=None, joined=None, left=None, raw=None):
        """
        Populate the new message.

        Args:
            id (str)
            channel (.Channel)
            at (datetime.datetime)
            original (str)
            text (str)
            user (.User)
            action (bool)
            deleted (bool)
            reply_to (str)
            raw
        """
        self.channel = channel
        self.id = id
        self.at = at or datetime.now()
        self.original = original
        self.text = text
        self.user = user
        self.action = action
        self.deleted = deleted
        self.reply_to = reply_to
        self.joined = joined
        self.left = left
        self.raw = raw
