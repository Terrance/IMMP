from copy import copy
from datetime import datetime
from enum import Enum
import re

import aiohttp

from .util import pretty_str


@pretty_str
class User:
    """
    Generic class to represent senders of messages.

    Attributes:
        id (str):
            Plug-specific user identifier.
        plug (.Plug):
            Source plug instance, representing the domain this user comes from.
        username (str):
            User's chosen or allocated display name.
        real_name (str):
            User's preferred and/or family name.
        avatar (str):
            URL of the user's profile picture.
        link (str):
            Public profile URL, or identifier used for invites.
        raw:
            Optional plug-specific underlying user object.
    """

    def __init__(self, *, id=None, plug=None, username=None, real_name=None, avatar=None,
                 link=None, raw=None):
        self.id = id
        self.plug = plug
        self.username = username
        self.real_name = real_name
        self.avatar = avatar
        if not (hasattr(self.__class__, "link") and isinstance(self.__class__.link, property)):
            # Subclasses may implement as a property, in which case the attribute set would fail.
            self.link = link
        self.raw = raw

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.id == other.id

    def __hash__(self):
        return hash(self.id)

    def __repr__(self):
        return "<{}: {} {}>".format(self.__class__.__name__, repr(self.id),
                                    repr(self.real_name or self.username))


class Segment:
    """
    Substring of message text with consistent formatting.

    Calling :meth:`str` on an instance will return the segment text.  Similarly, the length of a
    segment is just the length of the contained text.

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
        mention (.User):
            Target user mentioned in this segment.
    """

    def __init__(self, text, *, bold=False, italic=False, underline=False, strike=False,
                 code=False, pre=False, link=None, mention=None):
        self.text = text
        self.bold = bold
        self.italic = italic
        self.underline = underline
        self.strike = strike
        self.code = code
        self.pre = pre
        self.link = link
        self.mention = mention

    def __len__(self):
        return len(self.text)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.text == other.text

    def __hash__(self):
        return hash(self.text)

    def __str__(self):
        return self.text

    def __repr__(self):
        attrs = [" {}".format(attr) for attr in ("bold", "italic", "underline", "strike", "code",
                                                 "pre", "link", "mention") if getattr(self, attr)]
        return "<{}: {}{}>".format(self.__class__.__name__, repr(self.text), "".join(attrs))


class RichText:
    """
    Common standard for formatted message text, akin to Hangouts' message segments.  This is a
    container designed to hold instances of :class:`.Segment`.

    Calling :meth:`str` on an instance will return the entire message text without formatting.
    Indexing and slicing are possible, but will return :class:`.RichText` even if subclassed.

    Note that the length of an instance is equal to the sum of each segment's text length, not the
    number of segments.  To count the segments, call :meth:`list` on it to get a plain list.
    """

    def __init__(self, segments=None):
        self._segments = (list(segments) if segments else None) or []

    def normalise(self):
        """
        Make a copy of this message with formatting applied from text boundaries.

        For example::

            Some[b] bold [/b]text.

        The bold boundaries would be moved to surround "bold" excluding the spaces.

        Returns:
            .RichText:
                Normalised message text instance.
        """
        normalised = []
        for segment in self._segments:
            clone = copy(segment)
            before, clone.text, after = re.match(r"(\s*)(.*)(\s*)", clone.text).groups()
            if before:
                normalised.append(Segment(before))
            normalised.append(clone)
            if after:
                normalised.append(Segment(after))
        return RichText(normalised)

    def clone(self):
        """
        Make a copy of this message text and all its segments.

        Returns:
            .RichText:
                Cloned message text instance.
        """
        return RichText([copy(segment) for segment in self._segments])

    def prepend(self, *segments):
        """
        Insert one or more segments at the beginning of the text.

        Args:
            segments (.Segment list):
                New segments to lead the message text.
        """
        self._segments = list(segments) + self._segments

    def append(self, *segments):
        """
        Insert one or more segments at the end of the text.

        Args:
            segments (.Segment list):
                New segments to tail the message text.
        """
        self._segments += segments

    def trim(self, length):
        """
        Reduce a long message text to a snippet with an ellipsis.

        Args:
            length (int):
                Maximum length of the message text.

        Returns:
            .RichText:
                Trimmed message text instance.
        """
        if len(self) <= length:
            return self
        clone = RichText()
        total = 0
        for segment in self:
            if total + len(segment) < length:
                clone.append(copy(segment))
                total += len(segment)
            else:
                snip = (length - total) - 2
                snipped = copy(segment)
                snipped.text = "{}...".format(snipped.text[:snip])
                clone.append(snipped)
                break
        return clone

    def __len__(self):
        return sum(len(segment) for segment in self._segments)

    def __iter__(self):
        return iter(self._segments)

    def __getitem__(self, key):
        item = self._segments[key]
        return RichText(item) if isinstance(key, slice) else item

    def __eq__(self, other):
        return (isinstance(other, self.__class__) and len(self) == len(other) and
                all(x == y for x, y in zip(self, other)))

    def __hash__(self):
        return hash(self._segments)

    def __str__(self):
        # Fallback implementation: just return the message text without formatting.
        return "".join(str(segment) for segment in self._segments)

    def __repr__(self):
        return "<{}: {}>".format(self.__class__.__name__, repr(self._segments))


@pretty_str
class Attachment:
    """
    Base class for secondary data attached to a message.
    """


class File(Attachment):
    """
    Base file attachment object.

    Attributes:
        title (str):
            Name of file, if the plug supports names.
        type (.Type):
            Basic type of the file.
        source (str):
            Public URL to the original file location, if one is available.
    """

    class Type(Enum):
        unknown = 0
        image = 1

    def __init__(self, title=None, type=Type.unknown, source=None):
        self.title = title
        self.type = type
        self.source = source

    async def get_content(self, sess=None):
        """
        Stream the contents of the file, suitable for writing to a file or uploading elsewhere.

        The default implementation will try to fetch a file by the source field.  It may be
        overridden to add authentication or other metadata, but the method must remain callable
        with only a session passed to it.

        Args:
            sess (aiohttp.ClientSession):
                Existing HTTP session with which to make any requests.

        Returns:
            io.IOBase:
                Readable stream of the raw file.
        """
        sess = sess or aiohttp.ClientSession()
        return await sess.get(self.source)

    def __repr__(self):
        return "<{}: {} {}>".format(self.__class__.__name__, repr(self.title), self.type.name)


@pretty_str
class Message:
    """
    Base message object, understood by all plugs.

    Attributes:
        id (str):
            Unique (to the plug) message identifier.
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
        reply_to (.Message):
            Parent message, which this message replies to.
        joined (.User list):
            Collection of users that just joined the channel.
        left (.User list):
            Collection of users that just parted the channel.
        attachments (.Attachment list):
            Additional data included in the message.
        raw:
            Optional plug-specific underlying message or event object.
    """

    def __init__(self, id=None, at=None, original=None, text=None, user=None, action=False,
                 deleted=False, reply_to=None, joined=None, left=None, attachments=None, raw=None):
        self.id = id
        self.at = at or datetime.now()
        self.original = original
        self.text = text
        self.user = user
        self.action = action
        self.deleted = deleted
        self.reply_to = reply_to
        self.joined = joined or []
        self.left = left or []
        self.attachments = attachments or []
        self.raw = raw

    def render(self, *, real_name=True, delimiter=" ", quote_reply=False, trim=None):
        """
        Add the sender's name (if present) to the start of the message text, suitable for sending
        as-is on plugs that need all the textual message content in the body.

        Args:
            real_name (bool):
                ``True`` (default) to display real names, or ``False`` to prefer usernames.  If
                only one kind of name is available, it will be used regardless of this setting.
            delimiter (str):
                Characters added between the sender's name and the message text (space by default).
            quote_reply (bool):
                ``True`` to quote the parent message before the current one, prefixed with ``>``
                (not quoted by default).
            trim (int):
                Show an ellipsed snippet if the text exceeds this length, or ``None`` (default) for
                no trimming.

        Returns:
            .RichText:
                Rendered message body.
        """
        output = RichText()
        name = None
        action = self.action
        if self.user:
            if real_name:
                name = self.user.real_name or self.user.username
            else:
                name = self.user.username or self.user.real_name
        if self.text:
            if isinstance(self.text, RichText):
                text = self.text
            else:
                text = RichText([Segment(self.text)])
            if trim:
                text = text.trim(trim)
            output.append(*text)
        elif self.attachments:
            action = True
            count = len(self.attachments)
            what = "{} files".format(count) if count > 1 else "this file"
            output.append(Segment("sent {}".format(what)))
        if name:
            output.prepend(Segment(delimiter))
            if not action:
                output.prepend(Segment(":", bold=True))
            if self.original:
                output.prepend(Segment(" [edit]"))
            output.prepend(Segment(name, bold=True))
        elif self.original:
            output.prepend(Segment("[edit] "))
        if action:
            for segment in output:
                segment.italic = True
        if quote_reply and self.reply_to:
            output.prepend(Segment("> "),
                           *self.reply_to.render(real_name=real_name, trim=32),
                           Segment("\n"))
        return output

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.id == other.id

    def __hash__(self):
        return hash(self.id)

    def __repr__(self):
        return "<{}: {} ({} @ {}): {}>".format(self.__class__.__name__, repr(self.id),
                                               repr(self.user), self.at, repr(str(self.text)))
