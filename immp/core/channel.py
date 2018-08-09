from voluptuous import ALLOW_EXTRA, Any, Optional, Schema

from .util import ConfigProperty, pretty_str


_GROUP_FIELDS = ("channels", "anywhere", "named", "private", "shared")


class _Schema:

    group = Schema({Optional(field, default=list): Any([str], []) for field in _GROUP_FIELDS},
                   extra=ALLOW_EXTRA, required=True)


@pretty_str
class Channel:
    """
    Container class that holds a (:class:`.Plug`, :class:`str`) pair representing a room
    inside the plug's network.

    Attributes:
        plug (.Plug):
            Related plug instance where the channel resides.
        source (str):
            Plug-specific channel identifier.
    """

    class Property(ConfigProperty):

        def __init__(self):
            super().__init__("channel")

    def __init__(self, plug, source):
        self.plug = plug
        self.source = str(source)

    async def is_private(self):
        """
        Equivalent to :meth:`.Plug.channel_is_private`.

        Returns:
            bool:
                ``True`` if the channel is private; ``None`` if the service doesn't have a notion
                of private channels.
        """
        return await self.plug.channel_is_private(self)

    async def title(self):
        """
        Equivalent to :meth:`.Plug.channel_title`.

        Returns:
            str:
                Display name for the channel.
        """
        return await self.plug.channel_title(self)

    async def link(self):
        """
        Equivalent to :meth:`.Plug.channel_link`.

        Returns:
            str:
                Internal deep link to this channel.
        """
        return await self.plug.channel_link(self)

    async def rename(self, title):
        """
        Equivalent to :meth:`.Plug.channel_rename`.

        Args:
            title (str):
                New display name for the channel.
        """
        return await self.plug.channel_rename(self, title)

    async def members(self):
        """
        Equivalent to :meth:`.Plug.channel_members`.

        Returns:
            .User list:
                Members present in the channel.
        """
        return await self.plug.channel_members(self)

    async def invite(self, user):
        """
        Equivalent to :meth:`.Plug.channel_invite`.

        Args:
            user (.User):
                New user to invite.
        """
        return await self.plug.channel_invite(self, user)

    async def remove(self, user):
        """
        Equivalent to :meth:`.Plug.channel_remove`.

        Args:
            user (.User):
                Existing user to kick.
        """
        return await self.plug.channel_remove(self, user)

    async def send(self, msg):
        """
        Push a message object to the related plug on this channel.

        Args:
            msg (.Message):
                Original message received from another channel or plug.
        """
        return await self.plug.send(self, msg)

    def __eq__(self, other):
        return (isinstance(other, self.__class__) and
                self.plug == other.plug and self.source == other.source)

    def __hash__(self):
        return hash((self.plug.network_id, self.source))

    def __repr__(self):
        return "<{}: {} @ {}>".format(self.__class__.__name__, self.plug.name, self.source)


@pretty_str
class Group:
    """
    Container of multiple channels.

    Groups cannot be iterated, as they may hold any possible channel from a :class:`.Plug`, but you
    can test for membership of a given :class:`.Channel` using :meth:`has`.

    A group is defined by a base list of channels, and/or lists of channel types from plugs.  The
    latter may target **private** or **shared** (non-private) channels, **named** for host-defined
    channels, or **anywhere** as long as it belongs to the given plug.
    """

    class Property(ConfigProperty):

        def __init__(self):
            super().__init__("group")

        def __get__(self, instance, owner):
            value = super().__get__(instance, owner)
            if instance:
                return Group.merge(instance.host, *value)
            else:
                return value

    _channels = Channel.Property()

    def __init__(self, name, config, host):
        self.name = name
        self.config = _Schema.group(config)
        self.host = host

    @classmethod
    def merge(cls, host, *groups):
        config = {field: [] for field in _GROUP_FIELDS}
        for group in groups:
            for field in _GROUP_FIELDS:
                config[field] += group.config[field]
        return cls(None, config, host)

    async def has_channel(self, channel):
        if not isinstance(channel, Channel):
            raise TypeError
        elif channel in self._channels:
            return True
        elif self.has_plug(channel.plug, "anywhere"):
            return True
        elif self.has_plug(channel.plug, "named") and channel in self.host.channels.values():
            return True
        private = await channel.is_private()
        if self.has_plug(channel.plug, "private") and private:
            return True
        elif self.has_plug(channel.plug, "shared") and not private:
            return True
        else:
            return False

    def has_plug(self, plug, *fields):
        return any(plug.name in self.config[field] for field in fields or _GROUP_FIELDS)

    def __repr__(self):
        return "<{}: {}>".format(self.__class__.__name__, self.name)