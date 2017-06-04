from .util import Base


class Channel(Base):
    """
    Container class that holds a (:class:`.Transport`, :class:`str`) pair reoresenting a room
    inside the transport's network.

    Attributes:
        name (str):
            User-provided, unique name of the transport, used for config references.
        transport (.Transport):
            Related transport instance where the channel resides.
        source (str):
            Transport-specific channel identifier.
    """

    def __init__(self, name, transport, source):
        self.name = name
        self.transport = transport
        self.source = source

    async def send(self, msg):
        """
        Push a message object to the related transport on this channel.

        Args:
            msg (.Message):
                Original message received from another channel or transport.
        """
        return (await self.transport.send(self, msg))
