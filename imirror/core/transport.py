from asyncio import BoundedSemaphore, Queue

from .error import TransportError
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

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.name == other.name

    def __hash__(self):
        return hash(self.name)

    def __repr__(self):
        return "<{}: {} ({} @ {})>".format(self.__class__.__name__, self.name, self.source,
                                           self.transport.name)


class Transport(Base):
    """
    Base of all transport classes, handles communication with an external network by converting
    outside data into standardised message objects, and pushing new messages into the network.

    Instantiation may raise :class:`.ConfigError` if the provided configuration is invalid.

    Attributes:
        name (str):
            User-provided, unique name of the transport, used for config references.
        config (dict):
            Reference to the user-provided configuration.
        host (.Host):
            Controlling host instance, providing access to transports.
        connected (bool):
            Whether this transport currently maintains a connection to the external network.
    """

    def __init__(self, name, config, host):
        self.name = name
        self.config = config
        self.host = host
        self.connected = False
        # Message queue, to move processing from the event stream to the generator.
        self._queue = Queue()
        # Receiver lock, to put a hold on retrieving messages whilst a send is in progress.
        self._lock = BoundedSemaphore()

    async def connect(self):
        """
        Start a connection to the external network.
        """
        self.connected = True

    async def disconnect(self):
        """
        Terminate the external network connection.
        """
        self.connected = False

    def queue(self, channel, msg):
        """
        Add a new message to the receiver queue.

        Args:
            channel (.Channel):
                Source channel for the incoming message.
            msg (.Message):
                Message received and processed by the transport.
        """
        self._queue.put_nowait((channel, msg))

    async def receive(self):
        """
        Wrapper method to receive messages from the network.  Transports should implement
        :meth:`get` to yield a series of channel/message pairs from a continuous source (e.g.
        listening on a socket or long-poll).  For event-driven frameworks, use :meth:`queue` to
        submit new messages, which will be handled by default.

        Yields:
            (.Channel, .Message) tuple:
                Messages received and processed by the transport.
        """
        if not self.connected:
            raise TransportError("Can't receive messages when not connected")
        getter = self.get()
        async for channel, msg in getter:
            with (await self._lock):
                # No critical section here, just wait for any pending messages to be sent.
                pass
            yield (channel, msg)

    async def get(self):
        """
        Generator of :class:`.Message` objects from the underlying network.

        By default, reads from the built-in message queue, but may be overridden to use a
        different source.

        Yields:
            (.Channel, .Message) tuple:
                Messages received and processed by the transport.
        """
        while True:
            yield (await self._queue.get())

    async def send(self, channel, msg):
        """
        Wrapper method to send a message to the network.  Transports should implement :meth:`put`
        to convert the framework message into a native representation and submit it.

        Args:
            channel (.Channel):
                Target channel for the new message.
            msg (.Message):
                Original message received from another channel or transport.

        Returns:
            list:
                IDs of new messages sent to the transport.
        """
        if not self.connected:
            raise TransportError("Can't send messages when not connected")
        # When sending messages asynchronously, the network will likely return the new message
        # before the send request returns with confirmation.  Use the lock when sending in order
        # return the new message ID(s) in advance of them appearing in the receive queue.
        with (await self._lock):
            return await self.put(channel, msg)

    async def put(self, channel, msg):
        """
        Take a :class:`.Message` object, and push it to the underlying network.

        Because some transports may not support combinations of message components (such as text
        and an accompanying image), this method may send more than one physical message.

        Args:
            channel (.Channel):
                Target channel for the new message.
            msg (.Message):
                Original message received from another channel or transport.

        Returns:
            list:
                IDs of new messages sent to the transport.
        """
        raise NotImplementedError

    def __repr__(self):
        return "<{}: {}>".format(self.__class__.__name__, self.name)
