from asyncio import ensure_future, wait, BoundedSemaphore, Queue, CancelledError
import logging

from .error import TransportError
from .util import pretty_str, OpenState, Openable


log = logging.getLogger(__name__)


@pretty_str
class Channel:
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


class TransportStream:
    """
    Manager for reading from multiple asynchronous generators in parallel.

    Requires a callback coroutine that accepts (:class:`.Channel`, :class:`.Message`) arguments.
    """

    def __init__(self, callback, *transports):
        self._coros = {}
        self._transports = {}
        self.callback = callback
        self.add(*transports)

    def _queue(self, transport):
        # Poor man's async iteration -- there's no async equivalent to next(gen).
        log.debug("Queueing receive task for transport '{}'".format(transport.name))
        self._transports[ensure_future(self._coros[transport].asend(None))] = transport

    def add(self, *transports):
        """
        Register transports for reading.  Transports should be opened prior to registration.

        Args:
            transports (.Transport list):
                Connected transport instances to register.
        """
        for transport in transports:
            if not transport.state == OpenState.active:
                raise RuntimeError("Transport '{}' is not open".format(transport.name))
            self._coros[transport] = transport.receive()
            self._queue(transport)

    def has(self, transport):
        """
        Check for the existence of a transport in the manager.

        Args:
            transport (.Transport):
                Connected transport instance to check.

        Returns:
            bool:
                ``True`` if a :meth:`.Transport.receive` call is still active.
        """
        return (transport in self._coros)

    async def _wait(self):
        done, pending = await wait(list(self._transports.keys()), return_when="FIRST_COMPLETED")
        for task in done:
            transport = self._transports[task]
            try:
                channel, msg = task.result()
            except StopAsyncIteration:
                log.debug("Transport '{}' finished yielding during process".format(transport.name))
                del self._coros[transport]
            else:
                log.debug("Received: {} {}".format(repr(channel), repr(msg)))
                await self.callback(channel, msg)
                self._queue(transport)
            finally:
                del self._transports[task]

    async def process(self):
        """
        Retrieve messages from transports, and distribute them to receivers.
        """
        while self._transports:
            try:
                await self._wait()
            except CancelledError:
                log.debug("Host process cancelled, propagating to tasks")
                for task in self._transports.keys():
                    task.cancel()
                log.debug("Resuming tasks to collect final messages")
                await self._wait()
        log.debug("All tasks completed")


@pretty_str
class Transport(Openable):
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
    """

    def __init__(self, name, config, host):
        super().__init__()
        self.name = name
        self.config = config
        self.host = host
        # Active generator created from get(), referenced to cancel on disconnect.
        self._getter = None
        # Message queue, to move processing from the event stream to the generator.
        self._queue = Queue()
        # Receiver lock, to put a hold on retrieving messages whilst a send is in progress.
        self._lock = BoundedSemaphore()

    async def start(self):
        """
        Start a connection to the external network.

        If using an event-driven framework that yields and runs in the background, you should use
        a signal of some form (e.g. :class:`asyncio.Condition`) to block this method until the
        underlying network is ready for use.
        """
        await super().start()

    async def stop(self):
        """
        Terminate the external network connection.

        Like with :meth:`start`, this should block if needed, such that this method ends when the
        transport can be started again.
        """
        await super().stop()

    async def private_channel(self, user):
        """
        Retrieve a :class:`.Channel` representing a private (one-to-one) conversation between a
        given user and the service.  Returns ``None`` if the user does not have a private channel.

        Args:
            user (.User):
                Requested user instance.

        Returns:
            .Channel:
                Private channel for this user.
        """
        return None

    async def channel_members(self, channel):
        """
        Retrieve a :class:`.User` list representing all members of the given channel.  May return
        ``None`` if the transport doesn't recognise the channel, or is unable to query members.

        Args:
            channel (.Channel):
                Requested channel instance.

        Returns:
            .User list:
                Members present in the channel.
        """
        return None

    def queue(self, channel, msg):
        """
        Add a new message to the queue, picked up from :meth:`get` by default.

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
        if not self.state == OpenState.active:
            raise TransportError("Can't receive messages when not active")
        if self._getter:
            raise TransportError("Transport is already receiving messages")
        self._getter = self.get()
        try:
            async for channel, msg in self._getter:
                with (await self._lock):
                    # No critical section here, just wait for any pending messages to be sent.
                    pass
                yield (channel, msg)
        finally:
            await self._getter.aclose()
            self._getter = None

    async def get(self):
        """
        Generator of :class:`.Message` objects from the underlying network.

        By default, it reads from the built-in message queue (see :meth:`queue`), which works well
        for event-based libraries.  If the receive logic is a singular logic path (e.g. repeated
        long-polling), this method may be overridden to yield messages manually.

        If :attr:`state` is :attr:`.OpenState.stopping`, this method should stop making network
        requests for further messages, return any remaining or queued messages, and then terminate.

        Yields:
            (.Channel, .Message) tuple:
                Messages received and processed by the transport.
        """
        try:
            while self.state == OpenState.active:
                yield (await self._queue.get())
        except GeneratorExit:
            log.debug("Immediate exit from transport '{}' getter".format(self.name))
            # Caused by gen.aclose(), in this state we can't yield any further messages.
        except CancelledError:
            log.debug("Cancel request for transport '{}' getter".format(self.name))
            # Fetch any remaining messages from the queue.
            if not self._queue.empty():
                log.debug("Retrieving {} queued message(s) for '{}'"
                          .format(self._queue.qsize(), self.name))
                while not self._queue.empty():
                    yield self._queue.get_nowait()

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
        if not self.state == OpenState.active:
            raise TransportError("Can't send messages when not active")
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
        return []

    def __repr__(self):
        return "<{}: {}>".format(self.__class__.__name__, self.name)
