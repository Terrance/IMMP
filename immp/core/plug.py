from asyncio import BoundedSemaphore, CancelledError, Queue, ensure_future, wait
import logging

from .error import PlugError
from .util import Openable, OpenState, pretty_str


log = logging.getLogger(__name__)


@pretty_str
class Channel:
    """
    Container class that holds a (:class:`.Plug`, :class:`str`) pair reoresenting a room
    inside the plug's network.

    Attributes:
        name (str):
            User-provided, unique name of the plug, used for config references.
        plug (.Plug):
            Related plug instance where the channel resides.
        source (str):
            Plug-specific channel identifier.
    """

    def __init__(self, name, plug, source):
        self.name = name
        self.plug = plug
        self.source = source

    async def send(self, msg):
        """
        Push a message object to the related plug on this channel.

        Args:
            msg (.Message):
                Original message received from another channel or plug.
        """
        return (await self.plug.send(self, msg))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.name == other.name

    def __hash__(self):
        return hash(self.name)

    def __repr__(self):
        return "<{}: {} ({} @ {})>".format(self.__class__.__name__, self.name, self.source,
                                           self.plug.name)


class PlugStream:
    """
    Manager for reading from multiple asynchronous generators in parallel.

    Requires a callback coroutine that accepts (:class:`.Channel`, :class:`.Message`) arguments.
    """

    def __init__(self, callback, *plugs):
        self._coros = {}
        self._plugs = {}
        self.callback = callback
        self.add(*plugs)

    def _queue(self, plug):
        # Poor man's async iteration -- there's no async equivalent to next(gen).
        log.debug("Queueing receive task for plug '{}'".format(plug.name))
        self._plugs[ensure_future(self._coros[plug].asend(None))] = plug

    def add(self, *plugs):
        """
        Register plugs for reading.  Plugs should be opened prior to registration.

        Args:
            plugs (.Plug list):
                Connected plug instances to register.
        """
        for plug in plugs:
            if not plug.state == OpenState.active:
                raise RuntimeError("Plug '{}' is not open".format(plug.name))
            self._coros[plug] = plug.receive()
            self._queue(plug)

    def has(self, plug):
        """
        Check for the existence of a plug in the manager.

        Args:
            plug (.Plug):
                Connected plug instance to check.

        Returns:
            bool:
                ``True`` if a :meth:`.Plug.receive` call is still active.
        """
        return (plug in self._coros)

    async def _wait(self):
        done, pending = await wait(list(self._plugs.keys()), return_when="FIRST_COMPLETED")
        for task in done:
            plug = self._plugs[task]
            try:
                channel, msg = task.result()
            except StopAsyncIteration:
                log.debug("Plug '{}' finished yielding during process".format(plug.name))
                del self._coros[plug]
            else:
                log.debug("Received: {} {}".format(repr(channel), repr(msg)))
                await self.callback(channel, msg)
                self._queue(plug)
            finally:
                del self._plugs[task]

    async def process(self):
        """
        Retrieve messages from plugs, and distribute them to hooks.
        """
        while self._plugs:
            try:
                await self._wait()
            except CancelledError:
                log.debug("Host process cancelled, propagating to tasks")
                for task in self._plugs.keys():
                    task.cancel()
                log.debug("Resuming tasks to collect final messages")
                await self._wait()
        log.debug("All tasks completed")


@pretty_str
class Plug(Openable):
    """
    Base of all plug classes, handles communication with an external network by converting
    outside data into standardised message objects, and pushing new messages into the network.

    Instantiation may raise :class:`.ConfigError` if the provided configuration is invalid.

    Attributes:
        name (str):
            User-provided, unique name of the plug, used for config references.
        config (dict):
            Reference to the user-provided configuration.
        host (.Host):
            Controlling host instance, providing access to plugs.
    """

    class Meta:
        """
        Metadata relating to the plug or underlying network.

        Attributes:
            network (str):
                Readable name of the network provided by this plug.
        """
        network = None

    def __init__(self, name, config, host):
        super().__init__()
        self.name = name
        self.config = config
        self.host = host
        # Active generator created from get(), referenced to cancel on disconnect.
        self._getter = None
        # Message queue, to move processing from the event stream to the generator.
        self._queue = Queue()
        # Hook lock, to put a hold on retrieving messages whilst a send is in progress.
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
        plug can be started again.
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
        ``None`` if the plug doesn't recognise the channel, or is unable to query members.

        Args:
            channel (.Channel):
                Requested channel instance.

        Returns:
            .User list:
                Members present in the channel.
        """
        return None

    async def channel_invite(self, channel, user):
        """
        Add the given user to the channel's list of members.

        Args:
            channel (.Channel):
                Requested channel instance.
            user (.User):
                New user to invite.
        """

    async def channel_remove(self, channel, user):
        """
        Remove the given user from the channel's list of members.

        Args:
            channel (.Channel):
                Requested channel instance.
            user (.User):
                Existing user to kick.
        """

    def queue(self, channel, msg):
        """
        Add a new message to the queue, picked up from :meth:`get` by default.

        Args:
            channel (.Channel):
                Source channel for the incoming message.
            msg (.Message):
                Message received and processed by the plug.
        """
        self._queue.put_nowait((channel, msg))

    async def receive(self):
        """
        Wrapper method to receive messages from the network.  Plugs should implement
        :meth:`get` to yield a series of channel/message pairs from a continuous source (e.g.
        listening on a socket or long-poll).  For event-driven frameworks, use :meth:`queue` to
        submit new messages, which will be handled by default.

        Yields:
            (.Channel, .Message) tuple:
                Messages received and processed by the plug.
        """
        if not self.state == OpenState.active:
            raise PlugError("Can't receive messages when not active")
        if self._getter:
            raise PlugError("Plug is already receiving messages")
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
                Messages received and processed by the plug.
        """
        try:
            while self.state == OpenState.active:
                yield (await self._queue.get())
        except GeneratorExit:
            log.debug("Immediate exit from plug '{}' getter".format(self.name))
            # Caused by gen.aclose(), in this state we can't yield any further messages.
        except CancelledError:
            log.debug("Cancel request for plug '{}' getter".format(self.name))
            # Fetch any remaining messages from the queue.
            if not self._queue.empty():
                log.debug("Retrieving {} queued message(s) for '{}'"
                          .format(self._queue.qsize(), self.name))
                while not self._queue.empty():
                    yield self._queue.get_nowait()

    async def send(self, channel, msg):
        """
        Wrapper method to send a message to the network.  Plugs should implement :meth:`put`
        to convert the framework message into a native representation and submit it.

        Args:
            channel (.Channel):
                Target channel for the new message.
            msg (.Message):
                Original message received from another channel or plug.

        Returns:
            list:
                IDs of new messages sent to the plug.
        """
        if not self.state == OpenState.active:
            raise PlugError("Can't send messages when not active")
        # When sending messages asynchronously, the network will likely return the new message
        # before the send request returns with confirmation.  Use the lock when sending in order
        # return the new message ID(s) in advance of them appearing in the receive queue.
        with (await self._lock):
            return await self.put(channel, msg)

    async def put(self, channel, msg):
        """
        Take a :class:`.Message` object, and push it to the underlying network.

        Because some plugs may not support combinations of message components (such as text
        and an accompanying image), this method may send more than one physical message.

        Args:
            channel (.Channel):
                Target channel for the new message.
            msg (.Message):
                Original message received from another channel or plug.

        Returns:
            list:
                IDs of new messages sent to the plug.
        """
        return []

    def __repr__(self):
        return "<{}: {}>".format(self.__class__.__name__, self.name)
