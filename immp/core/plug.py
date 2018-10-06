from asyncio import BoundedSemaphore, CancelledError, Queue, ensure_future, wait
from itertools import chain
import logging

from .error import PlugError
from .util import Openable, OpenState, pretty_str


log = logging.getLogger(__name__)


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
                sent, source, primary = task.result()
            except StopAsyncIteration:
                log.debug("Plug '{}' finished yielding during process".format(plug.name))
                del self._coros[plug]
            except Exception:
                log.exception("Plug '{}' raised error during process".format(plug.name))
                del self._coros[plug]
            else:
                log.debug("Received: {}".format(repr(sent)))
                await self.callback(sent, source, primary)
                self._queue(plug)
            finally:
                del self._plugs[task]

    async def process(self):
        """
        Retrieve messages from plugs, and distribute them to hooks.
        """
        while self._plugs:
            try:
                log.debug("Waiting for next message")
                await self._wait()
            except CancelledError:
                log.debug("Host process cancelled, propagating to tasks")
                for task in self._plugs.keys():
                    task.cancel()
                log.debug("Resuming tasks to collect final messages")
                await self._wait()
        log.debug("All tasks completed")

    def __repr__(self):
        return "<{}: {} tasks>".format(self.__class__.__name__, len(self._coros))


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
        virtual (bool):
            ``True`` if managed by another component (e.g. a hook that exposes plug functionality).
        network_name (str):
            Readable name of the underlying network, for use when displaying info about this plug.
        network_id (str):
            Unique and stable identifier for this plug.

            This should usually vary by the account or keys being used to connect, but persistent
            with later connections.  If a network provides multiple distinct spaces, this should
            also vary by space.
    """

    def __init__(self, name, config, host, virtual=False):
        super().__init__()
        self.name = name
        self.config = config
        self.host = host
        self.virtual = virtual
        # Active generator created from get(), referenced to cancel on disconnect.
        self._getter = None
        # Message queue, to move processing from the event stream to the generator.
        self._queue = Queue()
        # Message history, to match up received messages with their sent sources.
        # Mapping from (channel, message ID) to (source message, all IDs).
        self._sent = {}
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

    async def user_from_id(self, id):
        """
        Retrieve a :class:`.User` based on the underlying network's identifier.

        Args:
            id:
                Network identifier of the user.

        Returns:
            .User:
                Corresponding user instance.
        """
        return None

    async def user_from_username(self, username):
        """
        Retrieve a :class:`.User` based on the underlying network's username.

        Args:
            username:
                Network username of the user.

        Returns:
            .User:
                Corresponding user instance.
        """
        return None

    async def channel_for_user(self, user):
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

    async def channel_is_private(self, channel):
        """
        Test if a given channel represents a private (one-to-one) conversation between a given user
        and the service.  May return ``None`` if the network doesn't have a notion of public/shared
        and private channels.

        Args:
            channel (.Channel):
                Requested channel instance.

        Returns:
            bool:
                ``True`` if the channel is private.
        """
        return None

    async def channel_title(self, channel):
        """
        Retrieve the friendly name of this channel, as used in the underlying network.  May return
        ``None`` if the service doesn't have a notion of titles.

        Returns:
            str:
                Display name for the channel.
        """
        return None

    async def channel_link(self, channel):
        """
        Return a URL that acts as a direct link to the given channel.  This is not a join link,
        rather one that opens a conversation in the client (it may e.g. use a custom protocol).

        Returns:
            str:
                Internal deep link to this channel.
        """
        return None

    async def channel_rename(self, channel, title):
        """
        Update the friendly name of this conversation.

        Args:
            title (str):
                New display name for the channel.
        """

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

    def queue(self, sent):
        """
        Add a new message to the queue, picked up from :meth:`get` by default.

        Args:
            sent (.SentMessage):
                Message received and processed by the plug.
        """
        self._queue.put_nowait(sent)

    async def receive(self):
        """
        Wrapper method to receive messages from the network.  Plugs should implement
        :meth:`get` to yield a series of channel/message pairs from a continuous source (e.g.
        listening on a socket or long-poll).  For event-driven frameworks, use :meth:`queue` to
        submit new messages, which will be handled by default.

        Yields:
            (.SentMessage, .Message, bool) tuple:
                Messages received and processed by the plug, paired with a source message if one
                exists (from a call to :meth:`send`).
        """
        if not self.state == OpenState.active:
            raise PlugError("Can't receive messages when not active")
        if self._getter:
            raise PlugError("Plug is already receiving messages")
        self._getter = self.get()
        try:
            async for sent in self._getter:
                async with self._lock:
                    # No critical section here, just wait for any pending messages to be sent.
                    pass
                try:
                    # If this message was sent by us, retrieve the canonical version.  For source
                    # messages split into multiple parts, only make the first raw message primary.
                    source, ids = self._sent[(sent.channel, sent.id)]
                    primary = ids.index(sent.id) == 0
                except KeyError:
                    # The message came from elsewhere, consider it the canonical copy.
                    source = sent
                    primary = True
                yield (sent, source, primary)
        finally:
            await self._getter.aclose()
            self._getter = None

    async def get(self):
        """
        Generator of :class:`.SentMessage` objects from the underlying network.

        By default, it reads from the built-in message queue (see :meth:`queue`), which works well
        for event-based libraries.  If the receive logic is a singular logic path (e.g. repeated
        long-polling), this method may be overridden to yield messages manually.

        If :attr:`state` is :attr:`.OpenState.stopping`, this method should stop making network
        requests for further messages, return any remaining or queued messages, and then terminate.

        Yields:
            .SentMessage:
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
        # Allow any hooks to modify the outgoing message before sending.
        original = channel
        for hook in chain(self.host.resources.values(), self.host.hooks.values()):
            if not hook.state == OpenState.active:
                continue
            try:
                result = await hook.before_send(channel, msg)
            except Exception:
                log.exception("Hook '{}' failed before-send event".format(hook.name))
                continue
            if result:
                channel, msg = result
            else:
                # Message has been suppressed by a hook.
                break
        if not original == channel:
            log.debug("Redirecting message to new channel: {}".format(repr(channel)))
            # Restart sending with the new channel.
            return await channel.send(msg)
        # When sending messages asynchronously, the network will likely return the new message
        # before the send request returns with confirmation.  Use the lock when sending in order
        # return the new message ID(s) in advance of them appearing in the receive queue.
        async with self._lock:
            ids = await self.put(channel, msg)
        for id in ids:
            self._sent[(channel, id)] = (msg, ids)
        return ids

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
        return "<{}{}>".format(self.__class__.__name__,
                               ": {}".format(self.network_id) if self.network_id else "")
