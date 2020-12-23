from asyncio import BoundedSemaphore, Queue
import logging

from .error import PlugError
from .message import Message, Receipt
from .util import Configurable, Openable, OpenState, pretty_str


log = logging.getLogger(__name__)


@pretty_str
class Plug(Configurable, Openable):
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

    network_name = network_id = None

    def __init__(self, name, config, host, virtual=False):
        super().__init__(name, config, host)
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

    async def user_from_id(self, id_):
        """
        Retrieve a :class:`.User` based on the underlying network's identifier.

        Args:
            id (str):
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
            username (str):
                Network username of the user.

        Returns:
            .User:
                Corresponding user instance.
        """
        return None

    async def user_is_system(self, user):
        """
        Check if a given user is automated by the plug (for example a bot user from which the plug
        operates).  Hooks may exclude system users from certain operations.

        Returns:
            bool:
                ``True`` if the user relates to the plug itself.
        """
        return False

    async def public_channels(self):
        """
        Retrieve all shared channels known to this plug, either public or otherwise accessible.
        May return ``None`` if the network doesn't support channel discovery.

        Returns:
            .Channel list:
                All available non-private channels.
        """
        return None

    async def private_channels(self):
        """
        Retrieve all private (one-to-one) channels known to this plug.  May return ``None`` if the
        network doesn't support channel discovery.

        Returns:
            .Channel list:
                All available private channels.
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

    async def channel_history(self, channel, before=None):
        """
        Retrieve the most recent messages sent or received in the given channel.  May return an
        empty list if the plug is unable to query history.

        Args:
            channel (.Channel):
                Requested channel instance.
            before (.Receipt):
                Starting point message, or ``None`` to fetch the most recent.

        Returns:
            .Receipt list:
                Messages from the channel, oldest first.
        """
        return []

    async def get_message(self, receipt):
        """
        Lookup a :class:`.Receipt` and fetch the corresponding :class:`.SentMessage`.

        Args:
            receipt (.Receipt):
                Existing message reference to retrieve.

        Returns:
            .SentMessage:
                Full message.
        """
        return None

    async def resolve_message(self, msg):
        """
        Lookup a :class:`.Receipt` if no :class:`.Message` data is present, and fetch the
        corresponding :class:`.SentMessage`.

        Args:
            msg (.Message | .Receipt):
                Existing message reference to retrieve.

        Returns:
            .SentMessage:
                Full message.
        """
        if msg is None:
            return None
        elif isinstance(msg, Message):
            return msg
        elif isinstance(msg, Receipt):
            return await self.get_message(msg)
        else:
            raise TypeError

    def queue(self, sent):
        """
        Add a new message to the queue, picked up from :meth:`get` by default.

        Args:
            sent (.SentMessage):
                Message received and processed by the plug.
        """
        self._queue.put_nowait(sent)

    def _lookup(self, sent):
        try:
            # If this message was sent by us, retrieve the canonical version.  For source
            # messages split into multiple parts, only make the first raw message primary.
            source, ids = self._sent[(sent.channel, sent.id)]
            primary = ids.index(sent.id) == 0
        except KeyError:
            # The message came from elsewhere, consider it the canonical copy.
            source = sent
            primary = True
        return (sent, source, primary)

    async def stream(self):
        """
        Wrapper method to receive messages from the network.  Plugs should implement their own
        retrieval of messages, scheduled as a background task or managed by another async client,
        then call :meth:`queue` for each received message to have it yielded here.

        This method is called by the :class:`.PlugStream`, in parallel with other plugs to produce
        a single stream of messages across all sources.

        Yields:
            (.SentMessage, .Message, bool) tuple:
                Messages received and processed by the plug, paired with a source message if one
                exists (from a call to :meth:`send`).

        .. warning::
            Because the message buffer is backed by an :class:`asyncio.Queue`, only one generator
            from this method should be used at once -- each message will only be retrieved from the
            queue once, by the first instance that asks for it.
        """
        try:
            while True:
                sent = await self._queue.get()
                async with self._lock:
                    # No critical section here, just wait for any pending messages to be sent.
                    pass
                yield self._lookup(sent)
        except GeneratorExit:
            # Caused by gen.aclose(), in this state we can't yield any further messages.
            log.debug("Immediate exit from plug %r getter", self.name)
        except Exception:
            if not self._queue.empty():
                log.debug("Retrieving queued messages for %r", self.name)
            while not self._queue.empty():
                yield self._lookup(self._queue.get_nowait())
            raise

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
            .Receipt list:
                References to new messages sent to the plug.
        """
        if not self.state == OpenState.active:
            raise PlugError("Can't send messages when not active")
        # Allow any hooks to modify the outgoing message before sending.
        original = channel
        ordered = self.host.ordered_hooks()
        for hooks in ordered:
            for hook in hooks:
                try:
                    result = await hook.before_send(channel, msg)
                except Exception:
                    log.exception("Hook %r failed before-send event", hook.name)
                    continue
                if result:
                    channel, msg = result
                else:
                    # Message has been suppressed by a hook.
                    return []
        if not original == channel:
            log.debug("Redirecting message to new channel: %r", channel)
            # Restart sending with the new channel, including a new round of before-send.
            return await channel.send(msg)
        # When sending messages asynchronously, the network will likely return the new message
        # before the send request returns with confirmation.  Use the lock when sending in order
        # return the new message ID(s) in advance of them appearing in the receive queue.
        async with self._lock:
            receipts = await self.put(channel, msg)
        ids = [receipt.id for receipt in receipts]
        for id_ in ids:
            self._sent[(channel, id_)] = (msg, ids)
        return receipts

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
            .Receipt list:
                References to new messages sent to the plug.
        """
        return []

    async def delete(self, sent):
        """
        Request deletion of this message, if supported by the network.

        Args:
            sent (.SentMessage):
                Existing message to be removed.
        """

    def on_load(self):
        """
        Perform any additional one-time setup that requires other plugs or hooks to be loaded.
        """

    def __repr__(self):
        return "<{}{}>".format(self.__class__.__name__,
                               ": {}".format(self.network_id) if self.network_id else "")
