from .util import Configurable, Openable, pretty_str


@pretty_str
class Hook(Configurable, Openable):
    """
    Base of all hook classes, performs any form of processing on messages from all connected
    plugs, via the provided host instance.

    Instantiation may raise :class:`.ConfigError` if the provided configuration is invalid.

    Attributes:
        virtual (bool):
            ``True`` if managed by another component (e.g. a hook that exposes plug functionality).
    """

    def __init__(self, name, config, host, virtual=False):
        super().__init__(name, config, host)
        self.virtual = virtual

    def on_load(self):
        """
        Perform any additional one-time setup that requires other plugs or hooks to be loaded.
        """

    def on_ready(self):
        """
        Perform any post-startup tasks once all hooks and plugs are ready.
        """

    async def channel_migrate(self, old, new):
        """
        Move any private data between channels on admin request.  This is intended to cover data
        keyed by channel sources and plug network identifiers.

        Args:
            old (.Channel):
                Existing channel with local data.
            new (.Channel):
                Target replacement channel to migrate data to.

        Returns:
            bool:
                ``True`` if any data was migrated for the requested channel.
        """
        return False

    async def before_send(self, channel, msg):
        """
        Modify an outgoing message before it's pushed to the network.  The ``(channel, msg)`` pair
        must be returned, so hooks may modify in-place or return a different pair.  This method is
        called for each hook, one after another.  If ``channel`` is modified, the sending will
        restart on the new channel, meaning this method will be called again for all hooks.

        Hooks may also suppress a message (e.g. if their actions caused it, but it bears no value
        to the network) by returning ``None``.

        Args:
            channel (.Channel):
                Original source of this message.
            msg (.Message):
                Raw message received from another plug.

        Returns:
            (.Channel, .Message) tuple:
                The augmented or replacement pair, or ``None`` to suppress this message.
        """
        return (channel, msg)

    async def before_receive(self, sent, source, primary):
        """
        Modify an incoming message before it's pushed to other hooks.  The ``sent`` object must be
        returned, so hooks may modify in-place or return a different object.  This method is called
        for each hook, one after another, so any time-consuming tasks should be deferred to
        :meth:`process` (which is run for all hooks in parallel).

        Hooks may also suppress a message (e.g. if their actions caused it, but it bears no value
        to the rest of the system) by returning ``None``.

        Args:
            sent (.SentMessage):
                Raw message received from another plug.
            source (.Message):
                Original message data used to generate the raw message, if sent via the plug (e.g.
                from another hook), equivalent to ``msg`` if the source is otherwise unknown.
            primary (bool):
                ``False`` for supplementary messages if the source message required multiple raw
                messages in order to represent it (e.g. messages with multiple attachments where
                the underlying network doesn't support it), otherwise ``True``.

        Returns:
            .SentMessage:
                The augmented or replacement message, or ``None`` to suppress this message.
        """
        return sent

    async def on_receive(self, sent, source, primary):
        """
        Handle an incoming message received by any plug.

        Args:
            sent (.SentMessage):
                Raw message received from another plug.
            source (.Message):
                Original message data used to generate the raw message, if sent via the plug (e.g.
                from another hook), equivalent to ``msg`` if the source is otherwise unknown.
            primary (bool):
                ``False`` for supplementary messages if the source message required multiple raw
                messages in order to represent it (e.g. messages with multiple attachments where
                the underlying network doesn't support it), otherwise ``True``.
        """

    def on_config_change(self, source):
        """
        Handle a configuration change from another plug or hook.

        Args:
            source (.Configurable):
                Source plug or hook that triggered the event.
        """

    def __repr__(self):
        return "<{}: {}>".format(self.__class__.__name__, self.name)


class ResourceHook(Hook):
    """
    Variant of hooks that globally provide access to some resource.

    Only one of each class may be loaded, which happens before regular hooks, and such hooks are
    keyed by their class rather than a name, allowing for easier lookups.
    """
