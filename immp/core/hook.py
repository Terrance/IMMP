from .util import Openable, pretty_str


@pretty_str
class Hook(Openable):
    """
    Base of all hook classes, performs any form of processing on messages from all connected
    plugs, via the provided host instance.

    Instantiation may raise :class:`.ConfigError` if the provided configuration is invalid.

    Attributes:
        name (str):
            User-provided, unique name of the hook, used for config references.
        config (dict):
            Reference to the user-provided configuration.
        host (.Host):
            Controlling host instance, providing access to plugs.
    """

    def __init__(self, name, config, host):
        super().__init__()
        self.name = name
        self.config = config
        self.host = host

    async def start(self):
        """
        Perform any setup tasks.
        """

    async def stop(self):
        """
        Perform any teardown tasks.
        """

    async def preprocess(self, channel, msg, source, primary):
        """
        Modify an incoming message before it's pushed to other hooks.  The ``(channel, msg)`` pair
        must be returned, so hooks may modify in-place or return a different pair.  This method is
        called for each hook, one after another, so any time-consuming tasks should be deferred to
        :meth:`process` (which is run for all hooks in parallel).

        Hooks may also suppress a message (e.g. if their actions caused it, but it bears no value
        to the rest of the system) by returning ``None``.

        Args:
            channel (.Channel):
                Original source of this message.
            msg (.Message):
                Raw message received from another plug.
            source (.Message):
                Original message data used to generate the raw message, if sent via the plug (e.g.
                from another hook), equivalent to ``msg`` if the source is otherwise unknown.
            primary (bool):
                ``False`` for supplementary messages if the source message required multiple raw
                messages in order to represent it (e.g. messages with multiple attachments where
                the underlying network doesn't support it), otherwise ``True``.

        Returns:
            (.Channel, .Message) tuple:
                The augmented or replacement pair, or ``None`` to suppress this message.
        """
        return (channel, msg)

    async def process(self, channel, msg, source, primary):
        """
        Handle an incoming message received by any plug.

        Args:
            channel (.Channel):
                Original source of this message.
            msg (.Message):
                Raw message received from another plug.
            source (.Message):
                Original message data used to generate the raw message, if sent via the plug (e.g.
                from another hook), equivalent to ``msg`` if the source is otherwise unknown.
            primary (bool):
                ``False`` for supplementary messages if the source message required multiple raw
                messages in order to represent it (e.g. messages with multiple attachments where
                the underlying network doesn't support it), otherwise ``True``.
        """

    def __repr__(self):
        return "<{}: {}>".format(self.__class__.__name__, self.name)


class ResourceHook(Hook):
    """
    Variant of hooks that globally provide access to some resource.

    Only one of each class may be loaded, which happens before regular hooks, and such hooks are
    keyed by their class rather than a name, allowing for easier lookups.
    """
