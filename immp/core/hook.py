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

    async def preprocess(self, channel, msg):
        """
        Modify an incoming message before it's pushed to other hooks.  The (channel, message) pair
        must be returned, so hooks may modify in-place or return a different pair.  This method is
        called for each hook in turn, in registration order.

        Hooks may also suppress a message (e.g. if their actions caused it, but it bears no value
        to the rest of the system) by returning ``None``.

        Args:
            channel (.Channel):
                Original source of this message.
            msg (.Message):
                Original message received from another plug.

        Returns:
            (.Channel, .Message) tuple:
                The augmented or replacement pair, or ``None`` to suppress this message.
        """
        return (channel, msg)

    async def process(self, channel, msg):
        """
        Handle an incoming message from the host.

        Args:
            channel (.Channel):
                Original source of this message.
            msg (.Message):
                Original message received from another plug.
        """

    def __repr__(self):
        return "<{}: {}>".format(self.__class__.__name__, self.name)


class ResourceHook(Hook):
    """
    Variant of hooks that globally provide access to some resource.

    Only one of each class may be loaded, which happens before regular hooks, and such hooks are
    keyed by their class rather than a name, allowing for easier lookups.
    """
