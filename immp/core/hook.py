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
