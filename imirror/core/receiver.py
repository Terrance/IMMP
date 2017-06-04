from .util import Base


class Receiver(Base):
    """
    Base of all receiver classes, performs any form of processing on messages from all connected
    transports, via the provided host instance.

    Attributes:
        name (str):
            User-provided, unique name of the receiver, used for config references.
        config (dict):
            Reference to the user-provided configuration.
        host (.Host):
            Controlling host instance, providing access to transports.
    """

    def __init__(self, name, config, host):
        """
        Process the user-provided configuration.  May raise :cls:`.ConfigError` if invalid.

        Args:
            name (str)
            config (dict)
            host (.Host)
        """
        self.name = name
        self.config = config
        self.host = host

    async def process(self, msg):
        """
        Handle an incoming message from the host.

        Args:
            msg (.Message):
                Original message received from another transport.
        """
