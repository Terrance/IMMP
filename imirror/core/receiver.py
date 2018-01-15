from .util import Base


class Receiver(Base):
    """
    Base of all receiver classes, performs any form of processing on messages from all connected
    transports, via the provided host instance.

    Instantiation may raise :class:`.ConfigError` if the provided configuration is invalid.

    Attributes:
        name (str):
            User-provided, unique name of the receiver, used for config references.
        config (dict):
            Reference to the user-provided configuration.
        host (.Host):
            Controlling host instance, providing access to transports.
    """

    def __init__(self, name, config, host):
        self.name = name
        self.config = config
        self.host = host

    async def process(self, channel, msg):
        """
        Handle an incoming message from the host.

        Args:
            channel (.Channel):
                Original source of this message.
            msg (.Message):
                Original message received from another transport.
        """

    def __repr__(self):
        return "<{}: {}>".format(self.__class__.__name__, self.name)
