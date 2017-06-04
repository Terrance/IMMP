from .error import TransportError
from .util import Base


class Transport(Base):
    """
    Base of all transport classes, handles communication with an external network by converting
    outside data into standardised message objects, and pushing new messages into the network.

    Attributes:
        name (str):
            User-provided, unique name of the transport, used for config references.
        config (dict):
            Reference to the user-provided configuration.
        host (.Host):
            Controlling host instance, providing access to transports.
        connected (bool):
            Whether this transport currently maintains a connection to the external network.
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
        self.connected = False

    async def connect(self):
        """
        Start a connection to the external network.
        """
        self.connected = True

    async def disconnect(self):
        """
        Terminate the external network connection.
        """
        self.connected = False

    async def send(self, channel, msg):
        """
        Take a :class:`.Message` object, and push it to the underlying network.  The message should
        have its ID updated to match the server.

        Args:
            channel (.Channel):
                Target channel for the new message.
            msg (.Message):
                Original message received from another channel or transport.
        """
        if not self.connected:
            raise TransportError("Can't send messages when not connected")

    async def receive(self):
        """
        Generator of :class:`.Message` objects from the underlying network.
        """
        if not self.connected:
            raise TransportError("Can't receive messages when not connected")
