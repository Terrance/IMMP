from asyncio import wait
import logging

from aiostream import stream

from .error import ConfigError
from .transport import Channel
from .util import pretty_str


log = logging.getLogger(__name__)


@pretty_str
class Host:
    """
    Main class responsible for starting, stopping, and interacting with transports.

    Attributes:
        transports ((str, .Transport) dict):
            Collection of all registered transport instances, keyed by name.
        receivers ((str, .Receiver) dict):
            Collection of all registered message receivers, keyed by name.
        running (bool):
            Whether messages from transports are being processed by the host.
    """

    def __init__(self):
        self.transports = {}
        self.channels = {}
        self.receivers = {}
        self.running = False

    def add_transport(self, transport):
        """
        Register a transport to the host.

        Args:
            transport (.Transport):
                Existing transport instance to add.
        """
        if transport.name in self.transports:
            raise ConfigError("Transport name '{}' already registered".format(transport.name))
        log.debug("Adding transport: {}".format(transport.name))
        self.transports[transport.name] = transport

    def remove_transport(self, name):
        """
        Unregister an existing transport.

        Args:
            name (str):
                Name of a previously registered transport instance to remove.
        """
        try:
            del self.transports[name]
        except KeyError:
            raise RuntimeError("Transport '{}' not registered to host".format(name)) from None

    def add_channel(self, channel):
        """
        Register a channel to the host.  The channel's transport must be registered first.

        Args:
            channel (.Channel):
                Existing channel instance to add.
        """
        if channel.name in self.channels:
            raise ConfigError("Channel name '{}' already registered".format(channel.name))
        if channel.transport.name not in self.transports:
            raise ConfigError("Channel transport '{}' not yet registered"
                              .format(channel.transport.name))
        log.debug("Adding channel: {} ({}/{})"
                  .format(channel.name, channel.transport.name, channel.source))
        self.channels[channel.name] = channel

    def remove_channel(self, name):
        """
        Unregister an existing channel.

        Args:
            name (str):
                Name of a previously registered channel instance to remove.
        """
        try:
            del self.channels[name]
        except KeyError:
            raise RuntimeError("Channel '{}' not registered to host".format(name)) from None

    def add_receiver(self, receiver):
        """
        Register a receiver to the host.

        Args:
            receiver (.Receiver):
                Existing receiver instance to add.
        """
        if receiver.name in self.receivers:
            raise ConfigError("Receiver name '{}' already registered".format(receiver.name))
        log.debug("Adding receiver: {}".format(receiver.name))
        self.receivers[receiver.name] = receiver

    def remove_receiver(self, name):
        """
        Unregister an existing receiver.

        Args:
            name (str):
                Name of a previously registered receiver instance to remove.
        """
        try:
            del self.receivers[name]
        except KeyError:
            raise RuntimeError("Receiver '{}' not registered to host".format(name)) from None

    def resolve_channel(self, transport, source):
        """
        Take a transport and channel name, and resolve it from the configured channels.

        Args:
            transport (.Transport):
                Registered transport instance.
            source (str):
                Transport-specific channel identifier.

        Returns:
            .Channel:
                Generated channel container object.
        """
        for channel in self.channels.values():
            if channel.transport == transport and channel.source == source:
                return channel
        log.debug("Channel transport/source not found: {}/{}".format(transport.name, source))
        return Channel(None, transport, source)

    async def run(self):
        """
        Connect all transports, and distribute messages to receivers.
        """
        if self.transports:
            log.debug("Connecting transports")
            await wait([transport.connect() for transport in self.transports.values()])
        else:
            log.warn("No transports registered")
        if self.receivers:
            log.debug("Starting receivers")
            await wait([receiver.start() for receiver in self.receivers.values()])
        else:
            log.warn("No receivers registered")
        self.running = True
        getters = (transport.receive() for transport in self.transports.values())
        async with stream.merge(*getters).stream() as streamer:
            async for channel, msg in streamer:
                log.debug("Received: {} {}".format(repr(channel), repr(msg)))
                await wait([receiver.process(channel, msg)
                            for receiver in self.receivers.values()])

    async def close(self):
        """
        Disconnect all open transports.
        """
        await wait([receiver.stop() for receiver in self.receivers.values()])
        await wait([transport.disconnect() for transport in self.transports.values()])
