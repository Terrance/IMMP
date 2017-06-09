import asyncio
import logging

from aiostream import stream

from .channel import Channel
from .error import ConfigError
from .receiver import Receiver
from .transport import Transport
from .util import resolve_import, Base


log = logging.getLogger(__name__)


class Host(Base):
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

    def add_transport(self, name, path, config):
        """
        Register a new transport, along with its associated config.

        Args:
            name (str):
                User-provided, unique name of the transport, used for config references.
            path (str):
                Python dotted name of the form ``<module name>.<class name>``, representing the
                selected transport.
            config (dict):
                Reference to the user-provided configuration.
        """
        if name in self.transports:
            raise ConfigError("Transport name '{}' already registered".format(name))
        try:
            cls = resolve_import(path)
        except ImportError as e:
            raise ConfigError("Error trying to import transport class '{}'".format(path)) from e
        if not issubclass(cls, Transport):
            raise ConfigError("Transport class '{}' not a valid subclass".format(path))
        log.debug("Adding transport: {} ({})".format(name, path))
        transport = cls(name, config, self)
        self.transports[name] = transport

    def remove_transport(self, name):
        """
        Unregister an existing transport.

        Args:
            name (str):
                Name of a previously registered transport instance to disconnect and stop tracking.
        """
        try:
            transport = self.transports[name]
        except KeyError:
            raise RuntimeError("Transport '{}' not registered to host".format(name)) from None
        if transport.connected:
            raise RuntimeError("Transport '{}' still connected".format(name))
        del self.transports[name]

    def add_channel(self, name, transport, source):
        """
        Register a new channel.

        Args:
            name (str):
                User-provided, unique name of the transport, used for config references.
            transport (str):
                Name of the transport that provides this channel.
            source (str):
                Transport-specific channel identifier.
        """
        if name in self.channels:
            raise ConfigError("Channel name '{}' already registered".format(name))
        try:
            transport = self.transports[transport]
        except KeyError:
            raise ConfigError("Channel transport '{}' not registered".format(name))
        log.debug("Adding channel: {} ({} -> {})".format(name, transport.name, source))
        self.channels[name] = Channel(name, transport, source)

    def remove_channel(self, name):
        """
        Unregister an existing channel.

        Args:
            name (str):
                Name of a previously registered channel.
        """
        try:
            del self.channels[name]
        except KeyError:
            raise RuntimeError("Channel '{}' not added to host".format(name)) from None

    def add_receiver(self, name, path, config):
        """
        Register a new receiver, along with its associated config.

        Args:
            name (str):
                User-provided, unique name of the receiver, used for config references.
            path (str):
                Python dotted name of the form ``<module name>.<class name>``, representing the
                selected receiver.
            config (dict):
                Reference to the user-provided configuration.
        """
        if name in self.receivers:
            raise ConfigError("Receiver name '{}' already registered".format(name))
        try:
            cls = resolve_import(path)
        except ImportError:
            raise ConfigError("Error trying to import receiver class '{}'".format(path))
        if not issubclass(cls, Receiver):
            raise ConfigError("Receiver class '{}' not a valid subclass".format(path))
        log.debug("Adding receiver: {} ({})".format(name, path))
        receiver = cls(name, config, self)
        self.receivers[name] = receiver

    def remove_receiver(self, name):
        """
        Unregister an existing receiver.

        Args:
            receiver (.Receiver):
                Name of a previously registered receiver instance to stop using.
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
        if not self.transports:
            log.warn("No transports registered")
        if not self.receivers:
            log.warn("No receivers registered")
        await asyncio.wait([transport.connect() for transport in self.transports.values()])
        self.running = True
        receivers = (transport.receive() for transport in self.transports.values())
        async with stream.merge(*receivers).stream() as streamer:
            async for msg in streamer:
                log.debug("Received message: {}".format(msg))
                await asyncio.wait([receiver.process(msg) for receiver in self.receivers.values()])

    async def close(self):
        """
        Disconnect all open transports.
        """
        await asyncio.wait([transport.disconnect() for transport in self.transports.values()])
