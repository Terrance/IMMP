from asyncio import wait, ensure_future, CancelledError
import logging

from .error import ConfigError
from .transport import Channel, TransportStream
from .util import pretty_str


log = logging.getLogger(__name__)


@pretty_str
class Host:
    """
    Main class responsible for starting, stopping, and interacting with transports.

    To run as the main coroutine for an application, use :meth:`run` in conjunction with
    :func:`.AbstractEventLoop.run_until_complete`.  To stop a running host in an async context,
    await :meth:`quit`.

    For finer control, you can call :meth:`open`, :meth:`process` and :meth:`close` explicitly.

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
        self._stream = self._process = None

    @property
    def running(self):
        # This is the "public" status that external code can query.
        return self._stream is not None

    def add_transport(self, transport):
        """
        Register a transport to the host.

        If the host is already running, use :meth:`join_transport` instead, which will start the
        transport and begin processing messages straight away.

        Args:
            transport (.Transport):
                Existing transport instance to add.
        """
        if self._stream:
            raise RuntimeError("Host is already running, use join_transport() instead")
        if transport.name in self.transports:
            raise ConfigError("Transport name '{}' already registered".format(transport.name))
        log.debug("Adding transport: {}".format(transport.name))
        self.transports[transport.name] = transport

    async def join_transport(self, transport):
        """
        Register a transport to the host, and connect it if the host is running.

        Args:
            transport (.Transport):
                Existing transport instance to add.
        """
        if transport.name in self.transports:
            raise ConfigError("Transport name '{}' already registered".format(transport.name))
        log.debug("Joining transport: {}".format(transport.name))
        self.transports[transport.name] = transport
        if self._stream:
            await transport.open()
            self._stream.add(transport)

    def remove_transport(self, name):
        """
        Unregister an existing transport.

        .. warning::
            This will not notify any receivers with a reference to this transport, nor will it
            attempt to remove it from their state.

        Args:
            name (str):
                Name of a previously registered transport instance to remove.
        """
        try:
            transport = self.transports[name]
        except KeyError:
            raise RuntimeError("Transport '{}' not registered to host".format(name)) from None
        if self._stream and self._stream.has(transport):
            raise RuntimeError("Host and transport are still running")
        del self.transports[name]

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

    async def open(self):
        """
        Connect all open transports and start all receivers.
        """
        await wait([transport.open() for transport in self.transports.values()])
        await wait([receiver.open() for receiver in self.receivers.values()])

    async def close(self):
        """
        Disconnect all open transports and stop all receivers.
        """
        await wait([receiver.close() for receiver in self.receivers.values()])
        await wait([transport.close() for transport in self.transports.values()])

    async def _callback(self, channel, msg):
        await wait([receiver.process(channel, msg) for receiver in self.receivers.values()])

    async def process(self):
        """
        Retrieve messages from transports, and distribute them to receivers.
        """
        if self._stream:
            raise RuntimeError("Host is already processing")
        self._stream = TransportStream(self._callback, *self.transports.values())
        try:
            await self._stream.process()
        finally:
            self._stream = None

    async def run(self):
        """
        Main entry point for running a host as a full application.  Opens all transports and
        receivers, blocks (when awaited) for the duration of :meth:`process`, and closes all
        openables during shutdown.
        """
        if self._process:
            raise RuntimeError("Host is already running")
        await self.open()
        try:
            self._process = ensure_future(self.process())
            await self._process
        except CancelledError:
            log.debug("Host run cancelled")
        finally:
            self._process = None
            await self.close()

    async def quit(self):
        """
        Request the running host to stop processing.  This only works if started via :meth:`run`.
        """
        if self._process:
            self._process.cancel()
