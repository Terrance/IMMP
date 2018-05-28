from asyncio import CancelledError, ensure_future, wait
from itertools import chain
import logging

from .error import ConfigError
from .hook import ResourceHook
from .plug import Channel, PlugStream
from .util import OpenState, pretty_str


log = logging.getLogger(__name__)


@pretty_str
class Host:
    """
    Main class responsible for starting, stopping, and interacting with plugs.

    To run as the main coroutine for an application, use :meth:`run` in conjunction with
    :meth:`.AbstractEventLoop.run_until_complete`.  To stop a running host in an async context,
    await :meth:`quit`.

    For finer control, you can call :meth:`open`, :meth:`process` and :meth:`close` explicitly.

    Attributes:
        plugs ((str, .Plug) dict):
            Collection of all registered plug instances, keyed by name.
        hooks ((str, .Hook) dict):
            Collection of all registered message hooks, keyed by name.
        resources ((class, .ResourceHook) dict):
            Collection of all registered resource hooks, keyed by class.
        running (bool):
            Whether messages from plugs are being processed by the host.
    """

    def __init__(self):
        self.plugs = {}
        self.channels = {}
        self.hooks = {}
        self.resources = {}
        self._stream = self._process = None

    @property
    def running(self):
        # This is the "public" status that external code can query.
        return self._stream is not None

    def add_plug(self, plug):
        """
        Register a plug to the host.

        If the host is already running, use :meth:`join_plug` instead, which will start the plug
        and begin processing messages straight away.

        Args:
            plug (.Plug):
                Existing plug instance to add.
        """
        if self._stream:
            raise RuntimeError("Host is already running, use join_plug() instead")
        if plug.name in self.plugs:
            raise ConfigError("Plug name '{}' already registered".format(plug.name))
        log.debug("Adding plug: {}".format(plug.name))
        self.plugs[plug.name] = plug

    async def join_plug(self, plug):
        """
        Register a plug to the host, and connect it if the host is running.

        Args:
            plug (.Plug):
                Existing plug instance to add.
        """
        if plug.name in self.plugs:
            raise ConfigError("Plug name '{}' already registered".format(plug.name))
        log.debug("Joining plug: {}".format(plug.name))
        self.plugs[plug.name] = plug
        if self._stream:
            await plug.open()
            self._stream.add(plug)

    def remove_plug(self, name):
        """
        Unregister an existing plug.

        .. warning::
            This will not notify any hooks with a reference to this plug, nor will it attempt to
            remove it from their state.

        Args:
            name (str):
                Name of a previously registered plug instance to remove.
        """
        try:
            plug = self.plugs[name]
        except KeyError:
            raise RuntimeError("Plug '{}' not registered to host".format(name)) from None
        if self._stream and self._stream.has(plug):
            raise RuntimeError("Host and plug are still running")
        del self.plugs[name]

    def add_channel(self, channel):
        """
        Register a channel to the host.  The channel's plug must be registered first.

        Args:
            channel (.Channel):
                Existing channel instance to add.
        """
        if channel.name in self.channels:
            raise ConfigError("Channel name '{}' already registered".format(channel.name))
        if channel.plug.name not in self.plugs:
            raise ConfigError("Channel plug '{}' not yet registered"
                              .format(channel.plug.name))
        log.debug("Adding channel: {} ({}/{})"
                  .format(channel.name, channel.plug.name, channel.source))
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

    def add_hook(self, hook):
        """
        Register a hook to the host.

        Args:
            hook (.Hook):
                Existing hook instance to add.
        """
        if isinstance(hook, ResourceHook):
            if any(issubclass(hook.__class__, cls) for cls in self.resources):
                raise ConfigError("Resource hook type '{}' or superclass already registered"
                                  .format(hook.__class__.__name__))
            log.debug("Adding resource hook: '{}' ({})"
                      .format(hook.name, hook.__class__.__name__))
            self.resources[hook.__class__] = hook
        else:
            if hook.name in self.hooks:
                raise ConfigError("Hook name '{}' already registered".format(hook.name))
            log.debug("Adding hook: {}".format(hook.name))
            self.hooks[hook.name] = hook

    def remove_hook(self, name):
        """
        Unregister an existing hook.

        .. warning::
            This will not notify any dependent hooks with a reference to this hook, nor will it
            attempt to remove it from their state.

        Args:
            name (str):
                Name of a previously registered hook instance to remove.
        """
        try:
            del self.hooks[name]
        except KeyError:
            remove = []
            for cls, hook in self.resources.values():
                if hook.name == name:
                    remove.append(cls)
            if not remove:
                raise RuntimeError("Hook '{}' not registered to host".format(name)) from None
            for cls in remove:
                del self.resources[cls]

    def resolve_channel(self, plug, source):
        """
        Take a plug and channel name, and resolve it from the configured channels.

        Args:
            plug (.Plug):
                Registered plug instance.
            source (str):
                Plug-specific channel identifier.

        Returns:
            .Channel:
                Generated channel container object.
        """
        for channel in self.channels.values():
            if channel.plug == plug and channel.source == source:
                return channel
        log.debug("Channel plug/source not found: {}/{}".format(plug.name, source))
        return Channel(None, plug, source)

    async def open(self):
        """
        Connect all open plugs and start all hooks.
        """
        await wait([plug.open() for plug in self.plugs.values()])
        await wait([hook.open() for hook in self.resources.values()])
        await wait([hook.open() for hook in self.hooks.values()])

    async def close(self):
        """
        Disconnect all open plugs and stop all hooks.
        """
        await wait([hook.close() for hook in self.hooks.values()])
        await wait([hook.close() for hook in self.resources.values()])
        await wait([plug.close() for plug in self.plugs.values()])

    async def _callback(self, channel, msg):
        for hook in chain(self.resources.values(), self.hooks.values()):
            if not hook.state == OpenState.active:
                continue
            result = await hook.preprocess(channel, msg)
            if result:
                channel, msg = result
            else:
                # Message has been suppressed by a hook.
                break
        else:
            await wait([hook.process(channel, msg) for hook in self.resources.values()
                        if hook.state == OpenState.active])
            await wait([hook.process(channel, msg) for hook in self.hooks.values()
                        if hook.state == OpenState.active])

    async def process(self):
        """
        Retrieve messages from plugs, and distribute them to hooks.
        """
        if self._stream:
            raise RuntimeError("Host is already processing")
        self._stream = PlugStream(self._callback, *self.plugs.values())
        try:
            await self._stream.process()
        finally:
            self._stream = None

    async def run(self):
        """
        Main entry point for running as a full application.  Opens all plugs and hooks, blocks
        (when awaited) for the duration of :meth:`process`, and closes openables during shutdown.
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