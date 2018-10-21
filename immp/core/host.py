from asyncio import CancelledError, ensure_future, gather, wait
from itertools import chain
import logging

from .error import ConfigError
from .hook import ResourceHook
from .plug import PlugStream
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
        self._loaded = False
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

    def add_channel(self, name, channel):
        """
        Register a channel to the host.  The channel's plug must be registered first.

        Args:
            name (str):
                Unique identifier for this channel to be referenced by plugs and hooks.
            channel (.Channel):
                Existing channel instance to add.
        """
        if name in self.channels:
            raise ConfigError("Channel name '{}' already registered".format(name))
        if channel.plug.name not in self.plugs:
            raise ConfigError("Channel plug '{}' not yet registered"
                              .format(channel.plug.name))
        log.debug("Adding channel: {} ({}/{})"
                  .format(name, channel.plug.name, channel.source))
        self.channels[name] = channel

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

    def loaded(self):
        """
        Trigger the on-load event for all plugs and hooks.
        """
        for hook in self.resources.values():
            hook.on_load()
        for plug in self.plugs.values():
            plug.on_load()
        for hook in self.hooks.values():
            hook.on_load()
        self._loaded = True

    async def open(self):
        """
        Connect all open plugs and start all hooks.
        """
        if not self._loaded:
            raise RuntimeError("On-load event must be sent before opening")
        if self.resources:
            await wait([hook.open() for hook in self.resources.values()])
        if self.plugs:
            await wait([plug.open() for plug in self.plugs.values()])
        if self.hooks:
            await wait([hook.open() for hook in self.hooks.values()])

    async def close(self):
        """
        Disconnect all open plugs and stop all hooks.
        """
        if self.hooks:
            await wait([hook.close() for hook in self.hooks.values()])
        if self.plugs:
            await wait([plug.close() for plug in self.plugs.values()])
        if self.resources:
            await wait([hook.close() for hook in self.resources.values()])

    async def _safe_receive(self, hook, sent, source, primary):
        try:
            await hook.on_receive(sent, source, primary)
        except Exception:
            log.exception("Hook '{}' failed on-receive event".format(hook.name))

    async def _callback(self, sent, source, primary):
        hooks = [hook for hook in chain(self.resources.values(), self.hooks.values())
                 if hook.state == OpenState.active]
        for hook in hooks:
            try:
                result = await hook.before_receive(sent, source, primary)
            except Exception:
                log.exception("Hook '{}' failed before-receive event".format(hook.name))
                continue
            if result:
                sent = result
            else:
                # Message has been suppressed by a hook.
                return
        await gather(*(self._safe_receive(hook, sent, source, primary) for hook in hooks))

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
        if not self._loaded:
            self.loaded()
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
