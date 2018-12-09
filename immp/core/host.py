from asyncio import CancelledError, ensure_future, gather, wait
from itertools import chain
import logging

from .error import ConfigError
from .hook import ResourceHook
from .stream import PlugStream
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
        self.groups = {}
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

        Returns:
            str:
                Name used to reference this plug.
        """
        if plug.name in self.plugs:
            raise ConfigError("Plug name '{}' already registered".format(plug.name))
        log.debug("Adding plug: {} ({})".format(plug.name, plug.__class__.__name__))
        self.plugs[plug.name] = plug
        if self._loaded:
            plug.on_load()
        if self._stream:
            self._stream.add(plug)
        return plug.name

    def remove_plug(self, name):
        """
        Unregister an existing plug.

        .. warning::
            This will not notify any hooks with a reference to this plug, nor will it attempt to
            remove it from their state.

        Args:
            name (str):
                Name of a previously registered plug instance to remove.

        Returns:
            .Plug:
                Removed plug instance.
        """
        if name not in self.plugs:
            raise RuntimeError("Plug '{}' not registered to host".format(name))
        log.debug("Removing plug: {}".format(name))
        plug = self.plugs.pop(name)
        for name, channel in list(self.channels.items()):
            if channel.plug == plug:
                self.remove_channel(name)
        if self._stream:
            self._stream.remove(plug)
        return plug

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
        if name not in self.channels:
            raise RuntimeError("Channel '{}' not registered to host".format(name))
        log.debug("Removing channel: {}".format(name))
        return self.channels.pop(name)

    def add_group(self, group):
        """
        Register a group to the host.

        Args:
            group (.Group):
                Existing group instance to add.

        Returns:
            str:
                Name used to reference this group.
        """
        if group.name in self.groups:
            raise ConfigError("Group name '{}' already registered".format(group.name))
        log.debug("Adding group: {}".format(group.name))
        self.groups[group.name] = group
        return group.name

    def remove_group(self, name):
        """
        Unregister an existing group.

        .. warning::
            This will not notify any hooks with a reference to this group, nor will it attempt to
            remove it from their state.

        Args:
            name (str):
                Name of a previously registered group instance to remove.

        Returns:
            .Group:
                Removed group instance.
        """
        if name not in self.groups:
            raise RuntimeError("Group '{}' not registered to host".format(name))
        log.debug("Removing group: {}".format(name))
        return self.groups.pop(name)

    def add_hook(self, hook):
        """
        Register a hook to the host.

        Args:
            hook (.Hook):
                Existing hook instance to add.

        Returns:
            str:
                Name used to reference this hook.
        """
        if hook.name in self.hooks:
            raise ConfigError("Hook name '{}' already registered".format(hook.name))
        for cls, resource in self.resources.items():
            if hook.name == resource.name:
                raise ConfigError("Resource name '{}' already registered".format(hook.name))
            elif hook.__class__ == cls:
                raise ConfigError("Resource class '{}' already registered".format(cls.__name__))
        if isinstance(hook, ResourceHook):
            log.debug("Adding resource: {} ({})".format(hook.name, hook.__class__.__name__))
            self.resources[hook.__class__] = hook
        else:
            log.debug("Adding hook: {} ({})".format(hook.name, hook.__class__.__name__))
            self.hooks[hook.name] = hook
        if self._loaded:
            hook.on_load()
        return hook.name

    def remove_hook(self, name):
        """
        Unregister an existing hook.

        .. warning::
            This will not notify any dependent hooks with a reference to this hook, nor will it
            attempt to remove it from their state.

        Args:
            name (str):
                Name of a previously registered hook instance to remove.

        Returns:
            .Hook:
                Removed hook instance.
        """
        if name in self.hooks:
            log.debug("Removing hook: {}".format(name))
            return self.hooks.pop(name)
        else:
            for cls, hook in list(self.resources.items()):
                if hook.name == name:
                    log.debug("Removing resource: {} ({})".format(name, cls.__name__))
                    return self.resources.pop(cls)
            else:
                raise RuntimeError("Hook '{}' not registered to host".format(name))

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
        self._stream = PlugStream()
        self._stream.add(*self.plugs.values())
        try:
            async for sent, source, primary in self._stream:
                await self._callback(sent, source, primary)
        except StopAsyncIteration:
            log.debug("Plug stream finished")
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
