from asyncio import CancelledError, ensure_future, gather, wait
from itertools import chain
import logging

from .channel import Channel, Group
from .error import ConfigError
from .hook import Hook, ResourceHook
from .plug import Plug
from .stream import PlugStream
from .util import OpenState, pretty_str


log = logging.getLogger(__name__)


class HostGetter:
    """
    Filter property used to return a subset of objects by type.
    """

    def __init__(self, cls):
        self._cls = cls

    def __get__(self, instance, owner):
        if instance:
            return {name: obj for name, obj in instance._objects.items()
                    if isinstance(obj, self._cls)}
        else:
            return self


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
        channels ((str, .Channel) dict):
            Collection of all registered channels, keyed by name.
        groups ((str, .Group) dict):
            Collection of all registered group, keyed by name.
        hooks ((str, .Hook) dict):
            Collection of all registered hooks, keyed by name.
        plain_hooks ((str, .Hook) dict):
            As above, but excluding resources.
        resources ((class, .ResourceHook) dict):
            Collection of all registered resource hooks, keyed by class.
        running (bool):
            Whether messages from plugs are being processed by the host.
    """

    __slots__ = ("_objects", "_resources", "_priority", "_loaded", "_stream", "_process")

    def __init__(self):
        self._objects = {}
        self._resources = {}
        self._priority = {}
        self._loaded = False
        self._stream = self._process = None

    plugs = HostGetter(Plug)
    channels = HostGetter(Channel)
    groups = HostGetter(Group)
    hooks = HostGetter(Hook)
    resources = property(lambda self: self._resources)

    @property
    def plain_hooks(self):
        return {name: hook for name, hook in self.hooks.items()
                if not isinstance(hook, ResourceHook)}

    def ordered_hooks(self):
        """
        Sort all registered hooks by priority.

        Returns:
            (.Hook tuple, .Hook tuple) tuple:
                Prioritised and unsorted hooks.
        """
        ordered = tuple(hook for _, hook in sorted(self._priority.items())
                        if hook.state == OpenState.active)
        rest = tuple(hook for hook in chain(self.resources.values(), self.plain_hooks.values())
                     if hook not in ordered and hook.state == OpenState.active)
        return ordered, rest

    def __getitem__(self, key):
        if isinstance(key, str):
            return self._objects[key]
        elif isinstance(key, type) and issubclass(key, ResourceHook):
            return self.resources[key]
        else:
            raise TypeError(key)

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
        if not isinstance(plug, Plug):
            raise TypeError(plug)
        elif plug.name in self._objects:
            raise ConfigError("Plug name '{}' already registered".format(plug.name))
        log.info("Adding plug: %r (%s)", plug.name, plug.__class__.__name__)
        self._objects[plug.name] = plug
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
        if not isinstance(self._objects.get(name), Plug):
            raise RuntimeError("Plug '{}' not registered to host".format(name))
        log.info("Removing plug: %s", name)
        plug = self._objects.pop(name)
        for label, channel in list(self.channels.items()):
            if channel.plug == plug:
                self.remove_channel(label)
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
        if not isinstance(channel, Channel):
            raise TypeError(channel)
        elif name in self._objects:
            raise ConfigError("Channel name '{}' already registered".format(name))
        log.info("Adding channel: %r (%s/%s)", name, channel.plug.name, channel.source)
        self._objects[name] = channel

    def remove_channel(self, name):
        """
        Unregister an existing channel.

        Args:
            name (str):
                Name of a previously registered channel instance to remove.
        """
        if not isinstance(self._objects.get(name), Channel):
            raise RuntimeError("Channel '{}' not registered to host".format(name))
        log.info("Removing channel: %s", name)
        return self._objects.pop(name)

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
        if not isinstance(group, Group):
            raise TypeError(group)
        elif group.name in self._objects:
            raise ConfigError("Group name '{}' already registered".format(group.name))
        log.info("Adding group: %s", group.name)
        self._objects[group.name] = group
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
        if not isinstance(self._objects.get(name), Group):
            raise RuntimeError("Group '{}' not registered to host".format(name))
        log.info("Removing group: %s", name)
        return self._objects.pop(name)

    def add_hook(self, hook, priority=None):
        """
        Register a hook to the host.

        Args:
            hook (.Hook):
                Existing hook instance to add.
            priority (int):
                Optional ordering constraint, applied to send and receive events.  Hooks registered
                with a priority will be processed first in serial.  Those without will execute
                afterwards, in parallel (in the case of on-receive) or in registration order (for
                before-send and before-receive).

        Returns:
            str:
                Name used to reference this hook.
        """
        if not isinstance(hook, Hook):
            raise TypeError(hook)
        elif hook.name in self._objects:
            raise ConfigError("Hook name '{}' already registered".format(hook.name))
        elif hook.__class__ in self.resources:
            raise ConfigError("Resource class '{}' already registered"
                              .format(hook.__class__.__name__))
        elif priority is not None and priority in self._priority:
            raise ConfigError("Priority {} already registered".format(priority))
        log.info("Adding hook: %r (%s)", hook.name, hook.__class__.__name__)
        self._objects[hook.name] = hook
        if isinstance(hook, ResourceHook):
            log.info("Adding resource: %r (%s)", hook.name, hook.__class__.__name__)
            self.resources[hook.__class__] = hook
        if priority is not None:
            self._priority[priority] = hook
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
        for priority, hook in self._priority.items():
            if hook.name == name:
                del self._priority[priority]
                break
        if not isinstance(self._objects.get(name), Hook):
            raise RuntimeError("Hook '{}' not registered to host".format(name))
        log.info("Removing hook: %s", name)
        hook = self._objects.pop(name)
        if isinstance(hook, ResourceHook):
            log.info("Removing resource: %r (%s)", name, hook.__class__.__name__)
            del self.resources[hook.__class__]
        return hook

    def loaded(self):
        """
        Trigger the on-load event for all plugs and hooks.
        """
        for hook in self.resources.values():
            hook.on_load()
        for plug in self.plugs.values():
            plug.on_load()
        for hook in self.plain_hooks.values():
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
        if self.plain_hooks:
            await wait([hook.open() for hook in self.plain_hooks.values()])

    async def close(self):
        """
        Disconnect all open plugs and stop all hooks.
        """
        if self.plain_hooks:
            await wait([hook.close() for hook in self.plain_hooks.values()])
        if self.plugs:
            await wait([plug.close() for plug in self.plugs.values()])
        if self.resources:
            await wait([hook.close() for hook in self.resources.values()])

    async def _safe_receive(self, hook, sent, source, primary):
        try:
            await hook.on_receive(sent, source, primary)
        except Exception:
            log.exception("Hook %r failed on-receive event", hook.name)

    async def _callback(self, sent, source, primary):
        ordered, rest = self.ordered_hooks()
        for hook in ordered + rest:
            try:
                result = await hook.before_receive(sent, source, primary)
            except Exception:
                log.exception("Hook %r failed before-receive event", hook.name)
                continue
            if result:
                sent = result
            else:
                # Message has been suppressed by a hook.
                return
        for hook in ordered:
            await self._safe_receive(hook, sent, source, primary)
        if rest:
            await gather(*(self._safe_receive(hook, sent, source, primary) for hook in rest))

    async def channel_migrate(self, old, new):
        """
        Issue a migration call to all hooks.

        Args:
            old (.Channel):
                Existing channel with local data.
            new (.Channel):
                Target replacement channel to migrate data to.

        Returns:
            str list:
                Names of hooks that migrated any data for the requested channel.
        """
        hooks = list(self.hooks.values())
        if not hooks:
            return []
        results = await gather(*(hook.channel_migrate(old, new) for hook in hooks))
        return [hook.name for hook, result in zip(hooks, results) if result]

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

    def __repr__(self):
        return "<{}: {}P {}C {}G {}R {}H{}>".format(self.__class__.__name__, len(self.plugs),
                                                    len(self.channels), len(self.groups),
                                                    len(self.resources), len(self.plain_hooks),
                                                    " running" if self.running else "")
