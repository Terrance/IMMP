from asyncio import CancelledError, Task, ensure_future, gather, wait
from collections import defaultdict
from itertools import chain
import logging
from operator import attrgetter

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
        priority ((str, int) dict):
            Mapping from hook names to their configured ordering.  Unordered (lowest priority)
            hooks are not present.
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
    resources = property(lambda self: dict(self._resources))
    priority = property(lambda self: dict(self._priority))

    @property
    def plain_hooks(self):
        return {name: hook for name, hook in self.hooks.items()
                if not isinstance(hook, ResourceHook)}

    def ordered_hooks(self):
        """
        Sort all registered hooks by priority.

        Returns:
            (.Hook set) list:
                Hooks grouped by their relative order -- one set for each ascending priority,
                followed by unordered hooks at the end.
        """
        prioritised = defaultdict(set)
        rest = set()
        for hook in chain(self._resources.values(), self.plain_hooks.values()):
            if hook.state != OpenState.active:
                continue
            try:
                prioritised[self._priority[hook.name]].add(hook)
            except KeyError:
                rest.add(hook)
        ordered = [hooks for _, hooks in sorted(prioritised.items())]
        if rest:
            ordered.append(rest)
        return ordered

    def __contains__(self, key):
        return key in self._objects

    def __getitem__(self, key):
        if isinstance(key, str):
            return self._objects[key]
        elif isinstance(key, type) and issubclass(key, ResourceHook):
            return self._resources[key]
        else:
            raise TypeError(key)

    @property
    def running(self):
        # This is the "public" status that external code can query.
        return self._stream is not None

    def add_plug(self, plug, enabled=True):
        """
        Register a plug to the host.

        If the host is already running, use :meth:`join_plug` instead, which will start the plug
        and begin processing messages straight away.

        Args:
            plug (.Plug):
                Existing plug instance to add.
            enabled (bool):
                ``True`` to connect this plug at startup.

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
        if enabled:
            if self._loaded:
                plug.on_load()
            if self._stream:
                self._stream.add(plug)
        else:
            plug.disable()
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
        if name not in self.channels:
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
        if name not in self.groups:
            raise RuntimeError("Group '{}' not registered to host".format(name))
        log.info("Removing group: %s", name)
        return self._objects.pop(name)

    def add_hook(self, hook, enabled=True, priority=None):
        """
        Register a hook to the host.

        Args:
            hook (.Hook):
                Existing hook instance to add.
            enabled (bool):
                ``True`` to connect this plug at startup.
            priority (int):
                Optional ordering constraint, applied to send and receive events.  Hooks registered
                with a priority will be processed in ascending priority order, followed by those
                without prioritisation.  Where multiple hooks share a priority value, events may be
                processed in parallel (e.g. on-receive is dispatched to all hooks simultaneously).

        Returns:
            str:
                Name used to reference this hook.
        """
        if not isinstance(hook, Hook):
            raise TypeError(hook)
        elif hook.name in self._objects:
            raise ConfigError("Hook name '{}' already registered".format(hook.name))
        elif hook.__class__ in self._resources:
            raise ConfigError("Resource class '{}' already registered"
                              .format(hook.__class__.__name__))
        elif priority is not None and priority in self._priority:
            raise ConfigError("Priority {} already registered".format(priority))
        log.info("Adding hook: %r (%s)", hook.name, hook.__class__.__name__)
        self._objects[hook.name] = hook
        if isinstance(hook, ResourceHook):
            log.info("Adding resource: %r (%s)", hook.name, hook.__class__.__name__)
            mro = hook.__class__.__mro__
            subclass = mro[mro.index(ResourceHook) - 1]
            self._resources[subclass] = hook
        if priority is not None:
            self._priority[hook.name] = priority
        if enabled:
            if self._loaded:
                hook.on_load()
        else:
            hook.disable()
        return hook.name

    def prioritise_hook(self, name, priority):
        """
        Re-prioritise an existing hook.

        Args:
            name (str):
                Name of a previously registered hook instance to prioritise.
            priority (int):
                Optional ordering constraint -- see :meth:`add_hook`.
        """
        if name not in self.hooks:
            raise RuntimeError("Hook '{}' not registered to host".format(name))
        if priority is None:
            self._priority.pop(name, None)
        elif isinstance(priority, int) and priority >= 1:
            self._priority[name] = priority
        else:
            raise ValueError("Hook priority must be a positive integer")

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
        if name not in self.hooks:
            raise RuntimeError("Hook '{}' not registered to host".format(name))
        self._priority.pop(name, None)
        log.info("Removing hook: %s", name)
        hook = self._objects.pop(name)
        if isinstance(hook, ResourceHook):
            log.info("Removing resource: %r (%s)", name, hook.__class__.__name__)
            del self._resources[hook.__class__]
        return hook

    def loaded(self):
        """
        Trigger the on-load event for all plugs and hooks.
        """
        for hook in self._resources.values():
            hook.on_load()
        for plug in self.plugs.values():
            plug.on_load()
        for hook in self.plain_hooks.values():
            hook.on_load()
        self._loaded = True

    async def _try_state(self, state, objs, timeout=None):
        objs = [obj for obj in objs if obj.state != OpenState.disabled]
        if not objs:
            return
        action = "open" if state == OpenState.active else "close"
        getter = attrgetter(action)
        tasks = {Task(getter(obj)()): obj for obj in objs}
        done, pending = await wait(tasks.keys(), timeout=timeout)
        for task in done:
            exc = task.exception()
            if exc:
                obj = tasks[task]
                log.error("Failed to %s %r", action, obj.name, exc_info=exc)
        for task in pending:
            obj = tasks[task]
            log.warning("Failed to %s %r after %s seconds", action, obj.name, timeout)

    async def open(self):
        """
        Connect all open plugs and start all hooks.
        """
        if not self._loaded:
            raise RuntimeError("On-load event must be sent before opening")
        log.debug("Opening resources")
        await self._try_state(OpenState.active, self._resources.values(), 30)
        log.debug("Opening plugs")
        await self._try_state(OpenState.active, self.plugs.values(), 30)
        log.debug("Opening remaining hooks")
        await self._try_state(OpenState.active, self.plain_hooks.values(), 30)

    async def close(self):
        """
        Disconnect all open plugs and stop all hooks.
        """
        log.debug("Closing non-resource hooks")
        await self._try_state(OpenState.inactive, self.plain_hooks.values(), 30)
        log.debug("Closing plugs")
        await self._try_state(OpenState.inactive, self.plugs.values(), 30)
        log.debug("Closing resources")
        await self._try_state(OpenState.inactive, self._resources.values(), 30)

    async def _safe_receive(self, hook, sent, source, primary):
        try:
            await hook.on_receive(sent, source, primary)
        except Exception:
            log.exception("Hook %r failed on-receive event", hook.name)

    async def _callback(self, sent, source, primary):
        ordered = self.ordered_hooks()
        for hooks in ordered:
            for hook in hooks:
                try:
                    result = await hook.before_receive(sent, source, primary)
                except Exception:
                    log.exception("Hook %r failed before-receive event", hook.name)
                    continue
                if result:
                    if sent is source:
                        source = result
                    sent = result
                else:
                    # Message has been suppressed by a hook.
                    return
        for hooks in ordered:
            await gather(*(self._safe_receive(hook, sent, source, primary) for hook in hooks))

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

    def config_change(self, source):
        """
        Event handler called from a configurable when its config changes, dispatched to all hooks.

        Args:
            source (.Configurable):
                Source plug or hook that triggered the event.
        """
        for hook in self.hooks.values():
            hook.on_config_change(source)

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
                                                    len(self._resources), len(self.plain_hooks),
                                                    " running" if self.running else "")
