from asyncio import Condition
from enum import Enum
from importlib import import_module


def resolve_import(path):
    """
    Take the qualified name of a Python class, and return the physical class object.

    Args:
        path (str):
            Dotted Python class name, e.g. ``<module path>.<class name>``.

    Returns:
        type:
            Class object imported from module.
    """
    module, class_ = path.rsplit(".", 1)
    return getattr(import_module(module), class_)


def pretty_str(cls):
    """
    Class decorator to provide a default :meth:`__str__` based on the contents of :attr:`__dict__`.
    """

    def nest_str(obj):
        if isinstance(obj, dict):
            return "{{...{}...}}".format(len(obj)) if obj else "{}"
        elif isinstance(obj, list):
            return "[...{}...]".format(len(obj)) if obj else "[]"
        else:
            return str(obj)

    def __str__(self):
        args = "\n".join("{}: {}".format(k, nest_str(v).replace("\n", "\n" + " " * (len(k) + 2)))
                         for k, v in self.__dict__.items() if not k.startswith("_"))
        return "[{}]\n{}".format(self.__class__.__name__, args)

    cls.__str__ = __str__
    return cls


def config_props(plugs=False, channels=False, hooks=False):
    """
    Callable class decorator to add :attr:`plugs`, :attr:`channels` and :attr:`hooks` helper
    properties.  Works with :class:`.Hook` or :class:`.Plug` to read the corresponding config key
    and look up the referenced objects by label.
    """

    def inner(cls):
        if plugs:
            def plug_getter(self):
                return [self.host.plugs[label] for label in self.config["plugs"]]
            cls.plugs = property(plug_getter)
        if channels:
            def channel_getter(self):
                return [self.host.channels[label] for label in self.config["channels"]]
            cls.channels = property(channel_getter)
        if hooks:
            def hook_getter(self):
                return [self.host.hooks[label] for label in self.config["hooks"]]
            cls.hooks = property(hook_getter)
        return cls

    return inner


class OpenState(Enum):
    """
    Readiness status for instances of :class:`Openable`.
    """
    inactive = 0
    starting = 1
    active = 2
    stopping = 3


class Openable:
    """
    Abstract class to provide open and close hooks.  Subclasses should implement :meth:`start`
    and :meth:`stop`, whilst users should make use of :meth:`open` and :meth:`close`.

    Attributes:
        state (.OpenState):
            Current status of this resource.
    """

    def __init__(self):
        self.state = OpenState.inactive
        self._changing = Condition()

    async def open(self):
        """
        Open this resource ready for use.  Does nothing if already open, but raises
        :class:`RuntimeError` if currently changing state.
        """
        if self.state == OpenState.active:
            return
        elif self.state != OpenState.inactive:
            raise RuntimeError("Can't open when already closing")
        self.state = OpenState.starting
        await self.start()
        self.state = OpenState.active

    async def start(self):
        """
        Perform any operations needed to open this resource.
        """

    async def close(self):
        """
        Close this resource after it's used.  Does nothing if already closed, but raises
        :class:`RuntimeError` if currently changing state.
        """
        if self.state == OpenState.inactive:
            return
        elif self.state != OpenState.active:
            raise RuntimeError("Can't close when already opening/closing")
        self.state = OpenState.stopping
        await self.stop()
        self.state = OpenState.inactive

    async def stop(self):
        """
        Perform any operations needed to close this resource.
        """
