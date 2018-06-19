from asyncio import Condition
from enum import Enum
from importlib import import_module

from .error import ConfigError


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


def _make_config_prop(field):

    def config_get(self):
        """
        Read a list of object labels from a config dict, and return the corresponding objects.
        """
        objs = getattr(self.host, field)
        try:
            return tuple(objs[label] for label in self.config[field])
        except KeyError as e:
            raise ConfigError("No {} {} on host".format(field[:-1], repr(e.args[0]))) from None

    def config_set(self, value):
        """
        Replace a list of object labels in a config dict according to the given objects.
        """
        objs = getattr(self.host, field)
        labels = []
        for val in value:
            for label, obj in objs.items():
                if val == obj:
                    labels.append(label)
                    break
            else:
                raise ConfigError("{} {} not registered to host"
                                  .format(field.title()[:-1], repr(val)))
        # Update the config list in place, in case anyone has a reference to it.
        self.config[field].clear()
        self.config[field].extend(labels)

    return property(config_get, config_set)


def config_props(*fields):
    """
    Callable class decorator to add :attr:`plugs`, :attr:`channels` and :attr:`hooks` helper
    properties.  Works with :class:`.Hook` or :class:`.Plug` to read the corresponding config key
    and look up the referenced objects by label.

    Args:
        fields (str list):
            Can be one or more of ``plugs``, ``channels``, ``hooks``.
    """
    for field in fields:
        if field not in ("plugs", "channels", "hooks"):
            raise KeyError(field)

    def inner(cls):
        for field in fields:
            setattr(cls, field, _make_config_prop(field))
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
