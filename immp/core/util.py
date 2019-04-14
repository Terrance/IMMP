from asyncio import Condition
from enum import Enum
from importlib import import_module
import time

from .error import ConfigError


class IDGen:
    """
    Generator of generic timestamp-based identifiers.

    IDs are guaranteed unique for the lifetime of the application -- two successive calls will
    yield two different identifiers.
    """

    def __init__(self):
        self.last = 0

    def __call__(self):
        """
        Make a new identifier.

        Returns:
            str:
                Newly generated identifier.
        """
        new = max(self.last + 1, int(time.time()))
        self.last = new
        return str(new)

    def __repr__(self):
        return "<{}: {} -> {}>".format(self.__class__.__name__, self.last, self())


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


class ConfigProperty:
    """
    Data descriptor to present config from :class:`.Openable` instances using the actual objects
    stored in a :class:`.Host`.

    This class should be subclassed for each type maintained by the host, and the resulting
    subclasses may then be used by plugs and hooks using those config types.
    """

    def __init__(self, name, key=None, attr=None):
        self._name = name
        self._key = key or "{}s".format(name)
        self._attr = attr or self._key

    def __get__(self, instance, owner):
        if not instance:
            return self
        objs = getattr(instance.host, self._attr)
        try:
            return tuple(objs[label] for label in instance.config[self._key])
        except KeyError as e:
            raise ConfigError("No {} {} on host".format(self._name, repr(e.args[0]))) from None

    def __set__(self, instance, value):
        # Replace a list of object labels in a config dict according to the given objects.
        objs = getattr(instance.host, self._attr)
        labels = []
        for val in value:
            for label, obj in objs.items():
                if val == obj:
                    labels.append(label)
                    break
            else:
                raise ConfigError("{} {} not registered to host"
                                  .format(self._name.title(), repr(val)))
        # Update the config list in place, in case anyone has a reference to it.
        instance.config[self._key].clear()
        instance.config[self._key].extend(labels)

    def __repr__(self):
        return "<{}: {}>".format(self.__class__.__name__, repr(self._name))


class SingleConfigProperty:
    """
    Data descriptor to retrieve a single object based on a config key.
    """

    def __init__(self, key, cls=None):
        self._key = key
        self._cls = cls

    def __get__(self, instance, owner):
        if not instance:
            return self
        if not instance.config.get(self._key):
            return None
        try:
            obj = instance.host[instance.config[self._key]]
        except KeyError:
            raise ConfigError("No object {} on host".format(repr(self._key))) from None
        if self._cls and not isinstance(obj, self._cls):
            raise ConfigError("Reference {} not instance of {}"
                              .format(repr(self._key), self._cls.__name__))
        else:
            return obj

    def __repr__(self):
        return "<{}: {}{}>".format(self.__class__.__name__, repr(self._key),
                                   " {}".format(self._cls.__name__) if self._cls else "")


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
