from asyncio import Condition
from enum import Enum
from functools import reduce
from importlib import import_module
import re
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


def _no_escape(char):
    # Fail a match if the next character is escaped by a backslash.
    return r"(?<!\\)(?:\\\\)*{}".format(char)


def escape(raw, *chars):
    """
    Prefix special characters with backslashes, suitable for encoding in a larger string
    delimiting those characters.

    Args:
        raw (str):
            Unsafe input string.
        chars (str list):
            Control characters to be escaped.

    Returns:
        str:
            Escaped input string.
    """
    args = (raw, r"\\", *chars)
    return reduce(lambda current, char: current.replace(char, r"\{}".format(char)), args)


def unescape(raw, *chars):
    """
    Inverse of :func:`escape`, remove backslashes escaping special characters.

    Args:
        raw (str):
            Escaped input string.
        chars (str list):
            Control characters to be unescaped.

    Returns:
        str:
            Raw unescaped string.
    """
    args = (raw, *chars, r"\\")
    return reduce(lambda current, char: re.sub(_no_escape(r"\\{}".format(char)),
                                               char, current), args)


class ConfigProperty:
    """
    Data descriptor to present config from :class:`.Openable` instances using the actual objects
    stored in a :class:`.Host`.
    """

    def __init__(self, cls=None, key=None):
        self._cls = cls
        self._key = key

    def __set_name__(self, owner, name):
        if not self._key:
            self._key = name.lstrip("_")

    @classmethod
    def _describe(cls, spec):
        if isinstance(spec, list):
            return "[{}]".format(", ".join(cls._describe(inner) for inner in spec))
        elif isinstance(spec, tuple):
            return "{{{}}}".format(", ".join(inner.__name__ for inner in spec))
        else:
            return spec.__name__

    def _from_host(self, instance, name, cls=None):
        try:
            obj = instance.host[name]
        except KeyError:
            raise ConfigError("No object {} on host".format(repr(name))) from None
        if cls and not isinstance(obj, cls):
            raise ConfigError("Reference {} not instance of {}"
                              .format(repr(name), self._describe(cls)))
        else:
            return obj

    def __get__(self, instance, owner):
        if not instance:
            return self
        value = instance.config.get(self._key)
        if not value:
            return None
        if isinstance(self._cls, list):
            obj = tuple(self._from_host(instance, name, tuple(self._cls)) for name in value)
        else:
            obj = self._from_host(instance, value, self._cls)
        return obj

    def __repr__(self):
        return "<{}: {}{}>".format(self.__class__.__name__, repr(self._key),
                                   " {}".format(self._describe(self._cls)) if self._cls else "")


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
