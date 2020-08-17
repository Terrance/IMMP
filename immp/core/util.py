from asyncio import Condition
from collections.abc import MutableMapping, MutableSequence
from enum import Enum
from functools import partial, reduce, wraps
from importlib import import_module
import logging
import re
import time

try:
    from aiohttp import ClientSession
except ImportError:
    ClientSession = None

from .error import ConfigError
from .schema import Schema


log = logging.getLogger(__name__)


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
        if hasattr(self, "__dict__"):
            data = self.__dict__
        elif hasattr(self, "__slots__"):
            data = {attr: getattr(self, attr) for attr in self.__slots__}
        else:
            raise TypeError("No __dict__ or __slots__ to collect attributes")
        args = "\n".join("{}: {}".format(k, nest_str(v).replace("\n", "\n" + " " * (len(k) + 2)))
                         for k, v in data.items() if not k.startswith("_"))
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


class Watchable:
    """
    Container mixin to trigger a callback when its contents are changed.
    """

    _callback = None

    def __init__(self, watch):
        self._callback = watch

    def __call__(self):
        return self._callback()

    def _wrap_inline(self, obj):
        if isinstance(obj, MutableMapping):
            obj.update((key, self._wrap(value)) for key, value in obj.items())
        elif isinstance(obj, MutableSequence):
            obj[:] = (self._wrap(value) for value in obj)
        else:
            raise TypeError

    def _wrap(self, obj):
        if isinstance(obj, Watchable):
            return obj
        elif isinstance(obj, MutableMapping):
            return WatchedDict(self, {key: self._wrap(value) for key, value in obj.items()})
        elif isinstance(obj, MutableSequence):
            return WatchedList(self, [self._wrap(item) for item in obj])
        else:
            return obj

    @classmethod
    def unwrap(cls, obj):
        """
        Recursively replace :class:`Watchable` subclasses with their native equivalents.

        Args:
            obj:
                Container type, or any value.

        Returns:
            Unwrapped container, or the original value.
        """
        if isinstance(obj, MutableMapping):
            return {key: cls.unwrap(value) for key, value in obj.items()}
        elif isinstance(obj, MutableSequence):
            return [cls.unwrap(item) for item in obj]
        else:
            return obj

    @classmethod
    def _watch(cls, target, name):
        method = getattr(target, name)
        @wraps(method)
        def wrapped(self, *args, **kwargs):
            out = method(self, *args, **kwargs)
            if self._callback:
                self._callback()
            return out
        setattr(target, name, wrapped)

    @classmethod
    def watch(cls, *methods):
        """
        Class decorator for mixin users, to wrap methods that modify the underlying container, and
        should therefore trigger the callback when invoked.

        Args:
            methods (str list):
                List of method names to individually wrap.
        """
        def inner(target):
            for name in methods:
                cls._watch(target, name)
            return target
        return inner


@Watchable.watch("__setitem__", "__delitem__", "setdefault", "update", "pop", "popitem", "clear")
class WatchedDict(dict, Watchable):
    """
    Watchable-enabled :class:`dict` subclass.  Lists or dictionaries added as items in this
    container will be wrapped automatically.
    """

    def __init__(self, watch, initial, **kwargs):
        super().__init__(initial, **kwargs)
        self._wrap_inline(self)
        Watchable.__init__(self, watch)

    def __repr__(self):
        return "<{}: {}>".format(self.__class__.__name__, super().__repr__())

    def __setitem__(self, key, value):
        return super().__setitem__(key, self._wrap(value))

    def update(self, other=None, **kwargs):
        self._wrap_inline(kwargs)
        return super().update(self._wrap(other) if other else (), **kwargs)


@Watchable.watch("__setitem__", "__delitem__", "__iadd__", "__imul__",
                 "insert", "append", "extend", "pop", "remove", "clear", "reverse", "sort")
class WatchedList(list, Watchable):
    """
    Watchable-enabled :class:`list` subclass.  Lists or dictionaries added as items in this
    container will be wrapped automatically.
    """

    def __init__(self, watch, initial):
        super().__init__(initial)
        self._wrap_inline(self)
        Watchable.__init__(self, watch)

    def __repr__(self):
        return "<{}: {}>".format(self.__class__.__name__, super().__repr__())

    def __setitem__(self, key, value):
        return super().__setitem__(key, self._wrap(value))

    def insert(self, index, value):
        return super().insert(index, self._wrap(value))

    def append(self, value):
        return super().append(self._wrap(value))

    def extend(self, other):
        return super().extend([self._wrap(item) for item in other])


class ConfigProperty:
    """
    Data descriptor to present config from :class:`.Openable` instances using the actual objects
    stored in a :class:`.Host`.
    """

    __slots__ = ("_cls", "_key")

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


class IDGen:
    """
    Generator of generic timestamp-based identifiers.

    IDs are guaranteed unique for the lifetime of the application -- two successive calls will
    yield two different identifiers.
    """

    __slots__ = ("last",)

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


class LocalFilter(logging.Filter):
    """
    Logging filter that restricts capture to loggers within the ``immp`` namespace.
    """

    def filter(self, record):
        return record.name == "__main__" or record.name.split(".", 1)[0] == "immp"


class OpenState(Enum):
    """
    Readiness status for instances of :class:`Openable`.

    Attributes:
        disabled:
            Not currently in use, and won't be started by the host.
        inactive:
            Hasn't been started yet.
        starting:
            Currently starting up (during :meth:`.Openable.start`).
        active:
            Currently running.
        stopping:
            Currently closing down (during :meth:`.Openable.stop`).
        failed:
            Exception occurred during starting or stopping phases.
    """
    disabled = -1
    inactive = 0
    starting = 1
    active = 2
    stopping = 3
    failed = 4


class Configurable:
    """
    Superclass for objects managed by a :class:`.Host` and created using configuration.

    Attributes:
        schema (.Schema):
            Structure of the config expected by this configurable.  If not customised by the
            subclass, it defaults to ``dict`` (that is, any :class:`dict` structure is valid).

            It may also be set to :data:`None`, to declare that no configuration is accepted.
        name (str):
            User-provided, unique name of the hook, used for config references.
        config (dict):
            Reference to the user-provided configuration.  Assigning to this field will validate
            the data against the class schema, which may raise exceptions on failure as defined by
            :meth:`.Schema.validate`.
        host (.Host):
            Controlling host instance, providing access to plugs.
    """

    schema = Schema(dict)

    __slots__ = ("name", "_config", "host")

    def __init__(self, name, config, host):
        super().__init__()
        self.name = name
        self._config = None
        self.config = config
        self.host = host

    def _callback(self):
        log.debug("Triggering config change event for %r", self)
        self.host.config_change(self)

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, value):
        if self.schema:
            old = self._config
            self._config = WatchedDict(self._callback, self.schema(value))
            if old:
                self._callback()
        elif value:
            raise TypeError("{} doesn't accept configuration".format(self.__class__.__name__))
        else:
            self._config = None


class Openable:
    """
    Abstract class to provide open and close hooks.  Subclasses should implement :meth:`start`
    and :meth:`stop`, whilst users should make use of :meth:`open` and :meth:`close`.

    Attributes:
        state (.OpenState):
            Current status of this resource.
    """

    state = property(lambda self: self._state)

    def __init__(self):
        super().__init__()
        self._state = OpenState.inactive
        self._changing = Condition()

    async def open(self):
        """
        Open this resource ready for use.  Does nothing if already open, but raises
        :class:`RuntimeError` if currently changing state.
        """
        if self._state == OpenState.active:
            return
        elif self._state not in (OpenState.inactive, OpenState.failed):
            raise RuntimeError("Can't open when already opening/closing")
        self._state = OpenState.starting
        try:
            await self.start()
        except Exception:
            self._state = OpenState.failed
            raise
        else:
            self._state = OpenState.active

    async def start(self):
        """
        Perform any operations needed to open this resource.
        """

    async def close(self):
        """
        Close this resource after it's used.  Does nothing if already closed, but raises
        :class:`RuntimeError` if currently changing state.
        """
        if self._state == OpenState.inactive:
            return
        elif self._state != OpenState.active:
            raise RuntimeError("Can't close when already opening/closing")
        self._state = OpenState.stopping
        try:
            await self.stop()
        except Exception:
            self._state = OpenState.failed
            raise
        else:
            self._state = OpenState.inactive

    async def stop(self):
        """
        Perform any operations needed to close this resource.
        """

    def disable(self):
        """
        Prevent this openable from being run by the host.
        """
        if self._state == OpenState.disabled:
            return
        elif self._state in (OpenState.inactive, OpenState.failed):
            self._state = OpenState.disabled
        else:
            raise RuntimeError("Can't disable when currently running")

    def enable(self):
        """
        Restore normal operation of this openable.
        """
        if self._state == OpenState.disabled:
            self._state = OpenState.inactive


class HTTPOpenable(Openable):
    """
    Template openable including a :class:`aiohttp.ClientSession` instance for networking.

    Attributes:
        session (aiohttp.ClientSession):
            Managed session object.
    """

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        self.session = None

    async def start(self):
        await super().start()
        if not ClientSession:
            raise ConfigError("'aiohttp' module not installed")
        self.session = ClientSession()

    async def stop(self):
        await super().stop()
        if self.session:
            log.debug("Closing session")
            await self.session.close()
            self.session = None
