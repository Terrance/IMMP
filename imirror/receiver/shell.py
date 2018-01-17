import code
from collections import deque
import logging

try:
    import ptpython.repl
except ImportError:
    ptpython = None

try:
    import aioconsole
except ImportError:
    aioconsole = None

import imirror


log = logging.getLogger(__name__)


class ShellReceiver(imirror.Receiver):
    """
    A receiver to start a Python shell when a message is received.

    Note that the console will block all other tasks; notably, all transports will be unable to
    make any progress whilst the console is open.  See :class:`.AsyncShellReceiver` for an
    alternative solution.

    Config:
        all (bool):
            ``True`` to process any message, ``False`` (default) to restrict to defined channels.
        console (str):
            Use a different embedded console.  By default, :meth:`code.interact` is used, but set
            this to ``ptpython`` for an alternative shell (requires the :mod:`ptpython` module).
    """

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        self.all = bool(config.get("all"))
        console = config.get("console")
        if console == "ptpython":
            if ptpython:
                log.debug("Using ptpython console")
                self.console = self._ptpython
            else:
                raise imirror.TransportError("'ptpython' module not installed")
        elif console:
            raise imirror.ConfigError("Unknown console type '{}'".format(console))
        else:
            log.debug("Using native console")
            self.console = self._code

    def _ptpython(self, loc, glob):
        ptpython.repl.embed(glob, loc)

    def _code(self, loc, glob):
        code.interact(local=dict(glob, **loc))

    async def process(self, channel, msg):
        if channel in self.host.channels or self.all:
            log.debug("Entering console: {}".format(repr(msg)))
            self.console(locals(), globals())


class AsyncShellReceiver(imirror.Receiver):
    """
    A receiver to launch an asynchonous console alongside a host instance (requires the
    :mod:`aioconsole` module).  The console exposes the running :class:`.Host` instance as
    ``host``, and the current shell receiver as ``shell``.

    For example, to retrieve the most recent message and the channel where it appeared::

        >>> channel, msg = shell.last

    Config:
        port (int):
            Port to bind the console on.  Once running, one can connect using e.g. netcat.  The
            `aioconsole's docs <https://aioconsole.readthedocs.io/en/latest/#serving-the-console>`_ for more info.
        buffer (int):
            Number of received messages to keep at any one time (default: no limit).  When a new
            message comes in and the queue is full, the oldest message will be discarded.

    Attributes:
        buffer (collections.deque):
            Queue of recent messages, the length defined by the ``buffer`` config entry.
        last ((.Channel, .Message) tuple):
            Most recent message received from a connected transport.
    """

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        if not aioconsole:
            raise imirror.TransportError("'aioconsole' module not installed")
        self.port = int(config["port"])
        maxlen = int(config.get("buffer")) if "buffer" in config else None
        self.buffer = deque(maxlen=maxlen)

    @property
    def last(self):
        return self.buffer[-1]

    async def start(self):
        log.debug("Launching console on port {}".format(self.port))
        await aioconsole.start_interactive_server(factory=self._factory,
                                                  host="localhost", port=self.port)

    def _factory(self, streams=None):
        return aioconsole.AsynchronousConsole(locals={"host": self.host, "shell": self},
                                              streams=streams)

    async def process(self, channel, msg):
        self.buffer.append((channel, msg))
