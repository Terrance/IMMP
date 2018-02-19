import code
from collections import deque
import logging

from voluptuous import ALLOW_EXTRA, Any, Optional, Schema

import imirror


try:
    import ptpython.repl
except ImportError:
    ptpython = None

try:
    import aioconsole
except ImportError:
    aioconsole = None


log = logging.getLogger(__name__)


class _Schema(object):

    config_shell = Schema({Optional("all", default=False): bool,
                           Optional("console", default=None): Any("ptpython", None)},
                          extra=ALLOW_EXTRA, required=True)

    config_async = Schema({"port": int,
                           Optional("buffer", default=None): Any(int, None)},
                          extra=ALLOW_EXTRA, required=True)


class ShellReceiver(imirror.Receiver):
    """
    A receiver to start a Python shell when a message is received.

    .. warning::
        The console will block all other running tasks; notably, all transports will be unable to
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
        config = _Schema.config_shell(config)
        self.all = config["all"]
        if config["console"] == "ptpython":
            if ptpython:
                log.debug("Using ptpython console")
                self.console = self._ptpython
            else:
                raise imirror.TransportError("'ptpython' module not installed")
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

    .. warning::
        The console will be accessible on a locally bound port without authentication.  Do not use
        on a shared or untrusted system, as the host and all connected transports are exposed.

    Config:
        port (int):
            Port to bind the console on.  Once running, one can connect using e.g. netcat.  See
            `aioconsole's docs <https://aioconsole.readthedocs.io/en/latest/#serving-the-console>`_
            for more info.
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
        config = _Schema.config_async(config)
        if not aioconsole:
            raise imirror.TransportError("'aioconsole' module not installed")
        self.port = config["port"]
        self.buffer = deque(maxlen=config["buffer"])
        self._server = None

    @property
    def last(self):
        return self.buffer[-1]

    async def start(self):
        await super().start()
        log.debug("Launching console on port {}".format(self.port))
        self._server = await aioconsole.start_interactive_server(factory=self._factory,
                                                                 host="localhost", port=self.port)

    async def stop(self):
        await super().stop()
        if self._server:
            log.debug("Stopping console server")
            self._server.close()
            self._server = None

    def _factory(self, streams=None):
        return aioconsole.AsynchronousConsole(locals={"host": self.host, "shell": self},
                                              streams=streams)

    async def process(self, channel, msg):
        self.buffer.append((channel, msg))
