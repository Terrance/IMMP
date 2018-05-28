"""
Interact with and debug a running app in the console.

Synchronous
~~~~~~~~~~~

Config:
    all (bool):
        ``True`` to process any message, ``False`` (default) to restrict to defined channels.
    console (str):
        Use a different embedded console.  By default, :meth:`code.interact` is used, but set this
        to ``ptpython`` (requires the `ptpython <https://github.com/jonathanslenders/ptpython>`_
        Python module) for a more functional shell.

When a new message is received, a console will launch in the terminal where your app is running.
The variables :data:`channel` and :data:`msg` are defined in the local scope, whilst :data:`self`
refers to the shell hook itself.

.. warning::
    The console will block all other running tasks; notably, all plugs will be unable to make any
    progress whilst the console is open.

Asynchronous
~~~~~~~~~~~~

Config:
    port (int):
        Port to bind the console on.  Once running, one can connect using e.g. netcat.  See
        `aioconsole's docs <https://aioconsole.readthedocs.io/en/latest/#serving-the-console>`_
        for more info.
    buffer (int):
        Number of received messages to keep at any one time (default: no limit).  When a new
        message comes in and the queue is full, the oldest message will be discarded.

At startup, a console will be launched on the given port.  You can connect to it from a separate
terminal, for example::

    $ rlwrap nc localhost $PORT

Use of ``rlwrap`` provides you with readline-style keybinds, such as ↑ and ↓ to navigate through
previous commands.  The variables :data:`shell` and :data:`host` are defined, refering to the shell
hook and the running :class:`.Host` respectively.  This hook also maintains a cache of messages as
they're received, accessible via :attr:`.AsyncShellHook.buffer`.

.. note::
    This hook requires the `aioconsole <https://aioconsole.readthedocs.io>`_ Python module.

.. warning::
    The console will be accessible on a locally bound port without authentication.  Do not use on
    shared or untrusted systems, as the host and all connected plugs are exposed.
"""

import code
from collections import deque
import logging

from voluptuous import ALLOW_EXTRA, Any, Optional, Schema

import immp


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


class ShellHook(immp.ResourceHook):
    """
    Hook to start a Python shell when a message is received.
    """

    def __init__(self, name, config, host):
        super().__init__(name, _Schema.config_shell(config), host)
        if self.config["console"] == "ptpython":
            if ptpython:
                log.debug("Using ptpython console")
                self.console = self._ptpython
            else:
                raise immp.PlugError("'ptpython' module not installed")
        else:
            log.debug("Using native console")
            self.console = self._code

    def _ptpython(self, loc, glob):
        ptpython.repl.embed(glob, loc)

    def _code(self, loc, glob):
        code.interact(local=dict(glob, **loc))

    async def process(self, channel, msg):
        if channel in self.host.channels or self.config["all"]:
            log.debug("Entering console: {}".format(repr(msg)))
            self.console(locals(), globals())


class AsyncShellHook(immp.ResourceHook):
    """
    Hook to launch an asynchonous console alongside a :class:`.Host` instance.

    Attributes:
        buffer (collections.deque):
            Queue of recent messages, the length defined by the ``buffer`` config entry.
        last ((.Channel, .Message) tuple):
            Most recent message received from a connected plug.
    """

    def __init__(self, name, config, host):
        super().__init__(name, _Schema.config_async(config), host)
        if not aioconsole:
            raise immp.PlugError("'aioconsole' module not installed")
        self.buffer = deque(maxlen=self.config["buffer"])
        self._server = None

    @property
    def last(self):
        return self.buffer[-1]

    async def start(self):
        await super().start()
        log.debug("Launching console on port {}".format(self.config["port"]))
        self._server = await aioconsole.start_interactive_server(factory=self._factory,
                                                                 host="localhost",
                                                                 port=self.config["port"])

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