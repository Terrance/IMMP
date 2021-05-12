"""
Interact with and debug a running app in the console.

Asynchronous
~~~~~~~~~~~~

Requirements:
    Extra name: ``console``

    `aioconsole <https://aioconsole.readthedocs.io>`_

Config:
    bind (int or str):
        TCP port or UNIX socket path to bind the console on.  See
        `aioconsole's docs <https://aioconsole.readthedocs.io/en/latest/#serving-the-console>`_
        for more info.
    buffer (int):
        Number of received messages to keep cached for later inspection.  If unset (default), no
        buffer will be available.  Otherwise, when a new message comes in and the queue is full,
        the oldest message will be discarded.  Set to ``0`` for an unlimited buffer, not
        recommended on production deployments.

At startup, a console will be launched on the given port or socket.  You can connect to it from a
separate terminal, for example with netcat for TCP::

    $ rlwrap nc localhost $PORT

Or socat for sockets::

    $ rlwrap socat $PATH -

.. tip::
    Use of ``rlwrap`` provides you with readline-style keybinds, such as ↑ and ↓ to navigate
    through previous commands, and Ctrl-R to search the command history.

The variables :data:`shell` and :data:`host` are defined, refering to the shell hook and the
running :class:`.Host` respectively.  This hook also maintains a cache of messages as they're
received, accessible via :attr:`.AsyncShellHook.buffer`.

.. warning::
    The console will be accessible on a locally bound port without authentication.  Do not use on
    shared or untrusted systems, as the host and all connected plugs are exposed.

Synchronous
~~~~~~~~~~~

.. deprecated:: 0.10.0
    Use the asynchronous shell with a buffer in order to interact with incoming messages.

Requirements:
    `ptpython <https://github.com/jonathanslenders/ptpython>`_:
        Can be used with ``console: ptpython`` as described below.

Config:
    all (bool):
        ``True`` to process any message, ``False`` (default) to restrict to defined channels.
    console (str):
        Use a different embedded console.  By default, :meth:`code.interact` is used, but set this
        to ``ptpython`` for a more functional shell.

When a new message is received, a console will launch in the terminal where your app is running.
The variables :data:`channel` and :data:`msg` are defined in the local scope, whilst :data:`self`
refers to the shell hook itself.

.. warning::
    The console will block all other running tasks; notably, all plugs will be unable to make any
    progress whilst the console is open.
"""

import code
from collections import deque
from functools import partial
import logging
from pprint import pformat
from warnings import warn

try:
    import ptpython.repl
except ImportError:
    ptpython = None

try:
    import aioconsole
except ImportError:
    aioconsole = None

import immp


log = logging.getLogger(__name__)


class ShellHook(immp.ResourceHook):
    """
    Hook to start a Python shell when a message is received.
    """

    schema = immp.Schema({immp.Optional("all", False): bool,
                          immp.Optional("console"): immp.Nullable("ptpython")})

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        warn("ShellHook is deprecated, migrate to AsyncShellHook", DeprecationWarning)
        if self.config["console"] == "ptpython":
            if ptpython:
                log.debug("Using ptpython console")
                self.console = self._ptpython
            else:
                raise immp.PlugError("'ptpython' module not installed")
        else:
            log.debug("Using native console")
            self.console = self._code

    @staticmethod
    def _ptpython(local):
        ptpython.repl.embed(globals(), local)

    @staticmethod
    def _code(local):
        code.interact(local=dict(globals(), **local))

    async def on_receive(self, sent, source, primary):
        await super().on_receive(sent, source, primary)
        if sent.channel in self.host.channels or self.config["all"]:
            log.debug("Entering console: %r", sent)
            self.console(locals())


class AsyncShellHook(immp.ResourceHook):
    """
    Hook to launch an asynchonous console alongside a :class:`.Host` instance.

    Attributes:
        buffer (collections.deque):
            Queue of recent messages, the length defined by the ``buffer`` config entry.
        last ((.SentMessage, .Message) tuple):
            Most recent message received from a connected plug.
    """

    schema = immp.Schema({"bind": immp.Any(str, int),
                          immp.Optional("buffer"): immp.Nullable(int)})

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        if not aioconsole:
            raise immp.PlugError("'aioconsole' module not installed")
        self.buffer = None
        self._server = None

    @property
    def last(self):
        return self.buffer[-1] if self.buffer else None

    async def start(self):
        await super().start()
        if self.config["buffer"] is not None:
            self.buffer = deque(maxlen=self.config["buffer"] or None)
        if isinstance(self.config["bind"], str):
            log.debug("Launching console on socket %s", self.config["bind"])
            bind = {"path": self.config["bind"]}
        else:
            log.debug("Launching console on port %d", self.config["bind"])
            bind = {"port": self.config["bind"]}
        self._server = await aioconsole.start_interactive_server(factory=self._factory, **bind)

    async def stop(self):
        await super().stop()
        self.buffer = None
        if self._server:
            log.debug("Stopping console server")
            self._server.close()
            self._server = None

    @staticmethod
    def _pprint(console, obj):
        console.print(pformat(obj))

    def _factory(self, streams=None):
        context = {"host": self.host, "shell": self, "immp": immp}
        console = aioconsole.AsynchronousConsole(locals=context, streams=streams)
        context["pprint"] = partial(self._pprint, console)
        log.debug("Accepted console connection")
        return console

    async def on_receive(self, sent, source, primary):
        await super().on_receive(sent, source, primary)
        if self.buffer is not None:
            self.buffer.append((sent, source))
