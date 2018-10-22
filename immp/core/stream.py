from asyncio import CancelledError, ensure_future, wait
import logging

from .util import OpenState


log = logging.getLogger(__name__)


class PlugStream:
    """
    Manager for reading from multiple asynchronous generators in parallel.

    Requires a callback coroutine that accepts (:class:`.Channel`, :class:`.Message`) arguments.
    """

    def __init__(self, callback, *plugs):
        self._coros = {}
        self._plugs = {}
        self.callback = callback
        self.add(*plugs)

    def _queue(self, plug):
        # Poor man's async iteration -- there's no async equivalent to next(gen).
        log.debug("Queueing receive task for plug '{}'".format(plug.name))
        self._plugs[ensure_future(self._coros[plug].asend(None))] = plug

    def add(self, *plugs):
        """
        Register plugs for reading.  Plugs should be opened prior to registration.

        Args:
            plugs (.Plug list):
                Connected plug instances to register.
        """
        for plug in plugs:
            if not plug.state == OpenState.active:
                raise RuntimeError("Plug '{}' is not open".format(plug.name))
            self._coros[plug] = plug.receive()
            self._queue(plug)

    def has(self, plug):
        """
        Check for the existence of a plug in the manager.

        Args:
            plug (.Plug):
                Connected plug instance to check.

        Returns:
            bool:
                ``True`` if a :meth:`.Plug.receive` call is still active.
        """
        return (plug in self._coros)

    async def _wait(self):
        done, pending = await wait(list(self._plugs.keys()), return_when="FIRST_COMPLETED")
        for task in done:
            plug = self._plugs[task]
            try:
                sent, source, primary = task.result()
            except StopAsyncIteration:
                log.debug("Plug '{}' finished yielding during process".format(plug.name))
                del self._coros[plug]
            except Exception:
                log.exception("Plug '{}' raised error during process".format(plug.name))
                del self._coros[plug]
            else:
                log.debug("Received: {}".format(repr(sent)))
                await self.callback(sent, source, primary)
                self._queue(plug)
            finally:
                del self._plugs[task]

    async def process(self):
        """
        Retrieve messages from plugs, and distribute them to hooks.
        """
        while self._plugs:
            try:
                log.debug("Waiting for next message")
                await self._wait()
            except CancelledError:
                log.debug("Host process cancelled, propagating to tasks")
                for task in self._plugs.keys():
                    task.cancel()
                log.debug("Resuming tasks to collect final messages")
                await self._wait()
        log.debug("All tasks completed")

    def __repr__(self):
        return "<{}: {} tasks>".format(self.__class__.__name__, len(self._coros))
