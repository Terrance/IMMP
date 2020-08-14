from asyncio import FIRST_COMPLETED, CancelledError, Event, ensure_future, wait
import logging


log = logging.getLogger(__name__)


class PlugStream:
    """
    Message multiplexer, to read messages from multiple asynchronous generators in parallel.

    Instances of this class are async-iterable -- for each incoming message, a tuple is produced:
    the physical message received by the plug, a source message if originating from within the
    system, and a primary flag to indicate supplementary messages created when a system-sourced
    message can't be represented in a single plug message.

    .. warning::
        As per :meth:`.Plug.stream`, only one iterator of this class should be used at once.

    Yields:
        (.SentMessage, .Message, bool) tuple:
            Messages received and processed by any connected plug.
    """

    __slots__ = ("_agens", "_tasks", "_sync", "_close")

    def __init__(self):
        # Mapping from plugs to their stream() generator coroutines.
        self._agens = {}
        # Mapping from coroutine task wrappers back to their tasks.
        self._tasks = {}
        # When a plug is added or removed, the stream wouldn't be able to update until an event
        # arrives.  This is a signal used to recreate the task list without a message.
        self._sync = Event()
        # Generators can't be closed synchronously, schedule the plugs to be done on the next sync.
        self._close = set()

    def add(self, *plugs):
        """
        Connect plugs to the stream.  When the stream is active, their :meth:`.Plug.stream`
        methods will be called to start collecting queued messages.

        Args:
            plugs (.Plug list):
                New plugs to merge in.
        """
        for plug in plugs:
            if plug not in self._agens:
                self._agens[plug] = plug.stream()
        self._sync.set()

    def remove(self, *plugs):
        """
        Disconnect plugs from the stream.  Their :meth:`.Plug.stream` tasks will be cancelled, and
        any last messages will be collected before removing.

        Args:
            plugs (.Plug list):
                Active plugs to remove.
        """
        for plug in plugs:
            if plug in self._agens:
                self._close.add(plug)
        self._sync.set()

    async def _queue(self):
        for plug, coro in self._agens.items():
            if plug not in self._tasks.values():
                log.debug("Queueing receive task for plug %r", plug.name)
                # Poor man's async iteration -- there's no async equivalent to next(gen).
                self._tasks[ensure_future(coro.asend(None))] = plug
        for task, plug in list(self._tasks.items()):
            if plug not in self._agens and plug is not self._sync:
                task.cancel()
                self._close.add(plug)
                del self._tasks[task]
        for plug in self._close:
            log.debug("Cancelling receive task for plug %r", plug.name)
            await self._agens[plug].aclose()
        self._close.clear()
        if self._sync not in self._tasks.values():
            log.debug("Recreating sync task")
            self._sync.clear()
            self._tasks[ensure_future(self._sync.wait())] = self._sync

    async def __aiter__(self):
        log.info("Ready for first message")
        while True:
            try:
                await self._queue()
                done, _ = await wait(self._tasks, return_when=FIRST_COMPLETED)
            except (GeneratorExit, CancelledError):
                for task in self._tasks:
                    task.cancel()
                return
            for task in done:
                plug = self._tasks.pop(task)
                if plug is self._sync:
                    continue
                try:
                    sent, source, primary = task.result()
                except CancelledError:
                    del self._agens[plug]
                except Exception:
                    log.warning("Generator for plug %r exited, recreating",
                                plug.name, exc_info=True)
                    self._agens[plug] = plug.stream()
                else:
                    log.info("Received message ID %r in channel %r%s",
                             sent.id, sent.channel, " (primary)" if primary else "")
                    log.debug("Message content: %r", sent)
                    if sent is not source:
                        log.debug("Source message: %r", source)
                    yield (sent, source, primary)
            log.debug("Waiting for next message")

    def __repr__(self):
        done = sum(1 for task in self._tasks if task.done())
        pending = len(self._tasks) - done
        return "<{}: {} done, {} pending>".format(self.__class__.__name__, done, pending)
