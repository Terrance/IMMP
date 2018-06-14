"""
For testing, a plug with no external network.

Messages with sequential identifiers are created and returned every 10 seconds.  Any messages sent
to the plug are cloned and returned as if a network had processed them.
"""

from asyncio import ensure_future, sleep
from itertools import count
import logging

import immp


log = logging.getLogger(__name__)


class DummyPlug(immp.Plug):
    """
    Test plug that yields a message every 10 seconds.
    """

    network_name = "Dummy"
    network_id = "dummy"

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        self.counter = count()
        self.user = immp.User(id="dummy", real_name=name)
        self.channel = immp.Channel(self, "dummy")
        self._task = None

    async def start(self):
        self._task = ensure_future(self._timer())

    async def stop(self):
        if self._task:
            self._task.cancel()
            self._task = None

    async def put(self, channel, msg):
        # Make a clone of the message to echo back out of the generator.
        clone = immp.Message(id=next(self.counter),
                             at=msg.at,
                             original=msg.original,
                             text=msg.text,
                             user=msg.user,
                             action=msg.action,
                             deleted=msg.deleted,
                             raw=msg.raw)
        log.debug("Returning message: {}".format(repr(clone)))
        self.queue(self.channel, clone)
        # Don't return the clone ID, let it be delivered as a new message.
        return []

    async def _timer(self):
        while True:
            await sleep(10)
            log.debug("Creating next test message")
            self.queue(self.channel,
                       immp.Message(id=next(self.counter),
                                    text="Test",
                                    user=self.user))
