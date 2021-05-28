"""
For testing, a plug with no external network.

Messages with sequential identifiers are created and posted to the ``dummy`` channel every 10
seconds.  Any messages sent to the plug are echoed to this channel as if a network had itself
processed them.
"""

from asyncio import ensure_future, sleep
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
        self.counter = immp.IDGen()
        self.user = immp.User(id_="dummy", real_name=name)
        self.channel = immp.Channel(self, "dummy")
        self._task = None

    async def start(self):
        await super().start()
        self._task = ensure_future(self._timer())

    async def stop(self):
        await super().stop()
        if self._task:
            self._task.cancel()
            self._task = None

    async def put(self, channel, msg):
        # Make a clone of the message to echo back out of the generator.
        clone = immp.SentMessage(id_=self.counter(),
                                 channel=self.channel,
                                 text=msg.text,
                                 user=msg.user,
                                 action=msg.action)
        log.debug("Returning message: %r", clone)
        self.queue(clone)
        return [clone]

    async def _timer(self):
        while True:
            await sleep(10)
            log.debug("Creating next test message")
            self.queue(immp.SentMessage(id_=self.counter(),
                                        channel=self.channel,
                                        text="Test",
                                        user=self.user))
