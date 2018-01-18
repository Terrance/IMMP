from asyncio import sleep
from itertools import count
import logging

from aiostream import stream

import imirror


log = logging.getLogger(__name__)


class DummyTransport(imirror.Transport):
    """
    A fake transport that just yields a message every 10 seconds.
    """

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        self.counter = count()
        self.user = imirror.User(id="dummy", real_name=name)

    async def put(self, channel, msg):
        # Make a clone of the message to echo back out of the generator.
        clone = imirror.Message(id=next(self.counter),
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

    async def _receive_timer(self):
        while True:
            await sleep(10)
            log.debug("Creating next test message")
            yield (self.channel,
                   imirror.Message(id=next(self.counter),
                                   text="Test",
                                   user=self.user))

    async def get(self):
        self.channel = self.host.resolve_channel(self, "dummy")
        async with stream.merge(self._receive_timer(), super().get()).stream() as streamer:
            async for channel, msg in streamer:
                yield (channel, msg)
