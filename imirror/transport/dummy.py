import asyncio

from aiostream import stream

import imirror


class DummyUser(imirror.User):
    pass


class DummyMessage(imirror.Message):
    pass


class DummyTransport(imirror.Transport):
    """
    A fake transport that just yields a message every 10 seconds.
    """

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        self.user = DummyUser(id="dummy", real_name=name)
        self.count = 0
        self.lock = asyncio.BoundedSemaphore()
        self.queue = asyncio.Queue()

    async def send(self, channel, msg):
        await super().send(channel, msg)
        with (await self.lock):
            self.count += 1
            # Make a clone of the message to echo back out of the generator.
            clone = DummyMessage(id=self.count,
                                 at=msg.at,
                                 original=msg.original,
                                 text=msg.text,
                                 user=msg.user,
                                 action=msg.action,
                                 deleted=msg.deleted,
                                 joined=[],
                                 left=[],
                                 raw=msg.raw)
            await self.queue.put(clone)
            return self.count

    async def _receive_queue(self):
        while True:
            yield (await self.queue.get())

    async def _receive_timer(self):
        while True:
            await asyncio.sleep(10)
            with (await self.lock):
                self.count += 1
                yield DummyMessage(id=self.count,
                                   text="Test message",
                                   user=self.user)

    async def receive(self):
        await super().receive()
        channel = self.host.resolve_channel(self, "dummy")
        async with stream.merge(self._receive_queue(), self._receive_timer()).stream() as streamer:
            async for msg in streamer:
                yield (channel, msg)
