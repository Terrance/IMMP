import code

import imirror


class ShellReceiver(imirror.Receiver):
    """
    A receiver to start a Python shell when a message is received.
    """

    async def process(self, channel, msg):
        code.interact(local=dict(locals(), **globals()))
