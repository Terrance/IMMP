import re

from voluptuous import Schema, All, Optional, Length, ALLOW_EXTRA

import imirror
from imirror.receiver.command import Commandable


class _Schema(object):

    config = Schema({"channels": All([str], Length(min=1)),
                     Optional("responses", default=dict): {str: str}},
                    extra=ALLOW_EXTRA, required=True)


class AutoRespondReceiver(imirror.Receiver, Commandable):
    """
    Remote control for a running IMirror host.

    Config:
        channels (str list):
            List of channels to process responses in.
        responses (dict):
            Mapping from match regex to response text.
    """

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        config = _Schema.config(config)
        self.responses = config["responses"]
        self.channels = []
        for channel in config["channels"]:
            try:
                self.channels.append(host.channels[channel])
            except KeyError:
                raise imirror.ConfigError("No channel '{}' on host".format(channel)) from None
        self._sent = []

    def commands(self):
        return {"ar-add": self.add,
                "ar-remove": self.remove}

    async def add(self, channel, match, response):
        self.responses[match] = response
        await channel.send(imirror.Message(None, text="\U00002705 Added"))

    async def remove(self, channel, match):
        del self.responses[match]
        await channel.send(imirror.Message(None, text="\U00002705 Removed"))

    async def process(self, channel, msg):
        await super().process(channel, msg)
        # Only process if we recognise the channel.
        if channel not in self.channels:
            return
        # Skip our own response messages.
        if (channel, msg.id) in self._sent:
            return
        text = str(msg.text)
        for match, response in self.responses.items():
            if re.search(match, text, re.I):
                for id in await channel.send(imirror.Message(None, text=response)):
                    self._sent.append((channel, id))
