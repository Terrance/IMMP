"""
Basic text request/response handler.

Config:
    channels (str list):
        List of channels to process responses in.
    responses (dict):
        Mapping from match regex to response text.

Commands:
    ar-add <match> <response>:
        Add a new trigger / response pair.
    ar-remove <match>:
        Remove an existing trigger.

This hook will listen for messages in all given channels, for text content that matches any of the
defined regular expressions.  On a match, it will answer with the corresponding response.

Because all responses are defined in the config, you'll need to ensure it's saved when making
changes via the add/remove commands.
"""

import logging
import re

import immp
from immp.hook.command import CommandParser, command


CROSS = "\N{CROSS MARK}"
TICK = "\N{WHITE HEAVY CHECK MARK}"


log = logging.getLogger(__name__)


class AutoRespondHook(immp.Hook):
    """
    Basic text responses for given trigger words and phrases.
    """

    schema = immp.Schema({"groups": [str],
                          immp.Optional("responses", dict): {str: str}})

    group = immp.Group.MergedProperty("groups")

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        self.responses = self.config["responses"]
        self._sent = []

    @command("ar-add", parser=CommandParser.shlex)
    async def add(self, msg, match, response):
        """
        Add a new trigger / response pair.
        """
        text = "Updated" if match in self.responses else "Added"
        self.responses[match] = response
        await msg.channel.send(immp.Message(text="{} {}".format(TICK, text)))

    @command("ar-remove", parser=CommandParser.shlex)
    async def remove(self, msg, match):
        """
        Remove an existing trigger.
        """
        if match in self.responses:
            del self.responses[match]
            text = "{} Removed".format(TICK)
        else:
            text = "{} No such response".format(CROSS)
        await msg.channel.send(immp.Message(text=text))

    async def on_receive(self, sent, source, primary):
        await super().on_receive(sent, source, primary)
        if not primary or not await self.group.has_channel(sent.channel):
            return
        # Skip our own response messages.
        if (sent.channel, sent.id) in self._sent:
            return
        text = str(source.text)
        for match, response in self.responses.items():
            if re.search(match, text, re.I):
                log.debug("Matched regex %r in channel: %r", match, sent.channel)
                for id_ in await sent.channel.send(immp.Message(text=response)):
                    self._sent.append((sent.channel, id_))
