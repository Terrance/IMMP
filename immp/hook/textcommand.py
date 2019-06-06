"""
Simple custom commands to send preconfigured text messages to channels.

Config:
    commands ((str, str) dict):
        Mapping from command name to rich response text.
"""

from voluptuous import ALLOW_EXTRA, Schema

import immp
from immp.hook.command import command, DynamicCommands


class _Schema:

    config = Schema({"commands": {str: str}}, extra=ALLOW_EXTRA, required=True)


class TextCommandHook(immp.Hook, DynamicCommands):
    """
    Command provider to send configured text responses.
    """

    def __init__(self, name, config, host):
        super().__init__(name, _Schema.config(config), host)

    def commands(self):
        return {self._response.complete(name, name) for name in self.config["commands"]}

    @command()
    async def _response(self, name, msg):
        text = immp.RichText.unraw(self.config["commands"][name], self.host)
        await msg.channel.send(immp.Message(text=text))
