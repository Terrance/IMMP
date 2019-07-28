"""
Simple custom commands to send preconfigured text messages to channels.

Config:
    commands ((str, str) dict):
        Mapping from command name to rich response text.
"""

import immp
from immp.hook.command import command, DynamicCommands


class TextCommandHook(immp.Hook, DynamicCommands):
    """
    Command provider to send configured text responses.
    """

    schema = immp.Schema({"commands": {str: str}})

    def commands(self):
        return {self._response.complete(name, name) for name in self.config["commands"]}

    @command()
    async def _response(self, name, msg):
        text = immp.RichText.unraw(self.config["commands"][name], self.host)
        await msg.channel.send(immp.Message(text=text))
