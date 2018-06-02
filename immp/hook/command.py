"""
Backbone for other hooks to process commands contained in channel messages.

Config:
    prefix (str):
        Characters at the start of a message to denote commands.  Use a single character to
        make commands top-level (e.g. ``"?"`` would allow commands like ``?help``), or a string
        followed by a space for subcommands (e.g. ``"!bot "`` for ``!bot help``).
    channels (str list):
        List of channels to process commands in.
    hooks (str list):
        List of hooks to enable commands for.

The binding works by making commands exposed by all listed hooks available to all listed channels.
Note that the channels need not belong to the hooks -- you can, for example, bind some commands to
an admin-only channel elsewhere.

Multiple command hooks can be loaded for fine-grained control.
"""

import logging
from shlex import split

from voluptuous import ALLOW_EXTRA, All, Length, Schema

import immp


log = logging.getLogger(__name__)


class _Schema(object):

    config = Schema({"prefix": str,
                     "channels": All([str], Length(min=1)),
                     "hooks": [str]},
                    extra=ALLOW_EXTRA, required=True)


class Commandable(object):
    """
    Interface for hooks to implement, allowing them to provide their own commands.
    """

    def commands(self):
        """
        Generate a list of commands to be registered with the command hook.

        Returns:
            (str, function) dict:
                Mapping from command names to callback functions.
        """
        return {}


class CommandHook(immp.Hook):
    """
    Generic command handler for other hooks.
    """

    def __init__(self, name, config, host):
        super().__init__(name, _Schema.config(config), host)
        self.channels = []
        for label in self.config["channels"]:
            try:
                self.channels.append(host.channels[label])
            except KeyError:
                raise immp.ConfigError("No channel '{}' on host".format(label)) from None
        self.commands = {}
        for label in self.config["hooks"]:
            try:
                hook = host.hooks[label]
            except KeyError:
                raise immp.ConfigError("No hook '{}' on host".format(label)) from None
            if not isinstance(hook, Commandable):
                raise immp.ConfigError("Hook '{}' does not support commands"
                                       .format(label)) from None
            commands = hook.commands()
            if not commands:
                return
            log.debug("Adding commands for hook '{}': {}".format(label, ", ".join(commands)))
            self.commands.update(commands)

    async def process(self, channel, msg, source, primary):
        await super().process(channel, msg, source, primary)
        if not primary or channel not in self.channels:
            return
        if not (source.text and str(source.text).startswith(self.config["prefix"])):
            return
        try:
            # TODO: Preserve formatting.
            command = split(str(source.text)[len(self.config["prefix"]):])
        except ValueError:
            return
        if command[0] in self.commands:
            await self.commands[command[0]](channel, source, *command[1:])
