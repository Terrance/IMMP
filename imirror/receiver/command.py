import logging
from shlex import split

from voluptuous import ALLOW_EXTRA, All, Length, Schema

import imirror


log = logging.getLogger(__name__)


class _Schema(object):

    config = Schema({"prefix": str,
                     "channels": All([str], Length(min=1)),
                     "receivers": [str]},
                    extra=ALLOW_EXTRA, required=True)


class Commandable(object):
    """
    Interface for receivers to implement, allowing them to provide their own commands.
    """

    def commands(self):
        """
        Generate a list of commands to be registered with the command receiver.

        Returns:
            (str, function) dict:
                Mapping from command names to callback functions.
        """
        return {}


class CommandReceiver(imirror.Receiver):
    """
    Generic command handler for other receivers.

    Config:
        prefix (str):
            Characters at the start of a message to denote commands.  Use a single character to
            make commands top-level (e.g. ``"?"`` would allow commands like ``?help``), or a string
            followed by a space for subcommands (e.g. ``"!bot "`` for ``!bot help``).
        channels (str list):
            List of channels to process commands in.
        receivers (str list):
            List of receivers to enable commands for.
    """

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        config = _Schema.config(config)
        self.prefix = config["prefix"]
        self.channels = []
        for label in config["channels"]:
            try:
                self.channels.append(host.channels[label])
            except KeyError:
                raise imirror.ConfigError("No channel '{}' on host".format(label)) from None
        self.commands = {}
        for label in config["receivers"]:
            try:
                receiver = host.receivers[label]
            except KeyError:
                raise imirror.ConfigError("No receiver '{}' on host".format(label)) from None
            if not isinstance(receiver, Commandable):
                raise imirror.ConfigError("Receiver '{}' does not support commands"
                                          .format(label)) from None
            commands = receiver.commands()
            if not commands:
                return
            log.debug("Adding commands for receiver '{}': {}".format(label, ", ".join(commands)))
            self.commands.update(commands)

    async def process(self, channel, msg):
        await super().process(channel, msg)
        # Only process if we recognise the channel and the command.
        if channel not in self.channels:
            return
        if not (msg.text and str(msg.text).startswith(self.prefix)):
            return
        try:
            # TODO: Preserve formatting.
            command = split(str(msg.text)[len(self.prefix):])
        except ValueError:
            return
        if command[0] in self.commands:
            await self.commands[command[0]](channel, msg, *command[1:])
