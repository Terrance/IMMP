"""
Backbone for other hooks to process commands contained in channel messages.

Config:
    prefix (str):
        Characters at the start of a message to denote commands.  Use a single character to
        make commands top-level (e.g. ``"?"`` would allow commands like ``?help``), or a string
        followed by a space for subcommands (e.g. ``"!bot "`` for ``!bot help``).
    plugs (str list):
        List of plugs where commands should be processed in private channels.
    channels (str list):
        List of channels to process public commands in (independent of *plugs* above).
    hooks (str list):
        List of hooks to enable commands for.

The binding works by making commands exposed by all listed hooks available to all listed channels,
and to the private channels of all listed plugs.  Note that the channels need not belong to any
hook-specific config -- you can, for example, bind some commands to an admin-only channel
elsewhere.  Multiple command hooks can be loaded for fine-grained control.
"""

from collections import defaultdict
from enum import Enum
import logging
from shlex import split

from voluptuous import ALLOW_EXTRA, Optional, Schema

import immp


log = logging.getLogger(__name__)


class _Schema(object):

    config = Schema({"prefix": str,
                     Optional("plugs", default=list): [str],
                     Optional("channels", default=list): [str],
                     "hooks": [str]},
                    extra=ALLOW_EXTRA, required=True)


class CommandScope(Enum):
    """
    Constants representing the types of conversations a command is available in.

    Attributes:
        any:
            All configured channels.
        private:
            Only private channels, as configured per-plug.
        public:
            Only non-private channels, as configured per-channel.
    """
    any = 0
    private = 1
    public = 2


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
        self.plugs = []
        for label in self.config["plugs"]:
            try:
                self.plugs.append(host.plugs[label])
            except KeyError:
                raise immp.ConfigError("No plug '{}' on host".format(label)) from None
        self.channels = []
        for label in self.config["channels"]:
            try:
                self.channels.append(host.channels[label])
            except KeyError:
                raise immp.ConfigError("No channel '{}' on host".format(label)) from None
        self.commands = defaultdict(dict)
        for label in self.config["hooks"]:
            try:
                hook = host.hooks[label]
            except KeyError:
                raise immp.ConfigError("No hook '{}' on host".format(label)) from None
            if not isinstance(hook, Commandable):
                raise immp.ConfigError("Hook '{}' does not support commands"
                                       .format(label)) from None
            commands = hook.commands()
            for scope in CommandScope:
                if commands.get(scope):
                    log.debug("Adding commands for hook '{}' ({} scope): {}"
                              .format(label, scope.name, ", ".join(commands[scope])))
                    self.commands[scope].update(commands[scope])

    async def process(self, channel, msg, source, primary):
        await super().process(channel, msg, source, primary)
        if not primary:
            return
        if channel in self.channels:
            scope = CommandScope.public
        elif channel.plug in self.plugs and await channel.is_private():
            scope = CommandScope.private
        else:
            return
        if not (source.text and str(source.text).startswith(self.config["prefix"])):
            return
        try:
            # TODO: Preserve formatting.
            command = split(str(source.text)[len(self.config["prefix"]):])
        except ValueError:
            return
        func = (self.commands[scope].get(command[0]) or
                self.commands[CommandScope.any].get(command[0]))
        if func:
            try:
                await func(channel, source, *command[1:])
            except Exception:
                log.exception("Exception whilst running command: {}".format(source.text))
