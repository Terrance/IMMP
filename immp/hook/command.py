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


class _Schema:

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


@immp.pretty_str
class Command:
    """
    Container of a command function.

    Attributes:
        name (str):
            Command name, used to access the command when directly following the prefix.
        fn (method):
            Callback function to process the command usage.
        scope (.CommandScope):
            Accessibility of this command for the different channel types.
    """

    def __init__(self, name, fn, scope=CommandScope.any):
        self.name = name
        self.fn = fn
        self.scope = scope

    def __call__(self, *args, **kwargs):
        return self.fn(*args, **kwargs)

    def __repr__(self):
        return "<{}: {} @ {}>".format(self.__class__.__name__, self.name, self.scope)


class Commandable:
    """
    Interface for hooks to implement, allowing them to provide their own commands.
    """

    def commands(self):
        """
        Generate a list of commands to be registered with the command hook.

        Returns:
            .Command list:
                Commands available from the implementer.
        """
        return []


@immp.config_props("plugs", "channels", "hooks")
class CommandHook(immp.Hook):
    """
    Generic command handler for other hooks.
    """

    def __init__(self, name, config, host):
        super().__init__(name, _Schema.config(config), host)
        self.commands = defaultdict(dict)
        for hook in self.hooks:
            if not isinstance(hook, Commandable):
                raise immp.ConfigError("Hook '{}' does not support commands"
                                       .format(hook.name)) from None
            for command in hook.commands():
                log.debug("Adding command from hook '{}': {}".format(hook.name, repr(command)))
                self.commands[command.scope][command.name] = command

    async def process(self, channel, msg, source, primary):
        await super().process(channel, msg, source, primary)
        if not primary or not msg == source:
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
