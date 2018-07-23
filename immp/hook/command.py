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
    admins ((str, str list) dict):
        Users authorised to execute administrative commands, a mapping of network identifiers to
        lists of user identifiers.

The binding works by making commands exposed by all listed hooks available to all listed channels,
and to the private channels of all listed plugs.  Note that the channels need not belong to any
hook-specific config -- you can, for example, bind some commands to an admin-only channel
elsewhere.  Multiple command hooks can be loaded for fine-grained control.
"""

from collections import defaultdict
from enum import Enum
import logging
from shlex import split

from voluptuous import ALLOW_EXTRA, Any, Optional, Schema

import immp


log = logging.getLogger(__name__)


class _Schema:

    config = Schema({"prefix": str,
                     Optional("plugs", default=list): [str],
                     Optional("channels", default=list): [str],
                     Optional("hooks", default=list): [str],
                     Optional("admins", default=dict): Any({}, {str: [int]})},
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
        admin:
            Only authorised users in private channels.
    """
    any = 0
    private = 1
    public = 2
    admin = 3


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
        args (str):
            Readable summary of accepted arguments, e.g. ``<arg1> "<arg2>" [optional]``.
        help (str):
            Full description of the command.
    """

    def __init__(self, name, fn, scope=CommandScope.any, args=None, help=None):
        self.name = name
        self.fn = fn
        self.scope = scope
        self.args = args
        self.help = help

    async def __call__(self, *args, **kwargs):
        return await self.fn(*args, **kwargs)

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
class CommandHook(immp.Hook, Commandable):
    """
    Generic command handler for other hooks.
    """

    def __init__(self, name, config, host):
        super().__init__(name, _Schema.config(config), host)
        self._commands = defaultdict(dict)
        for hook in (self,) + self.hooks:
            if not isinstance(hook, Commandable):
                raise immp.ConfigError("Hook '{}' does not support commands"
                                       .format(hook.name)) from None
            for command in hook.commands():
                log.debug("Adding command from hook '{}': {}".format(hook.name, repr(command)))
                self._commands[command.scope][command.name] = command

    async def scopes(self, channel, user):
        scopes = []
        if channel in self.channels:
            scopes.extend([CommandScope.any, CommandScope.public])
        if channel.plug in self.plugs and await channel.is_private():
            scopes.extend([CommandScope.any, CommandScope.private])
            if user and user.id in self.config["admins"].get(user.plug.name, []):
                scopes.append(CommandScope.admin)
        return list(reversed(scopes))

    def get(self, scopes, name):
        for scope in scopes:
            if name in self._commands[scope]:
                return self._commands[scope][name]
        return None

    def commands(self):
        return [Command("help", self.help, CommandScope.any, "[command]",
                        "Show details about the given command, or list available commands.")]

    async def help(self, channel, msg, name=None):
        scope = await self.scope(channel)
        if name:
            command = self.get(scope, name)
            if command:
                text = immp.RichText([immp.Segment(command.name, bold=True)])
                if command.args:
                    text.append(immp.Segment(" {}".format(command.args)))
                if command.help:
                    text.append(immp.Segment(":\n", bold=True),
                                immp.Segment(command.help))
            else:
                text = "\N{CROSS MARK} No such command"
        else:
            text = immp.RichText([immp.Segment("Available commands:", bold=True)])
            commands = (list(self._commands[scope].values()) +
                        list(self._commands[CommandScope.any].values()))
            for command in sorted(commands, key=lambda c: c.name):
                text.append(immp.Segment("\n- {}".format(command.name)))
        await channel.send(immp.Message(text=text))

    async def process(self, channel, msg, source, primary):
        await super().process(channel, msg, source, primary)
        if not primary or not msg == source:
            return
        scopes = await self.scopes(channel, msg.user)
        if not scopes:
            return
        if not (source.text and str(source.text).startswith(self.config["prefix"])):
            return
        try:
            # TODO: Preserve formatting.
            name, *args = split(str(source.text)[len(self.config["prefix"]):])
        except ValueError:
            return
        command = self.get(scopes, name)
        if not command:
            return
        try:
            log.debug("Executing command in channel: {} {}".format(repr(channel), source.text))
            await command(channel, source, *args)
        except Exception:
            log.exception("Exception whilst running command: {}".format(source.text))
