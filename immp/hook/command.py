"""
Backbone for other hooks to process commands contained in channel messages.

Config:
    prefix (str):
        Characters at the start of a message to denote commands.  Use a single character to
        make commands top-level (e.g. ``"?"`` would allow commands like ``?help``), or a string
        followed by a space for subcommands (e.g. ``"!bot "`` for ``!bot help``).
    return-errors (bool):
        ``True`` to send unhandled exceptions raised by commands back to the source channel
        (``False`` by default).
    sets ((str, str list) dict):
        Subsets of hook commands by name, to restrict certain features.
    mapping ((str, dict) dict):
        Named config groups to enable commands in selected channels.

        groups (str list):
            List of groups (plugs and channels) to process public commands in.
        hooks (str list):
            List of hooks to enable commands for.
        sets (str list):
            List of command sets to enable.
        admins ((str, str list) dict):
            Users authorised to execute administrative commands, a mapping of network identifiers
            to lists of user identifiers.

The binding works by making commands exposed by all listed hooks available to all listed channels,
and to the private channels of all listed plugs.  Note that the channels need not belong to any
hook-specific config -- you can, for example, bind some commands to an admin-only channel
elsewhere.  Multiple groups can be used for fine-grained control.
"""

from collections import defaultdict
from copy import copy
from enum import Enum
import inspect
import logging
import re
import shlex

import immp


log = logging.getLogger(__name__)


class BadUsage(immp.HookError):
    """
    May be raised from within a command to indicate that the arguments were invalid.
    """


class CommandParser(Enum):
    """
    Constants representing the method used to parse the argument text following a used command.

    Attributes:
        spaces:
            Split using :meth:`str.split`, for simple inputs breaking on whitespace characters.
        shlex:
            Split using :func:`shlex.split`, which allows quoting of multi-word arguments.
        none:
            Don't split the argument, just provide the rich message text as-is.
        hybrid:
            Split as with ``spaces`` up to the number of accepted arguments, and return rich text
            in the last argument.  If optional arguments are present, only the last will receive
            the rich text, or none of them if not filled in.
    """
    spaces = 0
    shlex = 1
    none = 2
    hybrid = 3


class CommandScope(Enum):
    """
    Constants representing the types of conversations a command is available in.

    Attributes:
        anywhere:
            All configured channels.
        private:
            Only private channels, as configured per-plug.
        shared:
            Only non-private channels, as configured per-channel.
    """
    anywhere = 0
    private = 1
    shared = 2


class CommandRole(Enum):
    """
    Constants representing the types of users a command is available in.

    Attributes:
        anyone:
            All configured channels.
        admin:
            Only authorised users in the command group.
    """
    anyone = 0
    admin = 1


@immp.pretty_str
class BoundCommand:
    """
    Wrapper object returned when accessing a command via a :class:`.Hook` instance, similar to
    :class:`types.MethodType`.

    This object is callable, which invokes the command's underlying method against the bound hook.
    """

    def __init__(self, hook, cmd):
        self.hook = hook
        self.cmd = cmd

    def applicable(self, channel, user, private, admin):
        """
        Test the availability of the current command based on the scope and role.

        Args:
            channel (.Channel):
                Source channel where the command will be executed.
            user (.User):
                Author of the message to trigger the command.
            private (bool):
                Result of :meth:`.Channel.is_private`.
            admin (bool):
                ``True`` if the author is defined as an admin of this :class:`.CommandHook`.

        Returns:
            bool:
                ``True`` if the command may be used.
        """
        if self.scope == CommandScope.private and not private:
            return False
        elif self.scope == CommandScope.shared and private:
            return False
        elif self.role == CommandRole.admin and not admin:
            return False
        elif self.cmd.test:
            return self.cmd.test(self.hook, channel, user)
        else:
            return True

    def complete(self, name, *args):
        """
        Fully-qualify an underlying base command.

        Args:
            name (str):
                Command name, used to access the command when directly following the prefix.
            args (tuple):
                Additional arguments to pass to the underlying method.

        Returns:
            .BoundCommand:
                Wrapper around the now-fully-qualified instance of the base command.
        """
        return BoundCommand(self.hook, self.cmd.complete(name, *args))

    async def __call__(self, msg, *args):
        if isinstance(self.cmd, BaseCommand) and not isinstance(self.cmd, FullCommand):
            raise ValueError("Command is dynamic")
        else:
            return await self.cmd.fn(self.hook, *self.cmd.fixed_args, msg, *args)

    def __getattr__(self, name):
        # Propagate other attribute access to the unbound command object.
        return getattr(self.cmd, name)

    def __eq__(self, other):
        return (isinstance(other, self.__class__) and
                (self.hook, self.cmd) == (other.hook, other.cmd))

    def __hash__(self):
        return hash((self.hook, self.cmd))

    def __repr__(self):
        return "<{}: {} {}>".format(self.__class__.__name__, repr(self.hook), repr(self.cmd))


@immp.pretty_str
class BaseCommand:
    """
    Container of a command function.  Use the :meth:`command` decorator to wrap a class method and
    convert it into an instance of this class.  Unless dynamic, command objects will be an instance
    of the :class:`.FullCommand` subclass.

    Accessing an instance of this class via the attribute of a containing class will create a new
    :class:`BoundCommand` allowing invocation of the method.

    Attributes:
        fn (method):
            Callback method from a hook to process the command usage.
        parser (.CommandParser):
            Parse mode for the command arguments.
        scope (.CommandScope):
            Accessibility of this command for the different channel types.
        role (.CommandRole):
            Accessibility of this command for different users.
        test (method):
            Additional predicate that can enable or disable a command based on hook state.
        sync_aware (bool):
            ``True`` if the hook is aware of synced channels provided by a :class:`.SyncPlug`.
        fixed (int):
            Number of fixed arguments the underlying method requires.
        doc (str):
            Help for the command, taken from the callback method's docstring if present.
        spec (str):
            Human-readable summary of the arguments required by the method.
    """

    def __init__(self, fn, parser=CommandParser.spaces, scope=CommandScope.anywhere,
                 role=CommandRole.anyone, test=None, sync_aware=False):
        self.fn = fn
        self.parser = parser
        self.scope = scope
        self.role = role
        self.test = test
        self.sync_aware = sync_aware
        # Only positional arguments are produced by splitting the input, there are no keywords.
        if any(param.kind in (inspect.Parameter.KEYWORD_ONLY,
                              inspect.Parameter.VAR_KEYWORD) for param in self._args[1]):
            raise ValueError("Keyword-only command parameters are not supported: {}".format(fn))

    @property
    def _args(self):
        # Skip `self` argument.
        params = tuple(inspect.signature(self.fn).parameters.values())[1:]
        # Split on `msg` argument into fixed and called arguments.
        for i, param in enumerate(params):
            if param.name == "msg":
                return (params[:i], params[i + 1:])
        else:
            raise ValueError("Command method doesn't accept a `msg` parameter")

    @property
    def fixed(self):
        return len(self._args[0])

    @property
    def min(self):
        return len([arg for arg in self._args[1] if arg.default is inspect.Parameter.empty])

    @property
    def max(self):
        return len(self._args[1])

    @property
    def doc(self):
        return inspect.cleandoc(self.fn.__doc__) if self.fn.__doc__ else None

    @property
    def spec(self):
        parts = []
        for param in self._args[1]:
            if param.kind in (inspect.Parameter.POSITIONAL_ONLY,
                              inspect.Parameter.POSITIONAL_OR_KEYWORD):
                parts.append(("<{}>" if param.default is inspect.Parameter.empty else "[{}]")
                             .format(param.name))
            elif param.kind == inspect.Parameter.VAR_POSITIONAL:
                parts.append("[{}...]".format(param.name))
        return " ".join(parts)

    def parse(self, args):
        """
        Convert a string of multiple arguments into a list according to the chosen parse mode.

        Args:
            args (.RichText):
                Trailing argument text from a message.

        Returns:
            (str or RichText) list:
                Parsed arguments.
        """
        if not args:
            return []
        if self.parser == CommandParser.spaces:
            return str(args).split()
        elif self.parser == CommandParser.shlex:
            return shlex.split(str(args))
        elif self.parser == CommandParser.hybrid:
            filled = self.max
            parts = str(args).split(maxsplit=filled - 1)
            if len(parts) < filled:
                return parts
            full = re.split(r"(\s+)", str(args))
            index = filled * 2
            if full[0]:
                index -= 2
            offset = len("".join(full[:index]))
            return parts[:-1] + [args[offset::True]]
        else:
            return [args]

    def valid(self, *args):
        """
        Test the validity of the given arguments against the command's underlying method.  Raises
        :class:`ValueError` if the arguments don't match the signature.

        Args:
            args (str list):
                Parsed arguments.
        """
        params = self._args[1]
        varargs = len([arg for arg in params if arg.kind is inspect.Parameter.VAR_POSITIONAL])
        required = self.min - varargs
        if len(args) < required:
            raise ValueError("Expected at least {} args, got {}".format(required, len(args)))
        if len(args) > len(params) and not varargs:
            raise ValueError("Expected at most {} args, got {}".format(len(params), len(args)))

    def complete(self, name, *args):
        """
        Fully-qualify a base command.

        Args:
            name (str):
                Command name, used to access the command when directly following the prefix.
            args (tuple):
                Additional arguments to pass to the underlying method.

        Returns:
            .FullCommand:
                Fully-qualified instance of this base command.
        """
        return FullCommand(name, self.fn, self.parser, self.scope, self.role, self.test,
                           self.sync_aware, args)

    def __get__(self, instance, owner):
        return BoundCommand(instance, self) if instance else self

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.fn == other.fn

    def __hash__(self):
        return hash(self.fn)

    def __repr__(self):
        return "<{}: {}, {} {}>".format(self.__class__.__name__, self.scope.name,
                                        self.role.name, self.fn)


class FullCommand(BaseCommand):
    """
    Fully-qualified and named command, an extension of :class:`.BaseCommand` to make it callable.

    Attributes:
        name (str):
            Command name, used to access the command when directly following the prefix.
        fixed_args (tuple):
            Additional arguments to pass to the underlying method.
    """

    def __init__(self, name, fn, parser=CommandParser.spaces, scope=CommandScope.anywhere,
                 role=CommandRole.anyone, test=None, sync_aware=False, fixed_args=()):
        super().__init__(fn, parser, scope, role, test, sync_aware)
        self.name = name.lower()
        self.fixed_args = fixed_args
        if len(self.fixed_args) != self.fixed:
            raise ValueError("Expecting {} fixed arguments, got {}"
                             .format(self.fixed, len(self.fixed_args)))

    def complete(self, name, *args):
        raise ValueError("Already a full command")

    def __eq__(self, other):
        return super().__eq__(other) and self.name == other.name

    def __hash__(self):
        return hash((super().__hash__(), self.name))

    def __repr__(self):
        return "<{}: {} @ {}, {} {}>".format(self.__class__.__name__, self.name, self.scope.name,
                                             self.role.name, self.fn)


def command(name=None, parser=CommandParser.spaces, scope=CommandScope.anywhere,
            role=CommandRole.anyone, test=None, sync_aware=False):
    """
    Decorator: mark up a hook method as a command.

    This doesn't return the original function, rather a :class:`.BaseCommand` object.

    The method's accepted arguments must start with ``self``, and must accept one called ``msg``,
    which will be set to the :class:`.Message` instance that triggered this command.

    A *dynamic* command is an unnamed instance that can be used to create multiple commands with
    the same underlying method call.  These commands are not collected automatically -- the parent
    hook must implement :class:`.DynamicCommands`.

    Arguments:
        name (str):
            Command name, used to access the command when directly following the prefix.
        parser (.CommandParser):
            Parse mode for the command arguments.
        scope (.CommandScope):
            Accessibility of this command for the different channel types.
        role (.CommandRole):
            Accessibility of this command for different users.
        test (method):
            Additional predicate that can enable or disable a command based on hook state.
        sync_aware (bool):
            ``True`` if the hook is aware of synced channels provided by a :class:`.SyncPlug`.  In
            this case, the command handler will receive the native channel rather than the virtual
            sync channel.  See :meth:`.SyncPlug.sync_for` for resolving this to a virtual channel.
    """
    def wrap(fn):
        args = (fn, parser, scope, role, test, sync_aware)
        return FullCommand(name, *args) if name else BaseCommand(*args)
    return wrap


class DynamicCommands:
    """
    Interface for commands generated at runtime.  Typically combined with additional arguments in a
    command method's signature.
    """

    def commands(self):
        """
        Provide additional commands dependent on state or config.  Dynamic commands must be filled
        in using :meth:`.BaseCommand.complete` to provide their name and any fixed arguments.

        Returns:
            .BoundCommand set:
                Set of available commands.
        """
        raise NotImplementedError


class CommandHook(immp.Hook):
    """
    Generic command handler for other hooks.

    Hooks should wrap command handler methods with the :func:`command` decorator, which will be
    picked up by any instances of this class configured to include that hook.
    """

    schema = immp.Schema({"prefix": [str],
                          immp.Optional("return-errors", False): bool,
                          immp.Optional("sets", dict): {str: {str: [str]}},
                          "mapping": {str: {immp.Optional("groups", list): [str],
                                            immp.Optional("hooks", list): [str],
                                            immp.Optional("sets", list): [str],
                                            immp.Optional("admins", dict): {str: [str]}}}})

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        # Avoiding circular dependency between commands and sync -- use the full path to populate
        # that attribute path in the global `immp` import for later (so unused here).
        import immp.hook.sync  # noqa

    def discover(self, hook):
        """
        Inspect a :class:`.Hook` instance, scanning its attributes for commands.

        Returns:
            (str, .BoundCommand) dict:
                Commands provided by this hook, keyed by name.
        """
        if hook.state != immp.OpenState.active:
            return {}
        attrs = [getattr(hook, attr) for attr in dir(hook)]
        cmds = {cmd.name: cmd for cmd in attrs if isinstance(cmd, BoundCommand)
                and isinstance(cmd.cmd, FullCommand)}
        if isinstance(hook, DynamicCommands):
            cmds.update({cmd.name: cmd for cmd in hook.commands()})
        return cmds

    def _mapping_cmds(self, mapping, channel, user, private):
        cmdgroup = set()
        admin = user.plug and user.id in (mapping["admins"].get(user.plug.name) or [])
        for name in mapping["hooks"]:
            cmdgroup.update(set(self.discover(self.host.hooks[name]).values()))
        for label in mapping["sets"]:
            for name, cmdset in self.config["sets"][label].items():
                discovered = self.discover(self.host.hooks[name])
                cmdgroup.update(set(discovered[cmd] for cmd in cmdset))
        return {cmd for cmd in cmdgroup if cmd.applicable(channel, user, private, admin)}

    async def commands(self, channel, user):
        """
        Retrieve all commands, and filter against the mappings.

        Args:
            channel (.Channel):
                Source channel where the command will be executed.
            user (.User):
                Author of the message to trigger the command.

        Returns:
            (str, .BoundCommand) dict:
                Commands provided by all hooks, in this channel for this user, keyed by name.
        """
        log.debug("Collecting commands for %r in %r", user, channel)
        if isinstance(channel, immp.Plug):
            # Look for commands for a generic channel.
            plug = channel
            channel = immp.Channel(plug, "")
            private = False
        else:
            plug = None
            private = await channel.is_private()
        mappings = []
        for mapping in self.config["mapping"].values():
            for label in mapping["groups"]:
                group = self.host.groups[label]
                if plug and group.has_plug(plug, "anywhere", "named"):
                    mappings.append(mapping)
                elif not plug and await group.has_channel(channel):
                    mappings.append(mapping)
        cmds = set()
        for mapping in mappings:
            cmds.update(self._mapping_cmds(mapping, channel, user, private))
        mapped = {cmd.name: cmd for cmd in cmds}
        if len(cmds) > len(mapped):
            # Mapping by name silently overwrote at least one command with a duplicate name.
            raise immp.ConfigError("Multiple applicable commands with the same name")
        return mapped

    @command("help", sync_aware=True)
    async def help(self, msg, command=None):
        """
        List all available commands in this channel, or show help about a single command.
        """
        if await msg.channel.is_private():
            current = None
            private = msg.channel
        else:
            current = msg.channel
            private = await msg.user.private_channel()
        parts = defaultdict(dict)
        if current:
            parts[current] = await self.commands(current, msg.user)
        if private:
            parts[private] = await self.commands(private, msg.user)
            for name in parts[private]:
                parts[current].pop(name, None)
        parts[None] = await self.commands(msg.channel.plug, msg.user)
        for name in parts[None]:
            if private:
                parts[private].pop(name, None)
            if current:
                parts[current].pop(name, None)
        full = dict(parts[None])
        full.update(parts[current])
        full.update(parts[private])
        if command:
            try:
                cmd = full[command]
            except KeyError:
                text = "\N{CROSS MARK} No such command"
            else:
                text = immp.RichText([immp.Segment(cmd.name, bold=True)])
                if cmd.spec:
                    text.append(immp.Segment(" {}".format(cmd.spec)))
                if cmd.doc:
                    text.append(immp.Segment(":", bold=True),
                                immp.Segment("\n"),
                                *immp.RichText.unraw(cmd.doc, self.host))
        else:
            titles = {None: [immp.Segment("Global commands", bold=True)]}
            if private:
                titles[private] = [immp.Segment("Private commands", bold=True)]
            if current:
                titles[current] = [immp.Segment("Commands for ", bold=True),
                                   immp.Segment(await current.title(), bold=True, italic=True)]
            text = immp.RichText()
            for channel, cmds in parts.items():
                if not cmds:
                    continue
                if text:
                    text.append(immp.Segment("\n"))
                text.append(*titles[channel])
                for name, cmd in sorted(cmds.items()):
                    text.append(immp.Segment("\n- {}".format(name)))
                    if cmd.spec:
                        text.append(immp.Segment(" {}".format(cmd.spec), italic=True))
        await msg.channel.send(immp.Message(text=text))

    async def on_receive(self, sent, source, primary):
        await super().on_receive(sent, source, primary)
        if not primary or not sent.user or not sent.text or sent != source:
            return
        plain = str(sent.text)
        raw = None
        for prefix in self.config["prefix"]:
            if plain.lower().startswith(prefix):
                raw = plain[len(prefix):].split(maxsplit=1)
                break
        if not raw:
            return
        # Sync integration: exclude native channels of syncs from command execution.
        if isinstance(sent.channel.plug, immp.hook.sync.SyncPlug):
            log.debug("Suppressing command in virtual sync channel: %r", sent.channel)
            return
        synced = immp.hook.sync.SyncPlug.any_sync(self.host, sent.channel)
        if synced:
            log.debug("Mapping command channel: %r -> %r", sent.channel, synced)
        name = raw[0].lower()
        trailing = sent.text[-len(raw[1])::True] if len(raw) == 2 else None
        cmds = await self.commands(sent.channel, sent.user)
        try:
            cmd = cmds[name]
        except KeyError:
            log.debug("No matches for command name %r in %r", name, sent.channel)
            return
        else:
            log.debug("Matched command in %r: %r", sent.channel, cmd)
        try:
            args = cmd.parse(trailing)
            cmd.valid(*args)
        except ValueError:
            # Invalid number of arguments passed, return the command usage.
            await self.help(sent, name)
            return
        if synced and not cmd.sync_aware:
            msg = copy(sent)
            msg.channel = synced
        else:
            msg = sent
        try:
            log.debug("Executing command: %r %r", sent.channel, sent.text)
            await cmd(msg, *args)
        except BadUsage:
            await self.help(sent, name)
        except Exception as e:
            log.exception("Exception whilst running command: %r", sent.text)
            if self.config["return-errors"]:
                text = ": ".join(filter(None, (e.__class__.__name__, str(e))))
                await sent.channel.send(immp.Message(text="\N{WARNING SIGN} {}".format(text)))
