"""
Self-serve Discord roles for users.

Dependencies:
    :class:`.DiscordPlug`

Config:
    roles ((str, int) dict):
        Mapping from user-facing role names to Discord role IDs.

Commands:
    role <name>:
        Claim a role of this name.
    unrole <name>:
        Drop the role of this name.
"""

import immp
from immp.hook.command import CommandParser, CommandScope, command
from immp.plug.discord import DiscordPlug


class _NoSuchRole(Exception):
    pass


class DiscordRoleHook(immp.Hook):
    """
    Hook to assign and unassign Discord roles to and from users.
    """

    schema = immp.Schema({"roles": {str: int}})

    def _common(self, msg, name):
        if name not in self.config["roles"]:
            raise _NoSuchRole
        client = msg.channel.plug._client
        channel = client.get_channel(int(msg.channel.source))
        member = channel.guild.get_member(int(msg.user.id))
        for role in channel.guild.roles:
            if role.id == self.config["roles"][name]:
                return role, member
        else:
            raise _NoSuchRole

    def _test(self, channel, user):
        return isinstance(channel.plug, DiscordPlug)

    @command("role", scope=CommandScope.shared, parser=CommandParser.none,
             test=_test, sync_aware=True)
    async def role(self, msg, name):
        try:
            role, member = self._common(msg, str(name))
        except _NoSuchRole:
            await msg.channel.send(immp.Message(text="No such role"))
            return
        else:
            await member.add_roles(role)
            await msg.channel.send(immp.Message(text="\N{WHITE HEAVY CHECK MARK} Added"))

    @command("unrole", scope=CommandScope.shared, parser=CommandParser.none,
             test=_test, sync_aware=True)
    async def unrole(self, msg, name):
        try:
            role, member = self._common(msg, str(name))
        except _NoSuchRole:
            await msg.channel.send(immp.Message(text="No such role"))
            return
        else:
            await member.remove_roles(role)
            await msg.channel.send(immp.Message(text="\N{WHITE HEAVY CHECK MARK} Removed"))
