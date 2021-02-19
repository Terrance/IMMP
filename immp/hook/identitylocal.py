"""
Basic identity management for users in different networks.

Dependencies:
    :class:`.AsyncDatabaseHook`

Config:
    instance (int):
        Unique instance code.
    plugs (str list):
        List of plug names to accept identities for.
    multiple (bool):
        ``True`` (default) to allow linking multiple accounts from the same network.

Commands:
    id-add <name> <pwd>:
        Create a new identity, or link to an existing one from a second user.
    id-rename <name>:
        Rename the current identity.
    id-password <pwd>:
        Update the password for the current identity.
    id-reset:
        Delete the current identity and all linked users.
    id-role <name> [role]:
        List roles assigned to an identity, or add/remove a given role.

This is a local implementation of the identity protocol, providing self-serve identity linking to
users where a user management backend doesn't otherwise exist.

In order to support multiple copies of this hook with overlapping plugs (e.g. including a private
network in some groups), each hook has an instance code.  If a code isn't defined in the config, a
new one will be assigned at startup.  If multiple hooks are in use, it's important to define these
yourself, so that identities remained assigned to the correct instance.
"""

from hashlib import sha256
import logging

from tortoise import Model
from tortoise.exceptions import DoesNotExist
from tortoise.fields import ForeignKeyField, IntField, TextField

import immp
from immp.hook.access import AccessPredicate
from immp.hook.command import CommandRole, CommandScope, command
from immp.hook.database import AsyncDatabaseHook
from immp.hook.identity import Identity, IdentityProvider


CROSS = "\N{CROSS MARK}"
TICK = "\N{WHITE HEAVY CHECK MARK}"


log = logging.getLogger(__name__)


class IdentityGroup(Model):
    """
    Representation of a single identity.

    Attributes:
        instance (int):
            :class:`IdentityHook` instance code.
        name (str):
            Unique display name.
        pwd (str):
            Hashed password, used by the user to authenticate when linking identities.
        links (.IdentityLink iterable):
            All links contained by this group.
    """

    instance = IntField()
    name = TextField()
    pwd = TextField()

    class Meta:
        # Uniqueness constraint for each name in each identity instance.
        unique_together = (("instance", "name"),)

    @classmethod
    def hash(cls, pwd):
        return sha256(pwd.encode("utf-8")).hexdigest()

    @classmethod
    def select_related(cls):
        return cls.all().prefetch_related("links", "roles")

    async def to_identity(self, host, provider):
        tasks = []
        plugs = {plug.network_id: plug for plug in host.plugs.values()
                 if plug.state == immp.OpenState.active}
        for link in self.links:
            if link.network in plugs:
                tasks.append(plugs[link.network].user_from_id(link.user))
            else:
                log.debug("Ignoring identity link for unavailable plug: %r", link)
        users = await Identity.gather(*tasks)
        roles = [role.role for role in self.roles]
        return Identity(self.name, provider, users, roles)

    def __repr__(self):
        return "<{}: #{} {}>".format(self.__class__.__name__, self.id, repr(self.name))


class IdentityLink(Model):
    """
    Single link between an identity and a user.

    Attributes:
        group (.IdentityGroup):
            Containing group instance.
        network (str):
            Network identifier that the user belongs to.
        user (str):
            User identifier as given by the plug.
    """

    group = ForeignKeyField("db.IdentityGroup", "links")
    network = TextField()
    user = TextField()

    def __repr__(self):
        if isinstance(self.group, IdentityGroup):
            group = repr(self.group)
        else:
            group = "<{}: #{}>".format(IdentityGroup.__name__, self.group_id)
        return "<{}: #{} {} @ {} {}>".format(self.__class__.__name__, self.id, repr(self.user),
                                             repr(self.network), group)


class IdentityRole(Model):
    """
    Assignment of a role to an identity.

    Attributes:
        group (.IdentityGroup):
            Containing group instance.
        role (str):
            Plain role identifier.
    """

    group = ForeignKeyField("db.IdentityGroup", "roles")
    role = TextField()

    def __repr__(self):
        if isinstance(self.group, IdentityGroup):
            group = repr(self.group)
        else:
            group = "<{}: #{}>".format(IdentityGroup.__name__, self.group_id)
        return "<{}: #{} {} {}>".format(self.__class__.__name__, self.id, repr(self.role), group)


class LocalIdentityHook(immp.Hook, AccessPredicate, IdentityProvider):
    """
    Hook for managing physical users with multiple logical links across different plugs.  This
    effectively provides self-service identities, as opposed to being provided externally.
    """

    schema = immp.Schema({immp.Optional("instance"): immp.Nullable(int),
                          "plugs": [str],
                          immp.Optional("multiple", True): bool})

    _plugs = immp.ConfigProperty([immp.Plug])

    def on_load(self):
        self.host.resources[AsyncDatabaseHook].add_models(IdentityGroup, IdentityLink,
                                                          IdentityRole)

    async def start(self):
        await super().start()
        if not self.config["instance"]:
            # Find a non-conflicting number and assign it.
            codes = {hook.config["instance"] for hook in self.host.hooks.values()
                     if isinstance(hook, self.__class__)}
            code = 1
            while code in codes:
                code += 1
            log.debug("Assigning instance code %d to hook %r", code, self.name)
            self.config["instance"] = code

    async def get(self, name):
        """
        Retrieve the identity group using the given name.

        Args:
            name (str):
                Existing name to query.

        Returns:
            .IdentityGroup:
                Linked identity, or ``None`` if not linked.
        """
        try:
            return (await IdentityGroup.select_related()
                                       .get(instance=self.config["instance"], name=name))
        except DoesNotExist:
            return None

    async def find(self, user):
        """
        Retrieve the identity that contains the given user, if one exists.

        Args:
            user (.User):
                Existing user to query.

        Returns:
            .IdentityGroup:
                Linked identity, or ``None`` if not linked.
        """
        if not user or user.plug not in self._plugs:
            return None
        try:
            return (await IdentityGroup.select_related()
                                       .get(instance=self.config["instance"],
                                            links__network=user.plug.network_id,
                                            links__user=user.id))
        except DoesNotExist:
            return None

    async def channel_access(self, channel, user):
        return bool(await self.find(user))

    async def identity_from_name(self, name):
        group = await self.get(name)
        return await group.to_identity(self.host, self) if group else None

    async def identity_from_user(self, user):
        group = await self.find(user)
        return await group.to_identity(self.host, self) if group else None

    def _test(self, channel, user):
        return channel.plug in self._plugs

    @command("id-add", scope=CommandScope.private, test=_test)
    async def add(self, msg, name, pwd):
        """
        Create a new identity, or link to an existing one from a second user.
        """
        if not msg.user or msg.user.plug not in self._plugs:
            return
        if self.find(msg.user):
            text = "{} Already identified".format(CROSS)
        else:
            pwd = IdentityGroup.hash(pwd)
            exists = False
            try:
                group = await IdentityGroup.get(instance=self.config["instance"], name=name)
                exists = True
            except DoesNotExist:
                group = await IdentityGroup.create(instance=self.config["instance"],
                                                   name=name, pwd=pwd)
            if exists and not group.pwd == pwd:
                text = "{} Password incorrect".format(CROSS)
            elif not self.config["multiple"] and any(link.network == msg.user.plug.network_id
                                                     for link in group.links):
                text = "{} Already identified on {}".format(CROSS, msg.user.plug.network_name)
            else:
                await IdentityLink.create(group=group, network=msg.user.plug.network_id,
                                          user=msg.user.id)
                text = "{} {}".format(TICK, "Added" if exists else "Claimed")
        await msg.channel.send(immp.Message(text=text))

    @command("id-rename", scope=CommandScope.private, test=_test)
    async def rename(self, msg, name):
        """
        Rename the current identity.
        """
        if not msg.user:
            return
        group = await self.find(msg.user)
        if not group:
            text = "{} Not identified".format(CROSS)
        elif group.name == name:
            text = "{} No change".format(TICK)
        elif await IdentityGroup.filter(instance=self.config["instance"], name=name).exists():
            text = "{} Name already in use".format(CROSS)
        else:
            group.name = name
            await group.save()
            text = "{} Claimed".format(TICK)
        await msg.channel.send(immp.Message(text=text))

    @command("id-password", scope=CommandScope.private, test=_test)
    async def password(self, msg, pwd):
        """
        Update the password for the current identity.
        """
        if not msg.user:
            return
        group = await self.find(msg.user)
        if not group:
            text = "{} Not identified".format(CROSS)
        else:
            group.pwd = IdentityGroup.hash(pwd)
            await group.save()
            text = "{} Changed".format(TICK)
        await msg.channel.send(immp.Message(text=text))

    @command("id-reset", scope=CommandScope.private, test=_test)
    async def reset(self, msg):
        """
        Delete the current identity and all linked users.
        """
        if not msg.user:
            return
        group = await self.find(msg.user)
        if not group:
            text = "{} Not identified".format(CROSS)
        else:
            await group.delete()
            text = "{} Reset".format(TICK)
        await msg.channel.send(immp.Message(text=text))

    @command("id-role", scope=CommandScope.private, role=CommandRole.admin, test=_test)
    async def role(self, msg, name, role=None):
        """
        List roles assigned to an identity, or add/remove a given role.
        """
        try:
            group = await IdentityGroup.get(instance=self.config["instance"], name=name)
        except DoesNotExist:
            text = "{} Name not registered".format(CROSS)
        else:
            if role:
                if await IdentityRole.filter(group=group, role=role).delete():
                    text = "{} Removed".format(TICK)
                else:
                    await IdentityRole.create(group=group, role=role)
                    text = "{} Added".format(TICK)
            else:
                roles = await IdentityRole.filter(group=group)
                if roles:
                    labels = [role.role for role in roles]
                    text = "Roles for {}: {}".format(name, ", ".join(labels))
                else:
                    text = "No roles for {}.".format(name)
        await msg.channel.send(immp.Message(text=text))
