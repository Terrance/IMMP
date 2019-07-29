"""
Basic identity management for users in different networks.

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

.. note::
    This hook requires an active :class:`.DatabaseHook` to store data.
"""

from asyncio import gather
from hashlib import sha256
import logging

from peewee import CharField, ForeignKeyField, IntegerField

import immp
from immp.hook.access import AccessPredicate
from immp.hook.command import CommandRole, CommandScope, command
from immp.hook.database import BaseModel, DatabaseHook
from immp.hook.identity import Identity, IdentityProvider


CROSS = "\N{CROSS MARK}"
TICK = "\N{WHITE HEAVY CHECK MARK}"


log = logging.getLogger(__name__)


class IdentityGroup(BaseModel):
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

    instance = IntegerField()
    name = CharField()
    pwd = CharField()

    class Meta:
        # Uniqueness constraint for each name in each identity instance.
        indexes = ((("instance", "name"), True),)

    @classmethod
    def hash(cls, pwd):
        return sha256(pwd.encode("utf-8")).hexdigest()

    @classmethod
    def select_links(cls):
        return cls.select(cls, IdentityLink).join(IdentityLink)

    async def to_identity(self, host):
        tasks = []
        plugs = {plug.network_id: plug for plug in host.plugs.values()}
        for link in self.links:
            tasks.append(plugs[link.network].user_from_id(link.user))
        users = await gather(*tasks)
        roles = [role.role for role in self.roles]
        return Identity(self.name, users, roles)

    def __repr__(self):
        return "<{}: #{} {}>".format(self.__class__.__name__, self.id, repr(self.name))


class IdentityLink(BaseModel):
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

    group = ForeignKeyField(IdentityGroup, related_name="links", on_delete="cascade")
    network = CharField()
    user = CharField()

    def __repr__(self):
        return "<{}: #{} {} @ {} {}>".format(self.__class__.__name__, self.id, repr(self.user),
                                             repr(self.network), repr(self.group))


class IdentityRole(BaseModel):
    """
    Assignment of a role to an identity.

    Attributes:
        group (.IdentityGroup):
            Containing group instance.
        role (str):
            Plain role identifier.
    """

    group = ForeignKeyField(IdentityGroup, related_name="roles", on_delete="cascade")
    role = CharField()

    def __repr__(self):
        return "<{}: #{} {} {}>".format(self.__class__.__name__, self.id, repr(self.role),
                                        repr(self.group))


class LocalIdentityHook(immp.Hook, AccessPredicate, IdentityProvider):
    """
    Hook for managing physical users with multiple logical links across different plugs.  This
    effectively provides self-service identities, as opposed to being provided externally.
    """

    schema = immp.Schema({immp.Optional("instance"): immp.Nullable(int),
                          "plugs": [str],
                          immp.Optional("multiple", True): bool})

    _plugs = immp.ConfigProperty([immp.Plug])

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        self.db = None

    async def start(self):
        if not self.config["instance"]:
            # Find a non-conflicting number and assign it.
            codes = {hook.config["instance"] for hook in self.host.hooks.values()
                     if isinstance(hook, self.__class__)}
            code = 1
            while code in codes:
                code += 1
            log.debug("Assigning instance code %d to hook %r", code, self.name)
            self.config["instance"] = code
        self.db = self.host.resources[DatabaseHook].db
        self.db.create_tables([IdentityGroup, IdentityLink, IdentityRole], safe=True)

    def get(self, name):
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
            return IdentityGroup.select_links().where(IdentityGroup.name == name).get()
        except IdentityGroup.DoesNotExist:
            return None

    def find(self, user):
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
            return (IdentityGroup.select_links()
                                 .where(IdentityGroup.instance == self.config["instance"],
                                        IdentityLink.network == user.plug.network_id,
                                        IdentityLink.user == user.id).get())
        except IdentityGroup.DoesNotExist:
            return None

    async def channel_access(self, channel, user):
        return bool(IdentityLink.get(network=user.plug.network_id, user=user.id))

    async def identity_from_name(self, name):
        group = self.get(name)
        return await group.to_identity(self.host) if group else None

    async def identity_from_user(self, user):
        group = self.find(user)
        return await group.to_identity(self.host) if group else None

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
                group = IdentityGroup.get(instance=self.config["instance"], name=name)
                exists = True
            except IdentityGroup.DoesNotExist:
                group = IdentityGroup.create(instance=self.config["instance"], name=name, pwd=pwd)
            if exists and not group.pwd == pwd:
                text = "{} Password incorrect".format(CROSS)
            elif not self.config["multiple"] and any(link.network == msg.user.plug.network_id
                                                     for link in group.links):
                text = "{} Already identified on {}".format(CROSS, msg.user.plug.network_name)
            else:
                IdentityLink.create(group=group, network=msg.user.plug.network_id,
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
        group = self.find(msg.user)
        if not group:
            text = "{} Not identified".format(CROSS)
        elif group.name == name:
            text = "{} No change".format(TICK)
        elif IdentityGroup.select().where(IdentityGroup.instance == self.config["instance"],
                                          IdentityGroup.name == name).exists():
            text = "{} Name already in use".format(CROSS)
        else:
            group.name = name
            group.save()
            text = "{} Claimed".format(TICK)
        await msg.channel.send(immp.Message(text=text))

    @command("id-password", scope=CommandScope.private, test=_test)
    async def password(self, msg, pwd):
        """
        Update the password for the current identity.
        """
        if not msg.user:
            return
        group = self.find(msg.user)
        if not group:
            text = "{} Not identified".format(CROSS)
        else:
            group.pwd = IdentityGroup.hash(pwd)
            group.save()
            text = "{} Changed".format(TICK)
        await msg.channel.send(immp.Message(text=text))

    @command("id-reset", scope=CommandScope.private, test=_test)
    async def reset(self, msg):
        """
        Delete the current identity and all linked users.
        """
        if not msg.user:
            return
        group = self.find(msg.user)
        if not group:
            text = "{} Not identified".format(CROSS)
        else:
            group.delete_instance()
            text = "{} Reset".format(TICK)
        await msg.channel.send(immp.Message(text=text))

    @command("id-role", scope=CommandScope.private, role=CommandRole.admin, test=_test)
    async def role(self, msg, name, role=None):
        """
        List roles assigned to an identity, or add/remove a given role.
        """
        try:
            group = IdentityGroup.get(instance=self.config["instance"], name=name)
        except IdentityGroup.DoesNotExist:
            text = "{} Name not registered".format(CROSS)
        else:
            if role:
                count = IdentityRole.delete().where(IdentityRole.group == group,
                                                    IdentityRole.role == role).execute()
                if count:
                    text = "{} Removed".format(TICK)
                else:
                    IdentityRole.create(group=group, role=role)
                    text = "{} Added".format(TICK)
            else:
                roles = IdentityRole.select().where(IdentityRole.group == group)
                if roles:
                    labels = [role.role for role in roles]
                    text = "Roles for {}: {}".format(name, ", ".join(labels))
                else:
                    text = "No roles for {}.".format(name)
        await msg.channel.send(immp.Message(text=text))
