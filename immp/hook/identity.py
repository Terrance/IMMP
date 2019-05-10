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
    id-show <name>:
        Recall a known identity and all of its links.
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
from voluptuous import ALLOW_EXTRA, Any, Optional, Schema

import immp
from immp.hook.access import AccessPredicate
from immp.hook.command import CommandRole, CommandScope, command
from immp.hook.database import BaseModel, DatabaseHook


CROSS = "\N{CROSS MARK}"
TICK = "\N{WHITE HEAVY CHECK MARK}"


log = logging.getLogger(__name__)


class _Schema:

    config = Schema({Optional("instance", default=None): Any(int, None),
                     "plugs": [str],
                     Optional("multiple", default=True): bool},
                    extra=ALLOW_EXTRA, required=True)


@immp.pretty_str
class Identity:
    """
    Basic representation of an external identity.

    Attributes:
        name (str):
            Common name used across any linked platforms.
        provider (.IdentityProvider):
            Service hook where the identity information was acquired from.
        links (User list):
            Physical platform users assigned to this identity.
        roles (str list):
            Optional set of role names, if applicable to the backend.
    """

    def __init__(self, name, provider=None, links=(), roles=()):
        self.name = name
        self.provider = provider
        self.links = links
        self.roles = roles

    def __eq__(self, other):
        return (isinstance(other, self.__class__) and
                (self.name, self.provider) == (other.name, other.provider))

    def __hash__(self):
        return hash(self.name, self.provider)

    def __repr__(self):
        return "<{}: {} x{}{}>".format(self.__class__.__name__, repr(self.name), len(self.links),
                                       " ({})".format(" ".join(self.roles)) if self.roles else "")


class IdentityProvider:
    """
    Interface for hooks to provide identity information from a backing source.
    """

    async def identity_from_name(self, name):
        """
        Look up an identity by the external provider's username for them.

        Args:
            name (str):
                External name to query.

        Returns:
            .Identity:
                Matching identity from the provider, or ``None`` if not found.
        """
        raise NotImplementedError

    async def identity_from_user(self, user):
        """
        Look up an identity by a linked network user.

        Args:
            user (.User):
                Plug user referenced by the identity.

        Returns:
            .Identity:
                Matching identity from the provider, or ``None`` if not found.
        """
        raise NotImplementedError


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


class IdentityHook(immp.Hook, AccessPredicate, IdentityProvider):
    """
    Hook for managing physical users with multiple logical links across different plugs.  This
    effectively provides self-service identities, as opposed to being provided externally.
    """

    plugs = immp.ConfigProperty("plugs", [immp.Plug])

    def __init__(self, name, config, host):
        super().__init__(name, _Schema.config(config), host)
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
        if not user or user.plug not in self.plugs:
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
        return channel.plug in self.plugs

    @command("id-show", test=_test)
    async def show(self, msg, name):
        """
        Recall a known identity and all of its links.
        """
        try:
            group = (IdentityGroup.select_links()
                                  .where(IdentityGroup.instance == self.config["instance"],
                                         IdentityGroup.name == name).get())
        except IdentityGroup.DoesNotExist:
            text = "{} Name not in use".format(CROSS)
        else:
            text = immp.RichText([immp.Segment(name, bold=True),
                                  immp.Segment(" may appear as:")])
            for link in group.links:
                for plug in self.host.plugs.values():
                    if plug.network_id == link.network:
                        break
                else:
                    continue
                user = await plug.user_from_id(link.user)
                text.append(immp.Segment("\n"))
                text.append(immp.Segment("({}) ".format(plug.network_name)))
                if user:
                    if user.link:
                        text.append(immp.Segment(user.real_name or user.username, link=user.link))
                    elif user.real_name and user.username:
                        text.append(immp.Segment("{} [{}]".format(user.real_name, user.username)))
                    else:
                        text.append(immp.Segment(user.real_name or user.username))
                else:
                    text.append(immp.Segment(link.user, code=True))
        await msg.channel.send(immp.Message(text=text))

    @command("id-add", scope=CommandScope.private, test=_test)
    async def add(self, msg, name, pwd):
        """
        Create a new identity, or link to an existing one from a second user.
        """
        if not msg.user or msg.user.plug not in self.plugs:
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
