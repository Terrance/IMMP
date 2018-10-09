"""
Basic identity management for users in different networks.

Config:
    instance (int):
        Unique instance code.
    plugs (str list):
        List of plug names to accept identities for.

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

from hashlib import sha256
import logging

from peewee import CharField, ForeignKeyField, IntegerField
from voluptuous import ALLOW_EXTRA, Any, Optional, Schema

import immp
from immp.hook.command import CommandRole, CommandScope, command
from immp.hook.database import BaseModel, DatabaseHook


CROSS = "\N{CROSS MARK}"
TICK = "\N{WHITE HEAVY CHECK MARK}"


log = logging.getLogger(__name__)


class _Schema:

    config = Schema({Optional("instance", default=None): Any(int, None),
                     "plugs": [str]},
                    extra=ALLOW_EXTRA, required=True)


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

    group = ForeignKeyField(IdentityGroup, related_name="links")
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

    group = ForeignKeyField(IdentityGroup, related_name="roles")
    role = CharField()

    def __repr__(self):
        return "<{}: #{} {} {}>".format(self.__class__.__name__, self.id, repr(self.role),
                                        repr(self.group))


@immp.config_props("plugs")
class IdentityHook(immp.Hook):
    """
    Hook for managing physical users with multiple logical links across different plugs.
    """

    def __init__(self, name, config, host):
        super().__init__(name, _Schema.config(config), host)

    async def start(self):
        if not self.config["instance"]:
            # Find a non-conflicting number and assign it.
            codes = {hook.config["instance"] for hook in self.host.hooks.values()
                     if isinstance(hook, self.__class__)}
            code = 1
            while code in codes:
                code += 1
            log.debug("Assigning instance code {} to hook '{}'".format(code, self.name))
            self.config["instance"] = code
        self.db = self.host.resources[DatabaseHook].db
        self.db.create_tables([IdentityGroup, IdentityLink, IdentityRole], safe=True)

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

    @command("id-show")
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

    @command("id-add", scope=CommandScope.private)
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
                group = IdentityGroup.create(name=name, pwd=pwd)
            if exists and not group.pwd == pwd:
                text = "{} Password incorrect".format(CROSS)
            else:
                IdentityLink.create(group=group, network=msg.user.plug.network_id,
                                    user=msg.user.id)
                text = "{} {}".format(TICK, "Added" if exists else "Claimed")
        await msg.channel.send(immp.Message(text=text))

    @command("id-rename", scope=CommandScope.private)
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

    @command("id-password", scope=CommandScope.private)
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

    @command("id-reset", scope=CommandScope.private)
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
            group.delete()
            text = "{} Reset".format(TICK)
        await msg.channel.send(immp.Message(text=text))

    @command("id-role", scope=CommandScope.private, role=CommandRole.admin)
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
