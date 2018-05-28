"""
Basic identity management for users in different networks.

Config:
    plugs (str list):
        List of plug names to accept identities for.

Commands:
    id-show <name>:
        Recall a known identity and all of its links.
    id-add <name> <pwd>:
        Create a new identity, or link to an existing one from a second user.
    id-rename <name>:
        Rename the current identity.
    id-reset:
        Delete the current identity and all linked users.

.. note::
    This hook requires an active :class:`.DatabaseHook` to store data.
"""

from hashlib import sha256

from peewee import CharField, ForeignKeyField
from voluptuous import ALLOW_EXTRA, Schema

import immp
from immp.hook.command import Commandable
from immp.hook.database import BaseModel, DatabaseHook


CROSS = "\U0000274C"
TICK = "\U00002705"


class _Schema(object):

    config = Schema({"plugs": [str]}, extra=ALLOW_EXTRA, required=True)


class IdentityGroup(BaseModel):
    """
    Representation of a single identity.

    Attributes:
        name (str):
            Unique display name.
        pwd (str):
            Hashed password, used by the user to authenticate when linking identities.
        links (.IdentityLink iterable):
            All links contained by this group.
    """

    name = CharField(unique=True)
    pwd = CharField()

    @classmethod
    def hash(cls, pwd):
        return sha256(pwd.encode("utf-8")).hexdigest()

    @classmethod
    def select_links(cls):
        return cls.select(cls, IdentityLink).join(IdentityLink)


class IdentityLink(BaseModel):
    """
    Single link between an identity and a user.

    Attributes:
        group (.IdentityGroup):
            Containing group instance.
        plug (.Plug):
            Plug that this user belongs to.
        user (str):
            User identifier as given by the plug.
    """

    group = ForeignKeyField(IdentityGroup, related_name="links")
    plug = CharField()
    user = CharField()


class IdentityHook(immp.Hook, Commandable):
    """
    Hook for managing physical users with multiple logical links across different plugs.
    """

    def __init__(self, name, config, host):
        super().__init__(name, _Schema.config(config), host)
        self.plugs = []
        for label in self.config["plugs"]:
            try:
                self.plugs.append(host.plugs[label])
            except KeyError:
                raise immp.ConfigError("No plug '{}' on host".format(label)) from None

    def commands(self):
        return {"id-show": self.show,
                "id-add": self.add,
                "id-rename": self.rename,
                "id-reset": self.reset}

    async def start(self):
        self.db = self.host.resources[DatabaseHook].db
        self.db.create_tables([IdentityGroup, IdentityLink], safe=True)

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
                                 .where(IdentityLink.plug == user.plug.name,
                                        IdentityLink.user == user.id).get())
        except IdentityGroup.DoesNotExist:
            return None

    async def show(self, channel, msg, name):
        try:
            group = (IdentityGroup.select_links()
                                  .where(IdentityGroup.name == name).get())
        except IdentityGroup.DoesNotExist:
            text = "{} Name not in use".format(CROSS)
        else:
            text = immp.RichText([immp.Segment(name, bold=True),
                                  immp.Segment(" may appear as:")])
            for link in group.links:
                plug = self.host.plugs[link.plug]
                user = await plug.user_from_id(link.user)
                text.append(immp.Segment("\n"))
                text.append(immp.Segment("({}) ".format(plug.Meta.network)))
                if user:
                    if user.link:
                        text.append(immp.Segment(user.real_name or user.username, link=user.link))
                    elif user.real_name and user.username:
                        text.append(immp.Segment("{} [{}]".format(user.real_name, user.username)))
                    else:
                        text.append(immp.Segment(user.real_name or user.username))
                else:
                    text.append(immp.Segment(link.user, code=True))
        await channel.send(immp.Message(text=text))

    async def add(self, channel, msg, name, pwd=None):
        if not msg.user or msg.user.plug not in self.plugs:
            return
        if self.find(msg.user):
            text = "{} Already identified".format(CROSS)
        elif not pwd:
            text = "{} Password required".format(CROSS)
        else:
            pwd = IdentityGroup.hash(pwd)
            exists = False
            try:
                group = IdentityGroup.get(name=name)
                exists = True
            except IdentityGroup.DoesNotExist:
                group = IdentityGroup.create(name=name, pwd=pwd)
            if exists and not group.pwd == pwd:
                text = "{} Password incorrect".format(CROSS)
            else:
                IdentityLink.create(group=group, plug=msg.user.plug.name, user=msg.user.id)
                text = "{} {}".format(TICK, "Added" if exists else "Claimed")
        await channel.send(immp.Message(text=text))

    async def rename(self, channel, msg, name):
        if not msg.user:
            return
        group = self.find(msg.user)
        if not group:
            text = "{} Not identified".format(CROSS)
        elif group.name == name:
            text = "{} No change".format(TICK)
        elif IdentityGroup.select().where(IdentityGroup.name == name).exists():
            text = "{} Name already in use".format(CROSS)
        else:
            group.name = name
            group.save()
            text = "\U00002705 Claimed"
        await channel.send(immp.Message(text=text))

    async def reset(self, channel, msg):
        if not msg.user:
            return
        group = self.find(msg.user)
        if not group:
            text = "{} Not identified".format(CROSS)
        else:
            group.delete()
            text = "{} Reset".format(TICK)
        await channel.send(immp.Message(text=text))