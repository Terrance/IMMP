"""
Fallback mention and word highlight support for plugs via private channels.

Mentions
~~~~~~~~

Config:
    plugs (str list):
        List of plug names to enable mentions for.
    usernames (bool):
        Whether to match network usernames (``True`` by default).
    real-names (bool):
        Whether to match user's display names (``False`` by default).
    ambiguous (bool):
        Whether to notify multiple potential users of an ambiguous mention (``False`` by default).

For networks that don't provide native user mentions, this plug can send users a private message
when mentioned by their username or real name.

A mention is matched from each ``@`` sign until whitespace is encountered.  For real names, spaces
and special characters are ignored, so that e.g. ``@fredbloggs`` will match *Fred Bloggs*.

Partial mentions are supported, failing any exact matches, by basic prefix search on real names.
For example, ``@fred`` will match *Frederick*, and ``@fredb`` will match *Fred Bloggs*.

Subscriptions
~~~~~~~~~~~~~

Config:
    plugs (str list):
        List of plug names to enable mentions for.

Commands:
    sub-add <text>:
        Add a subscription to your trigger list.
    sub-remove <text>:
        Remove a subscription from your trigger list.
    sub-exclude <text>:
        Don't trigger a specific subscription in the current public channel.
    sub-list:
        Show all active subscriptions.

Allows users to opt in to private message notifications when chosen highlight words are used in a
group conversation.

.. note::
    This hook requires an active :class:`.DatabaseHook` to store data.
"""

from asyncio import wait
from collections import defaultdict
import logging
import re

from peewee import CharField, ForeignKeyField
from voluptuous import ALLOW_EXTRA, Optional, Schema

import immp
from immp.hook.command import Command, Commandable, CommandScope
from immp.hook.database import BaseModel, DatabaseHook

log = logging.getLogger(__name__)


CROSS = "\N{CROSS MARK}"
TICK = "\N{WHITE HEAVY CHECK MARK}"


class _Schema:

    config = Schema({"plugs": [str],
                     Optional("usernames", default=True): bool,
                     Optional("real-names", default=False): bool,
                     Optional("ambiguous", default=False): bool},
                    extra=ALLOW_EXTRA, required=True)


class SubTrigger(BaseModel):
    """
    Individual subscription trigger phrase for an individual user.

    Attributes:
        network (str):
            Network identifier that the user belongs to.
        user (str):
            User identifier as given by the plug.
        text (str):
            Subscription text that they wish to be notified on.
    """

    network = CharField()
    user = CharField()
    text = CharField()

    def __repr__(self):
        return "<{}: #{} {} ({} @ {})>".format(self.__class__.__name__, self.id, repr(self.text),
                                               repr(self.user), repr(self.network))


class SubExclude(BaseModel):
    """
    Exclusion for a trigger in a specific channel.

    Attributes:
        trigger (.SubTrigger):
            Containing trigger instance.
        network (str):
            Network identifier that the channel belongs to.
        user (str):
            Channel's own identifier.
    """

    trigger = ForeignKeyField(model=SubTrigger, related_name="excludes")
    network = CharField()
    channel = CharField()

    def __repr__(self):
        return "<{}: #{} {} @ {} {}>".format(self.__class__.__name__, self.id, repr(self.user),
                                             repr(self.network), repr(self.trigger))


@immp.config_props("plugs")
class _AlertHookBase(immp.Hook):

    def __init__(self, name, config, host):
        super().__init__(name, _Schema.config(config), host)


class MentionsHook(_AlertHookBase):
    """
    Hook to send mention alerts via private channels.
    """

    @staticmethod
    def clean(text):
        return re.sub(r"\W", "", text).lower() if text else None

    def match(self, mention, members):
        """
        Identify users relevant to a mention.

        Args:
            mention (str):
                Raw mention text, e.g. ``@fred``.
            members (.User list):
                List of members in the channel where the mention took place.

        Returns:
            .User set:
                All applicable members to be notified.
        """
        name = self.clean(mention)
        real_matches = set()
        real_partials = set()
        for member in members:
            if self.config["usernames"] and self.clean(member.username) == name:
                # Assume usernames are unique, only match the corresponding user.
                return {member}
            if self.config["real-names"]:
                real = self.clean(member.real_name)
                if real == name:
                    real_matches.add(member)
                if real.startswith(name):
                    real_partials.add(member)
        if real_matches:
            # Assume multiple identical real names is unlikely.
            # If it's the same person with two users, they both get mentioned.
            return real_matches
        elif len(real_partials) == 1 or self.config["ambiguous"]:
            # Return a single partial match if it exists.
            # Only allow multiple partials if enabled, else ignore the mention.
            return real_partials
        else:
            return set()

    async def on_receive(self, sent, source, primary):
        await super().on_receive(sent, source, primary)
        if not primary or not source.text:
            return
        if sent.channel.plug not in self.plugs or await sent.channel.is_private():
            return
        members = await sent.channel.plug.channel_members(sent.channel)
        if not members:
            return
        mentioned = set()
        for mention in re.findall(r"@\S+", str(source.text)):
            matches = self.match(mention, members)
            if matches:
                log.debug("Mention '{}' applies: {}".format(mention, matches))
                mentioned.update(matches)
            else:
                log.debug("Mention '{}' doesn't apply".format(mention))
        if not mentioned:
            return
        text = immp.RichText()
        if source.user:
            text.append(immp.Segment(source.user.real_name or source.user.username, bold=True),
                        immp.Segment(" mentioned you"))
        else:
            text.append(immp.Segment("You were mentioned"))
        title = await sent.channel.title()
        link = await sent.channel.link()
        if title:
            text.append(immp.Segment(" in "),
                        immp.Segment(title, italic=True))
        text.append(immp.Segment(":\n"))
        text += source.text
        if source.user and source.user.link:
            text.append(immp.Segment("\n"),
                        immp.Segment("Go to user", link=source.user.link))
        if link:
            text.append(immp.Segment("\n"),
                        immp.Segment("Go to channel", link=link))
        tasks = []
        for member in mentioned:
            if member == source.user:
                continue
            private = await sent.channel.plug.channel_for_user(member)
            if private:
                tasks.append(private.send(immp.Message(text=text)))
        if tasks:
            await wait(tasks)


class SubscriptionsHook(_AlertHookBase, Commandable):
    """
    Hook to send trigger word alerts via private channels.
    """

    @classmethod
    def clean(cls, text):
        return re.sub(r"[^\w ]", "", text).lower()

    async def start(self):
        self.db = self.host.resources[DatabaseHook].db
        self.db.create_tables([SubTrigger, SubExclude], safe=True)

    def commands(self):
        return [Command("sub-add", self.add, CommandScope.private, "<text>",
                        "Add a subscription to your trigger list."),
                Command("sub-remove", self.remove, CommandScope.private, "<text>",
                        "Remove a subscription from your trigger list."),
                Command("sub-list", self.list, CommandScope.private, None,
                        "Show all active subscriptions."),
                Command("sub-exclude", self.exclude, CommandScope.public, "<text>",
                        "Don't trigger a specific subscription in the current public channel.")]

    async def add(self, channel, msg, *words):
        text = re.sub(r"[^\w ]", "", " ".join(words)).lower()
        sub, created = SubTrigger.get_or_create(network=channel.plug.network_id,
                                                user=msg.user.id, text=text)
        resp = "{} {}".format(TICK, "Subscribed" if created else "Already subscribed")
        await channel.send(immp.Message(text=resp))

    async def remove(self, channel, msg, *words):
        text = re.sub(r"[^\w ]", "", " ".join(words)).lower()
        count = SubTrigger.delete().where(SubTrigger.network == channel.plug.network_id,
                                          SubTrigger.user == msg.user.id,
                                          SubTrigger.text == text).execute()
        resp = "{} {}".format(TICK, "Unsubscribed" if count else "Not subscribed")
        await channel.send(immp.Message(text=resp))

    async def exclude(self, channel, msg, *words):
        text = re.sub(r"[^\w ]", "", " ".join(words)).lower()
        try:
            trigger = SubTrigger.get(network=channel.plug.network_id,
                                     user=msg.user.id, text=text)
        except SubTrigger.DoesNotExist:
            resp = "{} Not subscribed".format(CROSS)
        else:
            exclude, created = SubExclude.get_or_create(trigger=trigger,
                                                        network=channel.plug.network_id,
                                                        channel=channel.source)
            if not created:
                exclude.delete_instance()
            resp = "{} {}".format(TICK, "Excluded" if created else "No longer excluded")
        await channel.send(immp.Message(text=resp))

    async def list(self, channel, msg):
        subs = sorted(SubTrigger.select().where(SubTrigger.network == channel.plug.network_id,
                                                SubTrigger.user == msg.user.id))
        if subs:
            resp = "Current subscriptions:{}".format("\n- {}".format(sub.text) for sub in subs)
        else:
            resp = "No active subscriptions."
        await channel.send(immp.Message(text=resp))

    def match(self, text, channel, present):
        """
        Identify users subscribed to text snippets in a message.

        Args:
            text (str):
                Cleaned message text.
            channel (.Channel):
                Channel where the subscriptions were triggered.
            present (((str, str), .User) dict):
                Mapping from network/user IDs to members of the source channel.

        Returns:
            (.User, str set) dict:
                Mapping from applicable users to their filtered triggers.
        """
        subs = set()
        for sub in SubTrigger.select():
            key = (sub.network, sub.user)
            if key in present and sub.text in text:
                subs.add(sub)
        triggered = defaultdict(set)
        excludes = set(SubExclude.select().where(SubExclude.trigger << subs,
                                                 SubExclude.network == channel.plug.network_id,
                                                 SubExclude.channel == channel.source))
        for trigger in subs - set(exclude.trigger for exclude in excludes):
            triggered[present[(trigger.network, trigger.user)]].add(trigger.text)
        return triggered

    async def on_receive(self, sent, source, primary):
        await super().on_receive(sent, source, primary)
        if not primary or not source.text:
            return
        if sent.channel.plug not in self.plugs or await sent.channel.is_private():
            return
        members = await sent.channel.plug.channel_members(sent.channel)
        if not members:
            return
        present = {(member.plug.network_id, str(member.id)): member for member in members}
        triggered = self.match(self.clean(str(source.text)), sent.channel, present)
        if not triggered:
            return
        tasks = []
        for member, triggers in triggered.items():
            if member == source.user:
                continue
            private = await sent.channel.plug.channel_for_user(member)
            if not private:
                continue
            text = immp.RichText()
            mentioned = immp.Segment(", ".join(sorted(triggers)), italic=True)
            if source.user:
                text.append(immp.Segment(source.user.real_name or source.user.username, bold=True),
                            immp.Segment(" mentioned "), mentioned)
            else:
                text.append(mentioned, immp.Segment(" mentioned"))
            title = await sent.channel.title()
            link = await sent.channel.link()
            if title:
                text.append(immp.Segment(" in "),
                            immp.Segment(title, italic=True))
            text.append(immp.Segment(":\n"))
            text += source.text
            if source.user and source.user.link:
                text.append(immp.Segment("\n"),
                            immp.Segment("Go to user", link=source.user.link))
            if link:
                text.append(immp.Segment("\n"),
                            immp.Segment("Go to channel", link=link))
            tasks.append(private.send(immp.Message(text=text)))
        if tasks:
            await wait(tasks)
