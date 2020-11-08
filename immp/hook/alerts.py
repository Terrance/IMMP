"""
Fallback mention and word highlight support for plugs via private channels.

Mentions
~~~~~~~~

Config:
    plugs (str list):
        List of plug names to enable mention alerts for.
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
        List of plug names to enable subscription alerts for.

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
    This hook requires an active :class:`.AsyncDatabaseHook` to store data.
"""

from asyncio import wait
from collections import defaultdict
import logging
import re

from tortoise import Model
from tortoise.exceptions import DoesNotExist
from tortoise.fields import ForeignKeyField, TextField

import immp
from immp.hook.command import CommandScope, command
from immp.hook.database import AsyncDatabaseHook
from immp.hook.sync import SyncPlug


log = logging.getLogger(__name__)


CROSS = "\N{CROSS MARK}"
TICK = "\N{WHITE HEAVY CHECK MARK}"


class _Skip(Exception):
    # Message isn't applicable to the hook.
    pass


class SubTrigger(Model):
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

    network = TextField()
    user = TextField()
    text = TextField()

    def __repr__(self):
        return "<{}: #{} {} ({} @ {})>".format(self.__class__.__name__, self.id, repr(self.text),
                                               repr(self.user), repr(self.network))


class SubExclude(Model):
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

    trigger = ForeignKeyField("db.SubTrigger", "excludes")
    network = TextField()
    channel = TextField()

    def __repr__(self):
        if isinstance(self.trigger, SubTrigger):
            trigger = repr(self.trigger)
        else:
            trigger = "<{}: #{}>".format(SubTrigger.__name__, self.trigger_id)
        return "<{}: #{} {} @ {} {}>".format(self.__class__.__name__, self.id, repr(self.network),
                                             repr(self.channel), trigger)


class _AlertHookBase(immp.Hook):

    schema = immp.Schema({"groups": [str]})

    group = immp.Group.MergedProperty("groups")

    async def _get_members(self, msg):
        # Sync integration: avoid duplicate notifications inside and outside a synced channel.
        # Commands and excludes should apply to the sync, but notifications are based on the
        # network-native channel.
        if isinstance(msg.channel.plug, SyncPlug):
            # We're in the sync channel, so we've already handled this event in native channels.
            log.debug("Ignoring sync channel: %r", msg.channel)
            raise _Skip
        channel = msg.channel
        synced = SyncPlug.any_sync(self.host, msg.channel)
        if synced:
            # We're in the native channel of a sync, use this channel for reading config.
            log.debug("Translating sync channel: %r -> %r", msg.channel, synced)
            channel = synced
        members = [user for user in (await msg.channel.members()) or []
                   if self.group.has_plug(user.plug)]
        if not members:
            raise _Skip
        return channel, members


class MentionsHook(_AlertHookBase):
    """
    Hook to send mention alerts via private channels.
    """

    schema = immp.Schema({immp.Optional("usernames", True): bool,
                          immp.Optional("real-names", False): bool,
                          immp.Optional("ambiguous", False): bool}, _AlertHookBase.schema)

    @staticmethod
    def _clean(text):
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
        name = self._clean(mention)
        real_matches = set()
        real_partials = set()
        for member in members:
            if self.config["usernames"] and self._clean(member.username) == name:
                # Assume usernames are unique, only match the corresponding user.
                return {member}
            if self.config["real-names"]:
                real = self._clean(member.real_name)
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

    async def before_receive(self, sent, source, primary):
        await super().on_receive(sent, source, primary)
        if not primary or not source.text or await sent.channel.is_private():
            return sent
        try:
            _, members = await self._get_members(sent)
        except _Skip:
            return sent
        for match in re.finditer(r"@\S+", str(source.text)):
            mention = match.group(0)
            matches = self.match(mention, members)
            if len(matches) == 1:
                target = next(iter(matches))
                log.debug("Exact match for mention %r: %r", mention, target)
                text = sent.text[match.start():match.end():True]
                for segment in text:
                    segment.mention = target
                    sent.text = (sent.text[:match.start():True] + text +
                                 sent.text[match.end()::True])
        return sent

    async def on_receive(self, sent, source, primary):
        await super().on_receive(sent, source, primary)
        if not primary or not source.text or await sent.channel.is_private():
            return
        try:
            _, members = await self._get_members(sent)
        except _Skip:
            return
        mentioned = set()
        for mention in re.findall(r"@\S+", str(source.text)):
            matches = self.match(mention, members)
            if matches:
                log.debug("Mention %r applies: %r", mention, matches)
                mentioned.update(matches)
            else:
                log.debug("Mention %r doesn't apply", mention)
        for segment in source.text:
            if segment.mention and segment.mention in members:
                log.debug("Segment mention %r applies: %r", segment.text, segment.mention)
                mentioned.add(segment.mention)
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


class SubscriptionsHook(_AlertHookBase):
    """
    Hook to send trigger word alerts via private channels.
    """

    def on_load(self):
        self.host.resources[AsyncDatabaseHook].add_models(SubTrigger, SubExclude)

    @classmethod
    def _clean(cls, text):
        return re.sub(r"[^\w ]", "", text).lower()

    def _test(self, channel, user):
        return self.group.has_plug(channel.plug)

    @command("sub-add", scope=CommandScope.private, test=_test)
    async def add(self, msg, *words):
        """
        Add a subscription to your trigger list.
        """
        text = re.sub(r"[^\w ]", "", " ".join(words)).lower()
        _, created = await SubTrigger.get_or_create(network=msg.channel.plug.network_id,
                                                    user=msg.user.id, text=text)
        resp = "{} {}".format(TICK, "Subscribed" if created else "Already subscribed")
        await msg.channel.send(immp.Message(text=resp))

    @command("sub-remove", scope=CommandScope.private, test=_test)
    async def remove(self, msg, *words):
        """
        Remove a subscription from your trigger list.
        """
        text = re.sub(r"[^\w ]", "", " ".join(words)).lower()
        count = await SubTrigger.filter(network=msg.channel.plug.network_id,
                                        user=msg.user.id,
                                        text=text).delete()
        resp = "{} {}".format(TICK, "Unsubscribed" if count else "Not subscribed")
        await msg.channel.send(immp.Message(text=resp))

    @command("sub-list", scope=CommandScope.private, test=_test)
    async def list(self, msg):
        """
        Show all active subscriptions.
        """
        subs = await SubTrigger.filter(network=msg.user.plug.network_id,
                                       user=msg.user.id).order_by("text")
        if subs:
            text = immp.RichText([immp.Segment("Your subscriptions:", bold=True)])
            for sub in subs:
                text.append(immp.Segment("\n- {}".format(sub.text)))
        else:
            text = "No active subscriptions."
        await msg.channel.send(immp.Message(text=text))

    @command("sub-exclude", scope=CommandScope.shared, test=_test)
    async def exclude(self, msg, *words):
        """
        Don't trigger a specific subscription in the current channel.
        """
        text = re.sub(r"[^\w ]", "", " ".join(words)).lower()
        try:
            trigger = await SubTrigger.get(network=msg.user.plug.network_id,
                                           user=msg.user.id, text=text)
        except DoesNotExist:
            resp = "{} Not subscribed".format(CROSS)
        else:
            exclude, created = await SubExclude.get_or_create(trigger=trigger,
                                                              network=msg.channel.plug.network_id,
                                                              channel=msg.channel.source)
            if not created:
                await exclude.delete()
            resp = "{} {}".format(TICK, "Excluded" if created else "No longer excluded")
        await msg.channel.send(immp.Message(text=resp))

    @staticmethod
    async def match(text, channel, present):
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
        for sub in await SubTrigger.all():
            key = (sub.network, sub.user)
            if key in present and sub.text in text:
                subs.add(sub)
        triggered = defaultdict(set)
        excludes = await SubExclude.filter(trigger__id__in=tuple(sub.id for sub in subs),
                                           network=channel.plug.network_id,
                                           channel=channel.source)
        excluded = set(exclude.trigger.text for exclude in excludes)
        for trigger in subs:
            if trigger.text not in excluded:
                triggered[present[(trigger.network, trigger.user)]].add(trigger.text)
        return triggered

    async def channel_migrate(self, old, new):
        count = (await SubExclude.filter(network=old.plug.network_id, channel=old.source)
                                 .update(network=new.plug.network_id, channel=new.source))
        return count > 0

    async def on_receive(self, sent, source, primary):
        await super().on_receive(sent, source, primary)
        if not primary or not source.text or await sent.channel.is_private():
            return
        try:
            lookup, members = await self._get_members(sent)
        except _Skip:
            return
        present = {(member.plug.network_id, str(member.id)): member for member in members}
        triggered = await self.match(self._clean(str(source.text)), lookup, present)
        if not triggered:
            return
        tasks = []
        for member, triggers in triggered.items():
            if member == source.user:
                continue
            private = await member.private_channel()
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
