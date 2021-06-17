from asyncio import wait
from collections import defaultdict
import re

try:
    from tortoise import Model
    from tortoise.exceptions import DoesNotExist
    from tortoise.fields import ForeignKeyField, TextField
except ImportError:
    Model = None

import immp
from immp.hook.command import CommandScope, command
from immp.hook.database import AsyncDatabaseHook

from .common import AlertHookBase, Skip


CROSS = "\N{CROSS MARK}"
TICK = "\N{WHITE HEAVY CHECK MARK}"


if Model:

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
            return "<{}: #{} {} ({} @ {})>".format(self.__class__.__name__, self.id,
                                                   repr(self.text), repr(self.user),
                                                   repr(self.network))

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

        @classmethod
        def select_related(cls):
            return cls.all().prefetch_related("trigger")

        def __repr__(self):
            if isinstance(self.trigger, SubTrigger):
                trigger = repr(self.trigger)
            else:
                trigger = "<{}: #{}>".format(SubTrigger.__name__, self.trigger_id)
            return "<{}: #{} {} @ {} {}>".format(self.__class__.__name__, self.id,
                                                 repr(self.network), repr(self.channel), trigger)


class SubscriptionsHook(AlertHookBase):
    """
    Hook to send trigger word alerts via private channels.
    """

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        if not Model:
            raise immp.PlugError("'tortoise' module not installed")

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
        excludes = (await SubExclude.select_related()
                                    .filter(trigger__id__in=tuple(sub.id for sub in subs),
                                            network=channel.plug.network_id,
                                            channel=channel.source))
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
        except Skip:
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
