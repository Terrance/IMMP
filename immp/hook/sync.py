"""
Bridge multiple channels into a single unified conversation.

Config:
    channels ((str, str list) dict):
        Mapping from virtual channel names to lists of channel names to bridge.
    plug (str):
        Name of a virtual plug to register for this sync.

Commands:
    sync-members:
        List all members of the current conversation, across all channels.

When a message is received from any of the listed channels, a copy is pushed to all other channels
participating in the bridge.

If ``plug`` is specified, a virtual plug is registered under that name, with a channel for each
defined bridge.  Other hooks may reference these channels, to work with all channels in that sync
as one.  This allows them to listen to a unified stream of messages, or push new messages to all
synced channels.
"""

from asyncio import BoundedSemaphore, gather
from collections import defaultdict
import logging

from voluptuous import ALLOW_EXTRA, Any, Optional, Schema

import immp
from immp.hook.command import Command, Commandable, CommandScope


log = logging.getLogger(__name__)


class _Schema:

    config = Schema({"channels": {str: [str]},
                     Optional("plug", default=None): Any(str, None)},
                    extra=ALLOW_EXTRA, required=True)


class SyncPlug(immp.Plug):
    """
    Virtual plug that allows sending external messages to a synced conversation.
    """

    network_name = "Sync"

    @property
    def network_id(self):
        return "sync:{}".format(self.name)

    def __init__(self, name, hook, host):
        super().__init__(name, hook.config, host, virtual=True)
        self._hook = hook

    async def channel_is_private(self, channel):
        return False if channel.source in self.config["channels"] else None

    async def channel_members(self, channel):
        if channel.source not in self.config["channels"]:
            return None
        members = []
        for synced in self._hook.channels[channel.source]:
            members.extend(await synced.members() or [])
        return members

    async def send(self, channel, msg):
        if channel.source in self.config["channels"]:
            await self._hook.send(channel.source, msg)
            return []
        else:
            raise immp.PlugError("Send to unknown sync channel: {}".format(repr(channel)))


class SyncHook(immp.Hook, Commandable):
    """
    Hook to propagate messages between two or more channels.

    Attributes:
        plug (.SyncPlug):
            Virtual plug for this sync, if configured.
    """

    def __init__(self, name, config, host):
        super().__init__(name, _Schema.config(config), host)
        # Message cache, stores IDs of all synced messages by channel.  Mapping from source
        # messages to [{channel: [ID, ...], ...}] (source IDs may not be unique across networks).
        self._synced = {}
        # Hook lock, to put a hold on retrieving messages whilst a send is in progress.
        self._lock = BoundedSemaphore()
        # Add a virtual plug to the host, for external subscribers.
        if self.config["plug"]:
            log.debug("Creating virtual plug: {}".format(repr(self.config["plug"])))
            self.plug = SyncPlug(self.config["plug"], self, host)
            host.add_plug(self.plug)
            for label in self.config["channels"]:
                host.add_channel(label, immp.Channel(self.plug, label))
        else:
            self.plug = None

    @property
    def channels(self):
        try:
            return {virtual: [self.host.channels[label] for label in labels]
                    for virtual, labels in self.config["channels"].items()}
        except KeyError as e:
            raise immp.ConfigError("No channel {} on host".format(repr(e.args[0]))) from None

    def label_for_channel(self, channel):
        labels = []
        for label, channels in self.channels.items():
            if channel in channels:
                labels.append(label)
        if not labels:
            raise immp.ConfigError("Channel {} not bridged".format(repr(channel)))
        elif len(labels) > 1:
            raise immp.ConfigError("Channel {} defined more than once".format(repr(channel)))
        else:
            return labels[0]

    def commands(self):
        return [Command("sync-members", self.members, CommandScope.any, None,
                        "List all members of the current conversation, across all channels.")]

    async def members(self, channel, msg):
        members = defaultdict(list)
        missing = False
        for synced in self.channels[channel.source]:
            local = (await synced.plug.channel_members(synced))
            if local:
                members[synced.plug.network_name] += local
            else:
                missing = True
        if not members:
            return
        text = immp.RichText([immp.Segment("Members of this conversation:")])
        for network in sorted(members):
            text.append(immp.Segment("\n{}".format(network), bold=True))
            for member in sorted(members[network],
                                 key=lambda member: member.real_name or member.username):
                name = member.real_name or member.username
                text.append(immp.Segment("\n"))
                if member.link:
                    text.append(immp.Segment(name, link=member.link))
                elif member.real_name and member.username:
                    text.append(immp.Segment("{} [{}]".format(name, member.username)))
                else:
                    text.append(immp.Segment(name))
        if missing:
            text.append(immp.Segment("\n"),
                        immp.Segment("(list may be incomplete)"))
        await channel.send(immp.Message(user=immp.User(real_name="Sync"), text=text))

    async def _noop_send(self, msg):
        return [msg.id]

    async def _send(self, channel, msg):
        try:
            return await channel.send(msg)
        except Exception:
            log.exception("Failed to relay message to channel: {}".format(repr(channel)))
            return []

    async def send(self, label, msg, source=None):
        """
        Send a message to all channels in this sync.

        Args:
            label (str):
                Bridge that defines the underlying synced channels to send to.
            msg (.Message):
                External message to push.
            source (.Channel):
                Source channel of the message; if set and part of the sync, it will be skipped
                (used to avoid retransmitting a message we just received).
        """
        queue = []
        for synced in self.channels[label]:
            # If it's the channel we just got the message from, return the ID without resending.
            queue.append(self._noop_send(msg) if synced == source else self._send(synced, msg))
        # Just like with plugs, when sending a new (external) message to all channels in a
        # sync, we need to wait for all plugs to complete before processing further messages.
        with (await self._lock):
            channels = self.channels[label]
            # Send all the messages in parallel, and match the resulting IDs up by channel.
            ids = defaultdict(list, zip(channels, await gather(*queue)))
            revisions = defaultdict(list)
            if source:
                revisions[source].append((msg.id, msg.revision))
            self._synced[msg] = (ids, revisions)

    async def on_receive(self, channel, msg, source, primary):
        await super().on_receive(channel, msg, source, primary)
        try:
            label = self.label_for_channel(channel)
        except immp.ConfigError:
            return
        with (await self._lock):
            # No critical section here, just wait for any pending messages to be sent.
            pass
        pair = (msg.id, msg.revision)
        if source in self._synced:
            ids, revisions = self._synced[source]
            if msg.id in ids[channel]:
                if pair in revisions[channel]:
                    # This is a synced message being echoed back from another channel.
                    log.debug("Ignoring synced revision: {}/{}".format(*pair))
                    return
                revisions[channel].append(pair)
                if len(revisions[channel]) <= len(ids[channel]):
                    log.debug("Ignoring initial revision: {}/{}".format(*pair))
                    return
        log.debug("Sending message to synced channel {}: {}/{}".format(repr(label), *pair))
        await self.send(label, source, channel)
        # Push a copy of the message to the sync channel, if running.
        if self.plug:
            self.plug.queue(immp.Channel(self.plug, label), source)
