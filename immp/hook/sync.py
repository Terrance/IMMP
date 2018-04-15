"""
Bridge multiple channels into a single unified conversation.

Config:
    channels (str list):
        List of channel names to manage.
    plug (str):
        Name of a virtual plug to register for this sync.

When a message is received from any of the listed channels, a copy is pushed to all other channels
participating in the bridge.

If ``plug`` is specified, a virtual plug is registered under that name, with a single channel of
the same name.  Other hooks may reference this channel, to work with all channels in that sync as
one.  This allows them to listen to a unified stream of messages, or push new messages to all
synced channels.
"""

from asyncio import BoundedSemaphore, gather
from collections import defaultdict
import logging

from voluptuous import ALLOW_EXTRA, All, Any, Length, Optional, Schema

import immp
from immp.hook.command import Commandable


log = logging.getLogger(__name__)


class _Schema(object):

    config = Schema({"channels": All([str], Length(min=1)),
                     Optional("plug", default=None): Any(str, None)},
                    extra=ALLOW_EXTRA, required=True)


class SyncPlug(immp.Plug):
    """
    Virtual plug that allows sending external messages to a synced conversation.
    """

    def __init__(self, name, hook, host):
        super().__init__(name, {}, host)
        self._hook = hook

    async def send(self, channel, msg):
        if channel == self._hook.channel:
            return await self._hook.send(msg)


class SyncHook(immp.Hook, Commandable):
    """
    Hook to propagate messages between two or more channels.

    Attributes:
        plug (.SyncPlug):
            Virtual plug for this sync, if configured.
    """

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        config = _Schema.config(config)
        self.channels = []
        for channel in config["channels"]:
            try:
                self.channels.append(host.channels[channel])
            except KeyError:
                raise immp.ConfigError("No channel '{}' on host".format(channel)) from None
        # Message cache, stores mappings of synced message IDs keyed by channel.
        # [{Channel(): [id, ...], ...}, ...]
        self._synced = []
        # Hook lock, to put a hold on retrieving messages whilst a send is in progress.
        self._lock = BoundedSemaphore()
        # Add a virtual plug to the host, for external subscribers.
        if config["plug"]:
            tname = config["plug"]
            log.debug("Creating virtual plug '{}'".format(tname))
            self.plug = SyncPlug(tname, self, host)
            host.add_plug(self.plug)
            self.channel = immp.Channel(tname, self.plug, None)
            host.add_channel(self.channel)
        else:
            self.plug = None

    def commands(self):
        return {"sync-members": self.members}

    async def members(self, channel, mag):
        members = defaultdict(list)
        missing = False
        for synced in self.channels:
            local = (await synced.plug.channel_members(synced))
            if local:
                members[synced.plug.Meta.network] += local
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

    async def send(self, msg, source=None):
        """
        Send a message to all channels in this sync.

        Args:
            msg (.Message):
                External message to push.
            source (.Channel):
                Source channel of the message; if set and part of the sync, it will be skipped
                (used to avoid retransmitting a message we just received).

        Returns:
            dict:
                Mapping from destination channels to their generated message IDs.
        """
        queue = []
        for channel in self.channels:
            # If it's the channel we just got the message from, return the ID without resending.
            queue.append(self._noop_send(msg) if channel == source else channel.send(msg))
        # Just like with plugs, when sending a new (external) message to all channels in a
        # sync, we need to wait for all plugs to complete before processing further messages.
        with (await self._lock):
            # Send all the messages in parallel, and match the resulting IDs up by channel.
            sent = dict(zip(self.channels, await gather(*queue)))
            self._synced.append(sent)

    async def process(self, channel, msg):
        await super().process(channel, msg)
        # Only process if we recognise the channel.
        if channel not in self.channels:
            return
        with (await self._lock):
            # No critical section here, just wait for any pending messages to be sent.
            pass
        for sync in self._synced:
            if msg.id in sync[channel]:
                # This is a synced message being echoed back from another channel.
                log.debug("Ignoring echoed message: {}".format(repr(msg)))
                return
        log.debug("Syncing message to {} channel(s): {}".format(len(self.channels) - 1, repr(msg)))
        await self.send(msg, channel)
        # Push a copy of the message to the sync channel, if running.
        if self.plug:
            self.plug.queue(self.channel, msg)
