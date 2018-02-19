from asyncio import BoundedSemaphore, gather
from collections import defaultdict
import logging

from voluptuous import ALLOW_EXTRA, All, Any, Length, Optional, Schema

import imirror
from imirror.receiver.command import Commandable


log = logging.getLogger(__name__)


class _Schema(object):

    config = Schema({"channels": All([str], Length(min=1)),
                     Optional("transport", default=None): Any(str, None)},
                    extra=ALLOW_EXTRA, required=True)


class SyncTransport(imirror.Transport):
    """
    A virtual transport that allows sending external messages to a sync.
    """

    def __init__(self, name, receiver, host):
        super().__init__(name, {}, host)
        self._receiver = receiver

    async def send(self, channel, msg):
        if channel == self._receiver.channel:
            return await self._receiver.send(msg)


class SyncReceiver(imirror.Receiver, Commandable):
    """
    A receiver to propagate messages between two or more channels.

    If ``transport`` is specified, a virtual transport is registered under that name, with a
    single channel of the same name.  Other receivers may reference this channel to work with
    all channels in that sync, to either listen for messages or submit new ones.

    Config:
        channels (str list):
            List of channel names to manage.
        transport (str):
            Name of a virtual transport to register for this sync.

    Attributes:
        transport (.SyncTransport):
            Virtual transport for this sync, if configured.
    """

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        config = _Schema.config(config)
        self.channels = []
        for channel in config["channels"]:
            try:
                self.channels.append(host.channels[channel])
            except KeyError:
                raise imirror.ConfigError("No channel '{}' on host".format(channel)) from None
        # Message cache, stores mappings of synced message IDs keyed by channel.
        # [{Channel(): [id, ...], ...}, ...]
        self._synced = []
        # Receiver lock, to put a hold on retrieving messages whilst a send is in progress.
        self._lock = BoundedSemaphore()
        # Add a virtual transport to the host, for external subscribers.
        if config["transport"]:
            tname = config["transport"]
            log.debug("Creating virtual transport '{}'".format(tname))
            self.transport = SyncTransport(tname, self, host)
            host.add_transport(self.transport)
            self.channel = imirror.Channel(tname, self.transport, None)
            host.add_channel(self.channel)
        else:
            self.transport = None

    def commands(self):
        return {"sync-members": self.members}

    async def members(self, channel, mag):
        members = defaultdict(list)
        missing = False
        for synced in self.channels:
            local = (await synced.transport.channel_members(synced))
            if local:
                members[synced.transport.Meta.network] += local
            else:
                missing = True
        if not members:
            return
        text = imirror.RichText([imirror.Segment("Members of this conversation:")])
        for network in sorted(members):
            text.append(imirror.Segment("\n{}".format(network), bold=True))
            for member in sorted(members[network],
                                 key=lambda member: member.real_name or member.username):
                name = member.real_name or member.username
                text.append(imirror.Segment("\n"))
                if member.link:
                    text.append(imirror.Segment(name, link=member.link))
                elif member.real_name and member.username:
                    text.append(imirror.Segment("{} [{}]".format(name, member.username)))
                else:
                    text.append(imirror.Segment(name))
        if missing:
            text.append(imirror.Segment("\n"),
                        imirror.Segment("(list may be incomplete)"))
        await channel.send(imirror.Message(user=imirror.User(real_name="Sync"), text=text))

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
        # Just like with transports, when sending a new (external) message to all channels in a
        # sync, we need to wait for all transports to complete before processing further messages.
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
        if self.transport:
            self.transport.queue(self.channel, msg)
