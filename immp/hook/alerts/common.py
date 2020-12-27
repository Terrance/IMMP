import logging

import immp
from immp.hook.sync import SyncPlug


log = logging.getLogger(__name__)


class Skip(Exception):
    # Message isn't applicable to the hook.
    pass


class AlertHookBase(immp.Hook):

    schema = immp.Schema({"groups": [str]})

    group = immp.Group.MergedProperty("groups")

    async def _get_members(self, msg):
        # Sync integration: avoid duplicate notifications inside and outside a synced channel.
        # Commands and excludes should apply to the sync, but notifications are based on the
        # network-native channel.
        if isinstance(msg.channel.plug, SyncPlug):
            # We're in the sync channel, so we've already handled this event in native channels.
            log.debug("Ignoring sync channel: %r", msg.channel)
            raise Skip
        channel = msg.channel
        synced = SyncPlug.any_sync(self.host, msg.channel)
        if synced:
            # We're in the native channel of a sync, use this channel for reading config.
            log.debug("Translating sync channel: %r -> %r", msg.channel, synced)
            channel = synced
        members = [user for user in (await msg.channel.members()) or []
                   if self.group.has_plug(user.plug)]
        if not members:
            raise Skip
        return channel, members
