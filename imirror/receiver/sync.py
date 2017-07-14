import logging

import imirror


log = logging.getLogger(__name__)


class SyncReceiver(imirror.Receiver):
    """
    A receiver to propagate messages between two or more channels.

    Config
        channels (str list):
            List of channel names to manage.
    """

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        try:
            channels = config["channels"]
        except KeyError:
            raise imirror.ConfigError("Sync channels not specified") from None
        self.channels = []
        # Message cache, stores synced message IDs keyed by original message.
        # {Message(): {Channel(): id, ...}, ...}
        self.synced = {}
        for channel in channels:
            try:
                self.channels.append(host.channels[channel])
            except KeyError:
                raise imirror.ConfigError("No channel '{}' on host".format(channel)) from None

    async def process(self, channel, msg):
        await super().process(channel, msg)
        # Only process if we recognise the channel.
        if channel not in self.channels:
            return
        for sync in self.synced.values():
            if sync.get(channel) == msg.id:
                # This is a synced message being echoed back from another channel.
                log.debug("Ignoring echoed message: {}".format(msg))
                return
        log.debug("Syncing message to {} channel(s): {}".format(len(self.channels) - 1, msg))
        sync = {}
        for sync_channel in self.channels:
            if channel == sync_channel:
                # This is the channel we just got the message from.
                continue
            sync[sync_channel] = (await sync_channel.send(msg))
        self.synced[msg] = sync
