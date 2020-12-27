"""
History and link-join setting locks for Hangouts.

Dependencies:
    :class:`.HangoutsPlug`

Config:
    history ((str, bool) dict):
        Mapping from channel names to desired conversation history settings -- ``True`` to keep
        history enabled, ``False`` to keep it disabled.
    linkjoin ((str, bool) dict):
        Mapping from channel names to desired link join settings -- ``True`` to enable joining the
        hangout via link, ``False`` to disable it.
"""

import hangups
from hangups import hangouts_pb2

import immp
from immp.plug.hangouts import HangoutsPlug


HISTORY = {True: hangouts_pb2.OFF_THE_RECORD_STATUS_ON_THE_RECORD,
           False: hangouts_pb2.OFF_THE_RECORD_STATUS_OFF_THE_RECORD}

LINK_JOIN = {True: hangouts_pb2.GROUP_LINK_SHARING_STATUS_ON,
             False: hangouts_pb2.GROUP_LINK_SHARING_STATUS_OFF}


class HangoutsLockHook(immp.Hook):
    """
    Hook to enforce the history and link-join settings in Hangouts.
    """

    schema = immp.Schema({immp.Optional("history", dict): {str: bool},
                          immp.Optional("linkjoin", dict): {str: bool}})

    @property
    def channels(self):
        try:
            return {key: {self.host.channels[label]: setting
                          for label, setting in mapping.items()}
                    for key, mapping in self.config.items()}
        except KeyError as e:
            raise immp.HookError("No channel named '{}'".format(e.args[0]))

    async def on_receive(self, sent, source, primary):
        await super().on_receive(sent, source, primary)
        if sent != source or not isinstance(sent.channel.plug, HangoutsPlug):
            return
        conv = sent.channel.plug._convs.get(sent.channel.source)
        if isinstance(sent.raw, hangups.OTREvent):
            setting = HISTORY.get(self.channels["history"].get(sent.channel))
            if setting is None:
                return
            if setting != sent.raw.new_otr_status:
                request = hangouts_pb2.ModifyOTRStatusRequest(
                    request_header=sent.channel.plug._client.get_request_header(),
                    event_request_header=conv._get_event_request_header(),
                    otr_status=setting)
                await sent.channel.plug._client.modify_otr_status(request)
        elif isinstance(sent.raw, hangups.GroupLinkSharingModificationEvent):
            setting = LINK_JOIN.get(self.channels["linkjoin"].get(sent.channel))
            if setting is None:
                return
            if setting != sent.raw.new_status:
                request = hangouts_pb2.SetGroupLinkSharingEnabledRequest(
                    request_header=sent.channel.plug._client.get_request_header(),
                    event_request_header=conv._get_event_request_header(),
                    group_link_sharing_status=setting)
                await sent.channel.plug._client.set_group_link_sharing_enabled(request)
