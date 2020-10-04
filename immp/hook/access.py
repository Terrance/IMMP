"""
Channel join control, extended by other hooks.

Config:
    hooks ((str, str list) dict):
        Mapping of controlling hooks to a list of channels they manage.
    exclude ((str, str list) dict):
        Mapping of plugs to user IDs who should be ignored during checks.
    joins (bool):
        ``True`` to check each join as it happens.
    startup (bool):
        ``True`` to run a full check of all named channels on load.
    passive (bool):
        ``True`` to log violations without actually following through with removals.

This hook implements its own protocol for test purposes, by rejecting all joins and members of a
channel.  To make full use of it, other hooks with support for channel access can determine if a
user satisfies membership of an external group or application.
"""

import asyncio
from collections import defaultdict
import logging

import immp


log = logging.getLogger(__name__)


class AccessPredicate:
    """
    Interface for hooks to provide channel access control from a backing source.
    """

    async def channel_access(self, channel, user):
        """
        Verify if a user is allowed access to a channel.

        Args:
            channel (.Channel):
                Target channel.
            user (.User):
                Incoming user to be verified.

        Returns:
            bool:
                ``True`` if the user is to be granted access.
        """
        raise NotImplementedError


class ChannelAccessHook(immp.Hook, AccessPredicate):
    """
    Hook for controlling membership of, and joins to, secure channels.
    """

    schema = immp.Schema({immp.Optional("hooks", dict): {str: [str]},
                          immp.Optional("exclude", dict): {str: [str]},
                          immp.Optional("joins", True): bool,
                          immp.Optional("startup", False): bool,
                          immp.Optional("passive", False): bool})

    hooks = immp.ConfigProperty({AccessPredicate: [immp.Channel]})

    @property
    def channels(self):
        inverse = defaultdict(list)
        for hook, channels in self.hooks.items():
            for channel in channels:
                inverse[channel].append(hook)
        return inverse

    async def channel_access(self, channel, user):
        # Example predicate to block all access.
        return False

    async def _predicate(self, hook, channel, user):
        if not isinstance(hook, AccessPredicate):
            raise immp.HookError("Hook '{}' does not implement AccessPredicate".format(hook.name))
        allow = await hook.channel_access(channel, user)
        if not allow:
            log.debug("Hook %r disallows %r in %r", hook.name, user, channel)
            if not self.config["passive"]:
                await channel.remove(user)
        return allow

    async def _verify(self, channel, user):
        if user.id in self.config["exclude"].get(user.plug.name, []):
            log.debug("Skipping excluded user %r in channel %r", user, channel)
            return
        elif await user.is_system():
            log.debug("Skipping system user %r in channel %r", user, channel)
            return
        try:
            hooks = self.channels[channel]
        except KeyError:
            return
        for hook in hooks:
            if not await self._predicate(hook, channel, user):
                break

    async def _startup_check(self):
        log.debug("Running startup access checks")
        for channel in self.channels:
            members = await channel.members()
            if not members:
                continue
            for user in members:
                await self._verify(channel, user)
        log.debug("Finished startup access checks")

    async def start(self):
        await super().start()
        if self.config["startup"]:
            asyncio.ensure_future(self._startup_check())

    async def on_receive(self, sent, source, primary):
        await super().on_receive(sent, source, primary)
        if not self.config["joins"] or not primary or sent != source or not source.joined:
            return
        for user in source.joined:
            await self._verify(sent.channel, user)
