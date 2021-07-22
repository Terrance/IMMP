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
    default (bool):
        ``True`` (default) to implicitly grant access if no predicates provide a decision, or
        ``False`` to treat all abstains as an implicit deny.

This hook implements its own protocol for test purposes, by rejecting all joins and members of a
channel.  To make full use of it, other hooks with support for channel access can determine if a
user satisfies membership of an external group or application.
"""

from asyncio import ensure_future, gather
from collections import defaultdict
from itertools import product
import logging

import immp


log = logging.getLogger(__name__)


class AccessPredicate:
    """
    Interface for hooks to provide channel access control from a backing source.
    """

    async def channel_access_multi(self, members):
        """
        Bulk-verify if a set of users are allowed access to all given channels.

        By default, calls :meth:`channel_access` for each channel-user pair, but can be overridden
        in order to optimise any necessary work.

        Where possible, avoid raising exceptions for problems with individual users or channels, as
        it means all other decisions will be lost.

        Args:
            members ((.Channel, .User set) dict):
                Mapping from target channels to members awaiting verification.  If ``None`` is given
                for a channel's set of users, all members of the channel will be verified.

        Returns:
            ((.Channel, .User) set, (.Channel, .User) set):
                Two sets of channel-user pairs, the first for users who are allowed, the second
                for those who are denied.  Each pair should appear in at most one of the two lists;
                conflicts will be resolved to deny access.
        """
        allowed = set()
        denied = set()
        for channel, users in members.items():
            for user in users:
                decision = await self.channel_access(channel, user)
                if decision is not None:
                    (allowed if decision else denied).add((channel, user))
        return allowed, denied

    async def channel_access(self, channel, user):
        """
        Verify if a user is allowed access to a channel.

        Args:
            channel (.Channel):
                Target channel.
            user (.User):
                User to be verified.

        Returns:
            bool:
                ``True`` to grant access for this user to the given channel, ``False`` to deny
                access, or ``None`` to abstain from a decision.
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
                          immp.Optional("passive", False): bool,
                          immp.Optional("default", True): bool})

    hooks = immp.ConfigProperty({AccessPredicate: [immp.Channel]})

    @property
    def channels(self):
        inverse = defaultdict(list)
        for hook, channels in self.hooks.items():
            for channel in channels:
                inverse[channel].append(hook)
        return inverse

    # This hook acts as an example predicate to block all access.

    async def channel_access_multi(self, channels, users):
        return [], list(product(channels, users))

    async def channel_access(self, channel, user):
        return False

    async def verify(self, members):
        """
        Perform verification of each user in each channel, for all configured access predicates.
        Users who are denied access by any predicate will be removed, unless passive mode is set.

        Args:
            members ((.Channel, .User set) dict):
                Mapping from target channels to members awaiting verification.  If ``None`` is given
                for a channel's set of users, all members of the channel will be verified.
        """
        targets = {}
        for channel, users in members.items():
            present = set()
            current = await channel.members()
            for user in users or current or ():
                if current and user not in current:
                    log.debug("Skipping non-member user %r", user)
                if user.id in self.config["exclude"].get(user.plug.name, []):
                    log.debug("Skipping excluded user %r", user)
                elif await user.is_system():
                    log.debug("Skipping system user %r", user)
                else:
                    present.add(user)
            targets[channel] = present
        hooks = []
        tasks = []
        for hook, scope in self.hooks.items():
            interested = {channel: users for channel, users in targets.items() if channel in scope}
            log.debug("Requesting decisions from %r: %r", hook, set(interested))
            hooks.append(hook)
            tasks.append(ensure_future(hook.channel_access_multi(interested)))
        allowed = set()
        denied = set()
        for hook, result in zip(hooks, await gather(*tasks, return_exceptions=True)):
            if isinstance(result, Exception):
                log.warning("Failed to verify channel access with hook %r",
                            hook.name, exc_info=result)
                continue
            hook_allowed, hook_denied = result
            allowed.update(hook_allowed)
            if hook_denied:
                log.debug("Hook %r denied %d user-channel pair(s)", hook.name, len(hook_denied))
                denied.update(hook_denied)
        removals = defaultdict(set)
        for channel, users in targets.items():
            for user in users:
                pair = (channel, user)
                if pair in denied:
                    allow = False
                elif pair in allowed:
                    allow = True
                else:
                    allow = self.config["default"]
                if allow:
                    log.debug("Allowing access to %r for %r", channel, user)
                else:
                    log.debug("Denying access to %r for %r", channel, user)
                    removals[channel].add(user)
        active = not self.config["passive"]
        for channel, refused in removals.items():
            log.debug("%s %d user(s) from %r: %r", "Removing" if active else "Would remove",
                      len(refused), channel, refused)
            if active:
                await channel.remove_multi(refused)

    async def _startup_check(self):
        log.debug("Running startup access checks")
        await self.verify({channel: None for channel in self.channels})
        log.debug("Finished startup access checks")

    def on_ready(self):
        if self.config["startup"]:
            ensure_future(self._startup_check())

    async def on_receive(self, sent, source, primary):
        await super().on_receive(sent, source, primary)
        if self.config["joins"] and primary and sent == source and source.joined:
            await self.verify({sent.channel: source.joined})
