"""
Identity protocol backbone, and a generic user lookup command.

Config:
    identities (str list):
        List of identity provider names from which to allow lookups.
    public (bool):
        ``True`` to allow anyone with access to the ``who`` command to do a lookup, without
        necessarily being identified themselves (defaults to ``False``).

Commands:
    who <name>:
        Recall a known identity and all of its links.

This module defines a subclass for all hooks providing identity services -- no hook is needed from
here if using an identity hook elsewhere.  The :class:`.WhoIsHook` provides a command for users to
query basic identity information.
"""

from asyncio import gather
from collections import defaultdict
import logging

import immp
from immp.hook.command import command, CommandParser


CROSS = "\N{CROSS MARK}"
TICK = "\N{WHITE HEAVY CHECK MARK}"


log = logging.getLogger(__name__)


@immp.pretty_str
class Identity:
    """
    Basic representation of an external identity.

    Attributes:
        name (str):
            Common name used across any linked platforms.
        provider (.IdentityProvider):
            Service hook where the identity information was acquired from.
        links (.User list):
            Physical platform users assigned to this identity.
        roles (str list):
            Optional set of role names, if applicable to the backend.
        profile (str):
            URL to the identity profile page.
    """

    @classmethod
    async def gather(cls, *tasks):
        """
        Helper for fetching users from plugs, filtering out calls with no matches::

            >>> await Identity.gather(plug1.user_from_id(id1), plug2.user_from_id(id2))
            [<Plug1User: '123' 'User'>]

        Args:
            tasks (coroutine list):
                Non-awaited coroutines or tasks.

        Returns:
            .User list:
                Gathered results of those tasks.
        """
        tasks = list(filter(None, tasks))
        if not tasks:
            return []
        users = []
        for result in await gather(*tasks, return_exceptions=True):
            if isinstance(result, BaseException):
                log.warning("Failed to retrieve user for identity", exc_info=result)
            elif result:
                users.append(result)
        return users

    def __init__(self, name, provider=None, links=(), roles=(), profile=None):
        self.name = name
        self.provider = provider
        self.links = links
        self.roles = roles
        self.profile = profile

    def __eq__(self, other):
        return (isinstance(other, Identity) and
                (self.name, self.provider) == (other.name, other.provider))

    def __hash__(self):
        return hash((self.name, self.provider))

    def __repr__(self):
        return "<{}: {} x{}{}>".format(self.__class__.__name__, repr(self.name), len(self.links),
                                       " ({})".format(" ".join(self.roles)) if self.roles else "")


class IdentityProvider:
    """
    Interface for hooks to provide identity information from a backing source.

    Attributes:
        provider_name (str):
            Readable name of the underlying service, used when displaying info about this provider.
    """

    provider_name = None

    async def identity_from_name(self, name):
        """
        Look up an identity by the external provider's username for them.

        Args:
            name (str):
                External name to query.

        Returns:
            .Identity:
                Matching identity from the provider, or ``None`` if not found.
        """
        raise NotImplementedError

    async def identity_from_user(self, user):
        """
        Look up an identity by a linked network user.

        Args:
            user (.User):
                Plug user referenced by the identity.

        Returns:
            .Identity:
                Matching identity from the provider, or ``None`` if not found.
        """
        raise NotImplementedError


class WhoIsHook(immp.Hook):
    """
    Hook to provide generic lookup of user profiles across one or more identity providers.
    """

    schema = immp.Schema({"identities": [str],
                          immp.Optional("public", False): bool})

    _identities = immp.ConfigProperty([IdentityProvider])

    async def _query_all(self, query, providers=None):
        getter = "identity_from_{}".format("user" if isinstance(query, immp.User) else "name")
        providers = providers or self._identities
        tasks = (getattr(provider, getter)(query) for provider in providers)
        identities = []
        for provider, result in zip(providers, await gather(*tasks, return_exceptions=True)):
            if isinstance(result, Identity):
                identities.append(result)
            elif isinstance(result, Exception):
                log.warning("Failed to retrieve identity from %r (%r)",
                            provider.name, provider.provider_name, exc_info=result)
        return identities

    @command("who", parser=CommandParser.none)
    async def who(self, msg, name):
        """
        Recall a known identity and all of its links.
        """
        if self.config["public"]:
            providers = self._identities
        else:
            providers = [ident.provider for ident in await self._query_all(msg.user)]
        if providers:
            identities = await self._query_all(name[0].mention or str(name), providers)
            if identities:
                identities.sort(key=lambda ident: ident.provider.provider_name)
                links = defaultdict(list)
                roles = []
                for ident in identities:
                    for link in ident.links:
                        links[link].append(ident)
                    if ident.roles:
                        roles.append(ident)
                text = name.clone()
                text.prepend(immp.Segment("Info for "))
                for segment in text:
                    segment.bold = True
                text.append(immp.Segment("\nMatching providers:"))
                for i, ident in enumerate(identities):
                    text.append(immp.Segment("\n{}.\t".format(i + 1)),
                                immp.Segment(ident.provider.provider_name, link=ident.profile))
                if links:
                    text.append(immp.Segment("\nIdentity links:"))
                    for user in sorted(links, key=lambda user: user.plug.network_name):
                        text.append(immp.Segment("\n({}) ".format(user.plug.network_name)))
                        if user.link:
                            text.append(immp.Segment(user.real_name or user.username,
                                                     link=user.link))
                        elif user.real_name and user.username:
                            text.append(immp.Segment("{} [{}]".format(user.real_name,
                                                                      user.username)))
                        else:
                            text.append(immp.Segment(user.real_name or user.username))
                        known = links[user]
                        if known != identities:
                            indexes = [identities.index(ident) + 1 for ident in known]
                            text.append(immp.Segment(" {}".format(indexes)))
                if roles:
                    text.append(immp.Segment("\nRoles:"))
                    for ident in roles:
                        text.append(immp.Segment("\n({}) {}".format(ident.provider.provider_name,
                                                                    ", ".join(ident.roles))))
            else:
                text = "{} Name not in use".format(CROSS)
        else:
            text = "{} Not identified".format(CROSS)
        await msg.channel.send(immp.Message(text=text))
