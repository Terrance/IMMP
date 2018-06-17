"""
Fallback mention support for plugs via private channels.

Config:
    plugs (str list):
        List of plug names to enable mentions for.
    usernames (bool):
        Whether to match network usernames (``True`` by default).
    real-names (bool):
        Whether to match user's display names (``False`` by default).
    ambiguous (bool):
        Whether to notify multiple potential users of an ambiguous mention (``False`` by default).

For networks that don't provide native user mentions, this plug can send users a private message
when mentioned by their username or real name.

A mention is matched from each ``@`` sign until whitespace is encountered.  For real names, spaces
and special characters are ignored, so that e.g. ``@fredbloggs`` will match *Fred Bloggs*.

Partial mentions are supported, failing any exact matches, by basic prefix search on real names.
For example, ``@fred`` will match *Frederick*, and ``@fredb`` will match *Fred Bloggs*.
"""

from asyncio import wait
import logging
import re

from voluptuous import ALLOW_EXTRA, Optional, Schema

import immp


log = logging.getLogger(__name__)


class _Schema(object):

    config = Schema({"plugs": [str],
                     Optional("usernames", default=True): bool,
                     Optional("real-names", default=False): bool,
                     Optional("ambiguous", default=False): bool},
                    extra=ALLOW_EXTRA, required=True)


class MentionsHook(immp.Hook):
    """
    Hook to send mention alerts via private channels.
    """

    def __init__(self, name, config, host):
        super().__init__(name, _Schema.config(config), host)
        self.plugs = []
        for label in self.config["plugs"]:
            try:
                self.plugs.append(host.plugs[label])
            except KeyError:
                raise immp.ConfigError("No plug '{}' on host".format(label)) from None

    @staticmethod
    def clean(text):
        return re.sub(r"\W", "", text).lower() if text else None

    def match(self, mention, members):
        """
        Identify users relevant to a mention.

        Args:
            mention (str):
                Raw mention text, e.g. ``@fred``.
            members (.User list):
                List of members in the channel where the mention took place.

        Returns:
            .User set:
                All applicable members to be notified.
        """
        name = self.clean(mention)
        real_matches = set()
        real_partials = set()
        for member in members:
            if self.config["usernames"] and self.clean(member.username) == name:
                # Assume usernames are unique, only match the corresponding user.
                return {member}
            if self.config["real-names"]:
                real = self.clean(member.real_name)
                if real == name:
                    real_matches.add(member)
                if real.startswith(name):
                    real_partials.add(member)
        if real_matches:
            # Assume multiple identical real names is unlikely.
            # If it's the same person with two users, they both get mentioned.
            return real_matches
        elif len(real_partials) == 1 or self.config["ambiguous"]:
            # Return a single partial match if it exists.
            # Only allow multiple partials if enabled, else ignore the mention.
            return real_partials
        else:
            return set()

    async def process(self, channel, msg, source, primary):
        await super().process(channel, msg, source, primary)
        if not primary or channel.plug not in self.plugs or await channel.is_private():
            return
        members = await channel.plug.channel_members(channel)
        if not members:
            return
        mentioned = set()
        for mention in re.findall(r"@\S+", str(source.text)):
            matches = self.match(mention, members)
            if matches:
                log.debug("Mention '{}' applies: {}".format(mention, matches))
                mentioned.update(matches)
            else:
                log.debug("Mention '{}' doesn't apply".format(mention))
        if not mentioned:
            return
        text = immp.RichText()
        if source.user:
            text.append(immp.Segment(source.user.real_name or source.user.username,
                                     link=source.user.link,
                                     bold=(not source.user.link)),
                        immp.Segment(" mentioned you"))
        else:
            text.append(immp.Segment("You were mentioned"))
        title = await channel.title()
        if title:
            text.append(immp.Segment(" in "),
                        immp.Segment(title, italic=True))
        text.append(immp.Segment(":\n"))
        if isinstance(source.text, immp.RichText):
            text += source.text
        else:
            text.append(immp.Segment(source.text))
        tasks = []
        for member in mentioned:
            if member == source.user:
                continue
            private = await channel.plug.channel_for_user(member)
            if private and not channel == private:
                tasks.append(private.send(immp.Message(text=text)))
        if tasks:
            await wait(tasks)
