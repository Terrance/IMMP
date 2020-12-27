from asyncio import wait
import logging
import re

import immp

from .common import AlertHookBase, Skip


log = logging.getLogger(__name__)


class MentionsHook(AlertHookBase):
    """
    Hook to send mention alerts via private channels.
    """

    schema = immp.Schema({immp.Optional("usernames", True): bool,
                          immp.Optional("real-names", False): bool,
                          immp.Optional("ambiguous", False): bool}, AlertHookBase.schema)

    @staticmethod
    def _clean(text):
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
        name = self._clean(mention)
        real_matches = set()
        real_partials = set()
        for member in members:
            if self.config["usernames"] and self._clean(member.username) == name:
                # Assume usernames are unique, only match the corresponding user.
                return {member}
            if self.config["real-names"]:
                real = self._clean(member.real_name)
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

    async def before_receive(self, sent, source, primary):
        await super().on_receive(sent, source, primary)
        if not primary or not source.text or await sent.channel.is_private():
            return sent
        try:
            _, members = await self._get_members(sent)
        except Skip:
            return sent
        for match in re.finditer(r"@\S+", str(source.text)):
            mention = match.group(0)
            matches = self.match(mention, members)
            if len(matches) == 1:
                target = next(iter(matches))
                log.debug("Exact match for mention %r: %r", mention, target)
                text = sent.text[match.start():match.end():True]
                for segment in text:
                    segment.mention = target
                    sent.text = (sent.text[:match.start():True] + text +
                                 sent.text[match.end()::True])
        return sent

    async def on_receive(self, sent, source, primary):
        await super().on_receive(sent, source, primary)
        if not primary or not source.text or await sent.channel.is_private():
            return
        try:
            _, members = await self._get_members(sent)
        except Skip:
            return
        mentioned = set()
        for mention in re.findall(r"@\S+", str(source.text)):
            matches = self.match(mention, members)
            if matches:
                log.debug("Mention %r applies: %r", mention, matches)
                mentioned.update(matches)
            else:
                log.debug("Mention %r doesn't apply", mention)
        for segment in source.text:
            if segment.mention and segment.mention in members:
                log.debug("Segment mention %r applies: %r", segment.text, segment.mention)
                mentioned.add(segment.mention)
        if not mentioned:
            return
        text = immp.RichText()
        if source.user:
            text.append(immp.Segment(source.user.real_name or source.user.username, bold=True),
                        immp.Segment(" mentioned you"))
        else:
            text.append(immp.Segment("You were mentioned"))
        title = await sent.channel.title()
        link = await sent.channel.link()
        if title:
            text.append(immp.Segment(" in "),
                        immp.Segment(title, italic=True))
        text.append(immp.Segment(":\n"))
        text += source.text
        if source.user and source.user.link:
            text.append(immp.Segment("\n"),
                        immp.Segment("Go to user", link=source.user.link))
        if link:
            text.append(immp.Segment("\n"),
                        immp.Segment("Go to channel", link=link))
        tasks = []
        for member in mentioned:
            if member == source.user:
                continue
            private = await sent.channel.plug.channel_for_user(member)
            if private:
                tasks.append(private.send(immp.Message(text=text)))
        if tasks:
            await wait(tasks)
