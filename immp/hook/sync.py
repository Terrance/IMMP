"""
Bridge multiple channels into a single unified conversation, or relay messages from one channel to
one or more others.

Sync
~~~~

Requirements:
    Extra name: ``sync``

    `Jinja2 <http://jinja.pocoo.org>`_:
        Required to use ``name-format``.
    `emoji <https://github.com/carpedm20/emoji/>`_:
        Required to use ``strip-name-emoji``.

Dependencies:
    :class:`.AsyncDatabaseHook`:
        If present, synced message IDs will be persisted to the database, allowing referenced
        messages (replies, forwards, attachments) to be handled correctly across restarts.

Config:
    channels ((str, str list) dict):
        Mapping from virtual channel names to lists of channel names to bridge.
    plug (str):
        Name of a virtual plug to register for this sync.
    joins (bool):
        Whether to sync join and part messages across the bridge.
    renames (bool):
        Whether to sync channel title changes across the bridge.
    identities (str):
        Name of a registered :class:`.IdentityProvider` to provide unified names across networks.

        If enabled, this will rewrite mentions for users identified in both the source and any sync
        target channels, to use their platform-native identity.
    reset-author (bool):
        ``True`` to create and attach a new user with just a name, ``False`` (default) to clone and
        modify the existing user (thus keeping username, avatar etc.).
    name-format (str):
        Template to use for replacing real names on synced messages, parsed by :mod:`jinja2`.  If
        not set but the user is identified, it defaults to ``<real name> (<identity name>)``.

        Context variables:
            user (.User):
                Message author, may be ``None`` for system messages or when ``reset-author`` is set.
            identity (.IdentityGroup):
                Connected identity, or ``None`` if no link or ``identities`` isn't set.
            title (str):
                Channel title.
    strip-name-emoji (bool):
        ``True`` to remove emoji characters from message authors' real names.
    titles ((str, str) dict):
        Mapping from virtual channel names to display names.

Commands:
    sync-members:
        List all members of the current conversation, across all channels.
    sync-list:
        List all channels connected to this conversation.

When a message is received from any of the listed channels, a copy is pushed to all other channels
participating in the bridge.

If ``plug`` is specified, a virtual plug is registered under that name, with a channel for each
defined bridge.  Other hooks may reference these channels, to work with all channels in that sync
as one.  This allows them to listen to a unified stream of messages, or push new messages to all
synced channels.

Forward
~~~~~~~

Requirements:
    Extra name: ``sync``

    `Jinja2 <http://jinja.pocoo.org>`_:
        Required to use ``name-format``.
    `emoji <https://github.com/carpedm20/emoji/>`_:
        Required to use ``strip-name-emoji``.

Config:
    channels ((str, str list) dict):
        Mapping from source channel names to lists of channel names to forward to.
    users (str list):
        Whitelist of user IDs to accept source messages from.  If set, messages from anyone else in
        the source channel will be ignored.
    joins (bool):
        Whether to forward join and part messages.
    renames (bool):
        Whether to forward channel title changes.
    identities (str):
        Name of a registered :class:`.IdentityProvider` to provide unified names across networks.

        If enabled, this will rewrite mentions for users identified in both the source and any sync
        target channels, to use their platform-native identity.
    reset-author (bool):
        ``True`` to create and attach a new user with just a name, ``False`` (default) to clone and
        modify the existing user (thus keeping username, avatar etc.).
    name-format (str):
        Template to use for replacing real names on synced messages, parsed by :mod:`jinja2`.  If
        not set but the user is identified, it defaults to ``<real name> (<identity name>)``.

        Context variables:
            user (.User):
                Message author, may be ``None`` for system messages or when ``reset-author`` is set.
            identity (.IdentityGroup):
                Connected identity, or ``None`` if no link or ``identities`` isn't set.
            title (str):
                Channel title.
    strip-name-emoji (bool):
        ``True`` to remove emoji characters from message authors' real names.

When a message is received in a configured source channel, a copy is pushed to all downstream
channels.  Unlike a sync, this is a one-direction copy, useful for announcements or alerts.
"""

from asyncio import BoundedSemaphore, gather
from collections import defaultdict
from itertools import chain
import logging
import re

try:
    from tortoise import Model
    from tortoise.exceptions import DoesNotExist
    from tortoise.fields import TextField
except ImportError:
    Model = TextField = None

import immp
from immp.hook.command import command
from immp.hook.database import AsyncDatabaseHook
from immp.hook.identity import IdentityProvider


try:
    from jinja2 import Template, TemplateSyntaxError
except ImportError:
    Template = TemplateSyntaxError = None

try:
    from emoji import get_emoji_regexp
except ImportError:
    EMOJI_REGEX = None
else:
    _EMOJI_REGEX_RAW = get_emoji_regexp()
    EMOJI_REGEX = re.compile(r"(\s*)({})+(\s*)".format(_EMOJI_REGEX_RAW.pattern))


log = logging.getLogger(__name__)


def _emoji_replace(match):
    # Add correct spacing around removed emoji in a string.
    left, *_, right = match.groups()
    return " " if left and right else ""


if Model:

    class SyncBackRef(Model):
        """
        One of a set of references, each pointing to a representation of a source message.

        Attributes:
            key (str):
                Shared ID for each message in the same set.
            network (str):
                Network identifier of the plug for this message.
            channel (str):
                Origin channel of the referenced message.
            message (str):
                Message ID as generated by the plug.
        """

        key = TextField()
        network = TextField()
        channel = TextField()
        message = TextField()

        class Meta:
            # Unique constraint for each message.
            unique_together = (("network", "channel", "message"),)

        @classmethod
        async def map_from_sent(cls, sent):
            """
            Take a :class:`.Receipt` and attempt to resolve it to a key from a previously synced
            reference, then defer to :meth:`map_from_key` to make the mapping with the key.

            Args:
                sent (.Receipt):
                    Referenced message to lookup.

            Returns:
                ((str, str), .SyncBackRef) dict:
                    Generated reference mapping.
            """
            try:
                ref = await cls.filter(network=sent.channel.plug.network_id,
                                       channel=sent.channel.source,
                                       message=str(sent.id)).get()
            except DoesNotExist:
                raise KeyError
            else:
                return await cls.map_from_key(ref.key)

        @classmethod
        async def map_from_key(cls, key):
            """
            For a given key, fetch all messages references synced from the corresponding source, and
            group them by network and channel.

            Args:
                key (str):
                    Synced message identifier.

            Returns:
                ((str, str), .SyncBackRef) dict:
                    Generated reference mapping.
            """
            backrefs = await cls.filter(key=key)
            if not backrefs:
                raise KeyError
            mapped = defaultdict(list)
            for backref in backrefs:
                mapped[(backref.network, backref.channel)].append(backref)
            return key, mapped

        def __repr__(self):
            return "<{}: #{} {} {} @ {}/{}>".format(self.__class__.__name__, self.id, self.key,
                                                    self.message, self.network, self.channel)


class SyncRef:
    """
    Representation of a single synced message.

    Attributes:
        key (str):
            Unique synced message identifier, used by :class:`.SyncPlug` when yielding messages.
        ids ((.Channel, str) dict):
            Mapping from :class:`.Channel` to a list of echoed message IDs.
        revisions ((.Channel, (str, set) dict) dict):
            Mapping from :class:`.Channel` to message ID to synced revisions of that message.
        source (.Message):
            Original copy of the source message, if we have it.
    """

    next_key = immp.IDGen()

    @classmethod
    def from_backref_map(cls, key, mapped, host):
        """
        Take a mapping generated in :meth:`.SyncBackRef.map_from_key` and produce a local reference
        suitable for the memory cache.

        Args:
            key (str):
                Synced message identifier.
            mapped (((str, str), .SyncBackRef list) dict):
                Generated reference mapping.
            host (.Host):
                Parent host instance, needed to resolve network IDs to plugs.

        Returns:
            .SyncRef:
                Newly created reference.
        """
        ids = {}
        for (network, source), synced in mapped.items():
            for plug in host.plugs.values():
                if plug.network_id == network:
                    ids[immp.Channel(plug, source)] = [backref.message for backref in synced]
        return cls(ids, key=key)

    def __init__(self, ids, *, key=None, source=None, origin=None):
        self.key = key or self.next_key()
        self.ids = defaultdict(list, ids)
        self.revisions = defaultdict(lambda: defaultdict(set))
        self.source = source
        if origin:
            self.ids[origin.channel].append(origin.id)
            self.revision(origin)

    def revision(self, sent):
        """
        Log a new revision of a message.

        Args:
            sent (.Receipt):
                Updated message relating to a previously synced message.

        Returns:
            bool:
                ``True`` if this is an edit (i.e. we've already seen a base revision for this
                message) and needs syncing to other channels.
        """
        self.revisions[sent.channel][sent.id].add(sent.revision)
        return len(self.revisions[sent.channel][sent.id]) > 1

    def __repr__(self):
        return "<{}: #{} x{}{}>".format(self.__class__.__name__, self.key, len(self.ids),
                                        " {}".format(repr(self.source)) if self.source else "")


class SyncCache:
    """
    Synced message cache manager, using both in-memory and database-based caches.

    This class has :class:`dict`-like access, using either :class:`.Receipt` objects or
    :class:`.SyncPlug` message IDs that map to :class:`.SyncRef` keys.
    """

    def __init__(self, hook):
        self._hook = hook
        self._cache = {}
        # Reverse mapping, Channel -> ID -> SyncRef.
        self._lookup = defaultdict(dict)

    async def add(self, ref, back=False):
        """
        Add a :class:`.SyncRef` to the cache.  This will also commit a new :class:`.SyncBackRef` to
        the database if configured.

        Args:
            ref (.SyncRef):
                Newly synced message to store.
            back (bool):
                ``True`` if sourced from a :class:`.SyncBackRef`, and therefore doesn't need
                committing back to the database.

        Returns:
            .SyncRef:
                The same ref, useful for shorthand add-then-return.
        """
        self._cache[ref.key] = ref
        for channel, ids in ref.ids.items():
            for id_ in ids:
                self._lookup[channel][id_] = ref.key
                if self._hook._db and not back:
                    # Throwaway get to avoid loud integrity errors on duplicates.
                    await SyncBackRef.get_or_create(key=ref.key, network=channel.plug.network_id,
                                                    channel=channel.source, message=id_)
        return ref

    async def get(self, key):
        if isinstance(key, immp.Receipt) and key.channel.plug == self._hook.plug:
            # Message from the sync channel itself, so just look the key up directly.
            key = key.id
        if isinstance(key, immp.Receipt):
            try:
                # If in the local cache, the message already passed through sync in this session.
                # Use the existing cache entry as-is (entry in _lookup <=> entry in _cache).
                return self._cache[self._lookup[key.channel][key.id]]
            except KeyError:
                if not self._hook._db:
                    raise
            # Not cached locally, but the database is configured: check there for a reference,
            # build a new SyncRef with an empty source, and cache it.
            key, mapped = await SyncBackRef.map_from_sent(key)
            return await self.add(SyncRef.from_backref_map(key, mapped, self._hook.host), True)
        elif isinstance(key, str):
            # As above, check the local cache directly first.
            try:
                return self._cache[key]
            except KeyError:
                if not self._hook._db:
                    raise
            # Now check the database for the key.
            _, mapped = await SyncBackRef.map_from_key(key)
            return await self.add(SyncRef.from_backref_map(key, mapped, self._hook.host), True)
        else:
            raise TypeError(key)

    def __repr__(self):
        return "<{}: {} refs>".format(self.__class__.__name__, len(self._cache))


class SyncPlug(immp.Plug):
    """
    Virtual plug that allows sending external messages to a synced conversation.
    """

    schema = None

    network_name = "Sync"

    @property
    def network_id(self):
        return "sync:{}".format(self.name)

    def __init__(self, name, hook, host):
        super().__init__(name, {}, host, virtual=True)
        self._hook = hook

    @classmethod
    def any_sync(cls, host, channel):
        """
        Produce a synced channel for the given source, searching across all :class:`.SyncPlug`
        instances running on the host.

        Args:
            host (.Host):
                Controlling host instance.
            channel (.Channel):
                Original channel to lookup.

        Returns:
            .Channel:
                Sync channel containing the given channel as a source, or ``None`` if not synced.
        """
        synced = [plug.sync_for(channel) for plug in host.plugs.values() if isinstance(plug, cls)]
        try:
            return next(filter(None, synced))
        except StopIteration:
            return None

    def sync_for(self, channel):
        """
        Produce a synced channel for the given source.

        Args:
            channel (.Channel):
                Original channel to lookup.

        Returns:
            .Channel:
                Sync channel containing the given channel as a source, or ``None`` if not synced.
        """
        for label, synced in self._hook.channels.items():
            if channel in synced:
                return immp.Channel(self, label)
        return None

    def in_sync(self, channel):
        """
        Retrieve the list of member channels for a given sync channel.

        Args:
            channel (.Channel):
                Virtual channel belonging to this sync instance.

        Returns:
            .Channel list:
                Channels participating in this sync.
        """
        return self._hook.channels[channel.source] if channel.plug is self else []

    async def public_channels(self):
        return [immp.Channel(self, name) for name in self._hook.channels]

    async def private_channels(self):
        return []

    async def channel_is_private(self, channel):
        return False if channel.source in self._hook.config["channels"] else None

    async def channel_title(self, channel):
        return self._hook.config["titles"].get(channel.source, channel.source)

    async def channel_members(self, channel):
        if channel.source not in self._hook.config["channels"]:
            return None
        tasks = (synced.members() for synced in self._hook.channels[channel.source])
        members = set(chain(*(await gather(*tasks))))
        return list(sorted(members, key=lambda u: (u.plug.name, u.username or u.real_name)))

    async def put(self, channel, msg):
        if channel.source in self._hook.config["channels"]:
            ref = await self._hook.send(channel.source, msg)
            return [immp.Receipt(ref.key, channel)]
        else:
            raise immp.PlugError("Send to unknown sync channel: {}".format(repr(channel)))

    async def delete(self, sent):
        await self._hook.delete(await self._hook._cache.get(sent.id))


class _SyncHookBase(immp.Hook):

    schema = immp.Schema({"channels": {str: [str]},
                          immp.Optional("joins", False): bool,
                          immp.Optional("renames", False): bool,
                          immp.Optional("identities"): immp.Nullable(str),
                          immp.Optional("reset-author", False): bool,
                          immp.Optional("name-format"): immp.Nullable(str),
                          immp.Optional("strip-name-emoji", False): bool})

    _identities = immp.ConfigProperty(IdentityProvider)

    def _accept(self, msg, id_):
        if not self.config["joins"] and (msg.joined or msg.left):
            log.debug("Not syncing join/part message: %r", id_)
            return False
        if not self.config["renames"] and msg.title:
            log.debug("Not syncing rename message: %r", id_)
            return False
        return True

    async def _replace_recurse(self, msg, func, *args):
        # Switch out entire messages for copies or replacements.
        if msg.reply_to:
            msg.reply_to = await func(msg.reply_to, *args)
        attachments = []
        for attach in msg.attachments:
            if isinstance(attach, immp.Message):
                attachments.append(await func(attach, *args))
            else:
                attachments.append(attach)
        msg.attachments = attachments
        return msg

    async def _alter_recurse(self, msg, func, *args):
        # Alter properties on existing cloned message objects.
        await func(msg, *args)
        if msg.reply_to:
            await func(msg.reply_to, *args)
        for attach in msg.attachments:
            if isinstance(attach, immp.Message):
                await func(attach, *args)

    async def _rename_user(self, user, channel):
        # Use name-format or identities to render a suitable author real name.
        name = (user.real_name or user.username) if user else None
        identity = None
        if user and self._identities:
            try:
                identity = await self._identities.identity_from_user(user)
            except Exception as e:
                log.warning("Failed to retrieve identity information for %r", user,
                            exc_info=e)
        if self.config["name-format"]:
            if not Template:
                raise immp.PlugError("'jinja2' module not installed")
            title = await channel.title() if channel else None
            context = {"user": user, "identity": identity, "channel": title}
            try:
                name = Template(self.config["name-format"]).render(**context)
            except TemplateSyntaxError:
                log.warning("Bad name format template", exc_info=True)
        elif identity:
            name = "{} ({})".format(user.real_name or user.username, identity.name)
        if name and self.config["strip-name-emoji"]:
            if not EMOJI_REGEX:
                raise immp.PlugError("'emoji' module not installed")
            name = EMOJI_REGEX.sub(_emoji_replace, name).strip()
        if self.config["reset-author"] or not user:
            log.debug("Creating unlinked user with real name: %r", name)
            return immp.User(real_name=name)
        elif user.real_name != name:
            log.debug("Copying user with new real name: %r -> %r", user, name)
            return immp.User(id_=user.id, plug=user.plug, real_name=name,
                             avatar=user.avatar, link=user.link)
        else:
            return user

    async def _alter_name(self, msg):
        channel = msg.channel if isinstance(msg, immp.Receipt) else None
        msg.user = await self._rename_user(msg.user, channel)

    async def _alter_identities(self, msg, channel):
        # Replace mentions for identified users in the target channel.
        if not msg.text:
            return
        msg.text = msg.text.clone()
        for segment in msg.text:
            user = segment.mention
            if not user or user.plug == channel.plug:
                # No mention or already matches plug, nothing to do.
                continue
            identity = None
            if self.config["identities"]:
                try:
                    identity = await self._identities.identity_from_user(user)
                except Exception as e:
                    log.warning("Failed to retrieve identity information for %r", user, exc_info=e)
            # Try to find an identity corresponding to the target plug.
            links = identity.links if identity else []
            for user in links:
                if user.plug == channel.plug:
                    log.debug("Replacing mention: %r -> %r", user, user)
                    segment.mention = user
                    break
            else:
                # Fallback case: replace mention with a link to the user's profile.
                if user.link:
                    log.debug("Adding fallback mention link: %r -> %r", user, user.link)
                    segment.link = user.link
                else:
                    log.debug("Removing foreign mention: %r", user)
                segment.mention = None
            # Perform name substitution on the mention text.
            if self.config["name-format"]:
                at = "@" if segment.text.startswith("@") else ""
                renamed = await self._rename_user(user, channel)
                segment.text = "{}{}".format(at, renamed.real_name)

    async def _send(self, channel, msg):
        try:
            receipts = await channel.send(msg)
            log.debug("Synced IDs in %r: %r", channel, [receipt.id for receipt in receipts])
            return (channel, receipts)
        except Exception:
            log.exception("Failed to relay message to channel: %r", channel)
            return (channel, [])


class SyncHook(_SyncHookBase):
    """
    Hook to propagate messages between two or more channels.

    Attributes:
        plug (.SyncPlug):
            Virtual plug for this sync, if configured.
    """

    schema = immp.Schema({immp.Optional("joins", True): bool,
                          immp.Optional("renames", True): bool,
                          immp.Optional("plug"): immp.Nullable(str),
                          immp.Optional("titles", dict): {str: str}}, _SyncHookBase.schema)

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        # Message cache, stores IDs of all synced messages by channel.
        self._cache = SyncCache(self)
        # Hook lock, to put a hold on retrieving messages whilst a send is in progress.
        self._lock = BoundedSemaphore()
        # Add a virtual plug to the host, for external subscribers.
        if self.config["plug"]:
            log.debug("Creating virtual plug: %r", self.config["plug"])
            self.plug = SyncPlug(self.config["plug"], self, host)
            host.add_plug(self.plug)
            for label in self.config["channels"]:
                host.add_channel(label, immp.Channel(self.plug, label))
        else:
            self.plug = None
        self._db = False

    def on_load(self):
        try:
            self.host.resources[AsyncDatabaseHook].add_models(SyncBackRef)
        except KeyError:
            self._db = False
        else:
            self._db = True

    channels = immp.ConfigProperty({None: [immp.Channel]})

    def label_for_channel(self, channel):
        labels = []
        for label, channels in self.channels.items():
            if channel in channels:
                labels.append(label)
        if not labels:
            raise immp.ConfigError("Channel {} not bridged".format(repr(channel)))
        elif len(labels) > 1:
            raise immp.ConfigError("Channel {} defined more than once".format(repr(channel)))
        else:
            return labels[0]

    def _test(self, channel, user):
        return any(channel in channels for channels in self.channels.values())

    @command("sync-members", test=_test)
    async def members(self, msg):
        """
        List all members of the current conversation, across all channels.
        """
        members = defaultdict(list)
        missing = False
        for synced in self.channels[msg.channel.source]:
            local = (await synced.members())
            if local:
                members[synced.plug.network_name] += local
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
        await msg.channel.send(immp.Message(user=immp.User(real_name="Sync"), text=text))

    @command("sync-list", test=_test)
    async def list(self, msg):
        """
        List all channels connected to this conversation.
        """
        text = immp.RichText([immp.Segment("Channels in this sync:")])
        for synced in self.channels[msg.channel.source]:
            text.append(immp.Segment("\n{}".format(synced.plug.network_name)))
            title = await synced.title()
            if title:
                text.append(immp.Segment(": {}".format(title)))
        await msg.channel.send(immp.Message(user=immp.User(real_name="Sync"), text=text))

    async def send(self, label, msg, origin=None, ref=None, update=False):
        """
        Send a message to all channels in this synced group.

        Args:
            label (str):
                Bridge that defines the underlying synced channels to send to.
            msg (.Message):
                External message to push.  This should be the source copy when syncing a message
                from another channel.
            origin (.Receipt):
                Raw message that triggered this sync; if set and part of the sync, it will be
                skipped (used to avoid retransmitting a message we just received).  This should be
                the plug-native copy of a message when syncing from another channel.
            ref (.SyncRef):
                Existing sync reference, if message has been partially synced.
            update (bool):
                ``True`` to force resending an updated message to all synced channels.
        """
        base = immp.Message(text=msg.text, user=msg.user, edited=msg.edited, action=msg.action,
                            reply_to=msg.reply_to, joined=msg.joined, left=msg.left,
                            title=msg.title, attachments=msg.attachments, raw=msg)
        queue = []
        for synced in self.channels[label]:
            if origin and synced == origin.channel:
                continue
            elif not update and ref and ref.ids[synced]:
                log.debug("Skipping already-synced target channel %r: %r", synced, ref)
                continue
            local = base.clone()
            await self._replace_recurse(local, self._replace_ref, synced)
            await self._alter_recurse(local, self._alter_identities, synced)
            await self._alter_recurse(local, self._alter_name)
            queue.append(self._send(synced, local))
        # Just like with plugs, when sending a new (external) message to all channels in a sync, we
        # need to wait for all plugs to complete and have their IDs cached before processing any
        # further messages.
        async with self._lock:
            all_receipts = dict(await gather(*queue))
            ids = {channel: [receipt.id for receipt in receipts]
                   for channel, receipts in all_receipts.items()}
            if ref:
                ref.ids.update(ids)
            else:
                ref = SyncRef(ids, source=msg, origin=origin)
            await self._cache.add(ref)
        # Push a copy of the message to the sync channel, if running.
        if self.plug:
            sent = immp.SentMessage(id_=ref.key, channel=immp.Channel(self.plug, label),
                                    text=msg.text, user=msg.user, action=msg.action,
                                    reply_to=msg.reply_to, joined=msg.joined, left=msg.left,
                                    title=msg.title, attachments=msg.attachments, raw=msg)
            self.plug.queue(sent)
        return ref

    async def delete(self, ref, sent=None):
        queue = []
        for channel, ids in ref.ids.items():
            for id_ in ids:
                if not (sent and sent.channel == channel and sent.id == id_):
                    queue.append(immp.Receipt(id_, channel).delete())
        if queue:
            await gather(*queue)

    async def _replace_ref(self, msg, channel):
        if not isinstance(msg, immp.Receipt):
            log.debug("Not replacing non-receipt message: %r", msg)
            return msg
        base = None
        if isinstance(msg, immp.SentMessage):
            base = immp.Message(text=msg.text, user=msg.user, action=msg.action,
                                reply_to=msg.reply_to, joined=msg.joined, left=msg.left,
                                title=msg.title, attachments=msg.attachments, raw=msg.raw)
        try:
            ref = await self._cache.get(msg)
        except KeyError:
            log.debug("No match for source message: %r", msg)
            return base
        # Given message was a resync of the source message from a synced channel.
        if ref.ids.get(channel):
            log.debug("Found reference to previously synced message: %r", ref.key)
            at = ref.source.at if isinstance(ref.source, immp.Receipt) else None
            best = ref.source or msg
            return immp.SentMessage(id_=ref.ids[channel][0], channel=channel, at=at,
                                    text=best.text, user=best.user, action=best.action,
                                    reply_to=best.reply_to, joined=best.joined, left=best.left,
                                    title=best.title, attachments=best.attachments, raw=best.raw)
        elif channel.plug == msg.channel.plug:
            log.debug("Referenced message has origin plug, not modifying: %r", msg)
            return msg
        else:
            log.debug("Origin message not referenced in the target channel: %r", msg)
            return base

    async def on_receive(self, sent, source, primary):
        await super().on_receive(sent, source, primary)
        try:
            label = self.label_for_channel(sent.channel)
        except immp.ConfigError:
            return
        if not self._accept(source, sent.id):
            return
        async with self._lock:
            # No critical section here, just wait for any pending messages to be sent.
            pass
        ref = None
        update = False
        try:
            ref = await self._cache.get(sent)
        except KeyError:
            if sent.deleted:
                log.debug("Ignoring deleted message not in sync cache: %r", sent)
                return
            else:
                log.debug("Incoming message not in sync cache: %r", sent)
        else:
            if sent.deleted:
                log.debug("Incoming message is a delete, needs sync: %r", sent)
                await self.delete(ref)
                return
            elif (sent.edited and not ref.revisions) or ref.revision(sent):
                log.debug("Incoming message is an update, needs sync: %r", sent)
                update = True
            elif all(ref.ids[channel] for channel in self.channels[label]):
                log.debug("Incoming message already synced: %r", sent)
                return
            else:
                log.debug("Incoming message partially synced: %r", sent)
        log.debug("Sending message to synced channel %r: %r", label, sent.id)
        await self.send(label, source, sent, ref, update)


class ForwardHook(_SyncHookBase):
    """
    Hook to propagate messages from a source channel to one or more destination channels.
    """

    schema = immp.Schema({immp.Optional("users"): immp.Nullable([str]),
                          immp.Optional("groups", dict): {str: [str]}}, _SyncHookBase.schema)

    _channels = immp.ConfigProperty({immp.Channel: [immp.Channel]})
    _groups = immp.ConfigProperty({immp.Group: [immp.Channel]})

    async def _targets(self, channel):
        targets = set()
        if channel in self._channels:
            targets.update(self._channels[channel])
        for group, channels in self._groups.items():
            if await group.has_channel(channel):
                targets.update(channels)
        return targets

    async def send(self, msg, channels):
        """
        Send a message to all channels in this forwarding group.

        Args:
            msg (.Message):
                External message to push.
            channels (.Channel list):
                Set of target channels to forward the message to.
        """
        queue = []
        clone = msg.clone()
        await self._alter_recurse(clone, self._alter_name)
        for synced in channels:
            local = clone.clone()
            await self._alter_recurse(local, self._alter_identities, synced)
            queue.append(self._send(synced, local))
        # Send all the messages in parallel.
        await gather(*queue)

    def _accept(self, msg, id_):
        if not super()._accept(msg, id_):
            return False
        if self.config["users"] is not None:
            if not msg.user or msg.user.id not in self.config["users"]:
                log.debug("Not syncing message from non-whitelisted user: %r", msg.user.id)
                return False
        return True

    async def on_receive(self, sent, source, primary):
        await super().on_receive(sent, source, primary)
        if not primary or not self._accept(source, sent.id):
            return
        targets = await self._targets(sent.channel)
        if targets:
            await self.send(source, targets)
