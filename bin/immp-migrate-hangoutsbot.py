#!/usr/bin/env python3

"""
Migration script for hangoutsbot -> IMMP.

Transfers and converts data for:
- nicknames (identities)
- syncrooms (syncs)
- SlackRTM* (syncs, identities)
- telesync* (syncs, identities)
- tldr (notes)

* API keys in config.json will be used during migration to discover the bot remote user's IDs.
"""

from argparse import ArgumentParser, FileType
from collections import defaultdict
from functools import partial
import json
import logging
import os.path
import re

import anyconfig
from playhouse.db_url import connect
from requests import Session

from immp import Any, Nullable, Optional, Schema
from immp.hook.alerts import SubTrigger
from immp.hook.database import BaseModel
from immp.hook.identitylocal import IdentityGroup, IdentityLink
from immp.hook.notes import Note


log = logging.getLogger(__name__)


class _Schema:

    config = Schema({
        "plugins": [str],
        Optional("sync_rooms", list): [[str]],
        Optional("slackrtm"): Nullable({  # rewrite
            "syncs": [{
                "channel": [str, str],
                "hangout": str
            }],
            "teams": {str: {
                "token": str,
                "admins": [str]
            }}
        }),
        Optional("telesync"): Nullable({
            "api_key": str
        }),
        Optional("forwarding", default=dict): {str: {
            "targets": [str]
        }}
    })

    user = Schema({
        Optional("_hangups", lambda: {"is_self": False}): {"is_self": bool},
        Optional("nickname", ""): str,
        Optional("keywords", list): [str]
    })

    memory = Schema({
        "convmem": {str: {
            "title": str
        }},
        "user_data": {str: user},
        Optional("slackrtm", dict): {str: {
            "identities": {
                "hangouts": {str: str},
                "slack": {str: str}
            }
        }},
        Optional("profilesync", lambda: {"ho2tg": {}}): {  # telesync
            "tg2ho": {str: Any(str, {  # ho2tg is unreliable
                Optional("chat_id"): Nullable(str)  # ho_id is also unreliable
            })}
        },
        Optional("telesync", lambda: {"ho2tg": {}}): {
            "ho2tg": {str: str}  # HO: TG
        },
        Optional("tldr", dict): {  # HO: timestamp: text
            str: {str: str}
        }
    })


class RevDict(dict):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.inverse = {}
        for key, value in self.items():
            if value in self.inverse:
                raise KeyError(value)
            self.inverse[value] = key

    def __setitem__(self, key, value):
        if value in self.inverse and self.inverse[value] != key:
            raise KeyError(value)
        if key in self:
            del self.inverse[self[key]]
        super().__setitem__(key, value)
        self.inverse[value] = key

    def __delitem__(self, key):
        del self.inverse[self[key]]
        super().__delitem__(key)


class MultiRevDict(dict):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.inverse = defaultdict(list)
        for key, value in self.items():
            self.inverse[value].append(key)

    def __setitem__(self, key, value):
        if key in self:
            self.inverse[self[key]].remove(key)
        super().__setitem__(key, value)
        self.inverse[value].append(key)

    def __delitem__(self, key):
        value = self[key]
        self.inverse[value].remove(key)
        if value in self.inverse and not self.inverse[value]:
            del self.inverse[value]
        super().__delitem__(key)


class Data:

    def __init__(self, config, memory, db_url, path):
        self.config = config
        self.memory = memory
        self.database = connect(db_url)
        BaseModel._meta.database.initialize(self.database)
        # Find the bot self user to construct the network ID, needed by other hooks.
        for uid, user in memory["user_data"].items():
            if user["_hangups"]["is_self"]:
                log.debug("Bot user ID: {}".format(uid))
                self.network_id = "hangouts:{}".format(uid)
                break
        else:
            raise RuntimeError("Can't identify bot user ID")
        # Assorted data structures to help with lookups during the migration.
        self.plugs = {"hangouts": {"path": "immp.plug.hangouts.HangoutsPlug",
                                   "config": {"cookie": os.path.join(path, "cookies.json")}}}
        self.channels = RevDict()
        self.hooks = {"db": {"path": "immp.hook.database.DatabaseHook",
                             "config": {"url": db_url}}}
        self.identities = MultiRevDict()
        self.syncs = MultiRevDict()
        self.forwards = defaultdict(list)
        self.subs = {}
        # Internal counters for unique name generation.
        self.user_count = 0
        self.session = Session()

    # Assorted utility methods used during the migration.

    def get_nickname(self, uid):
        try:
            nick = self.memory["user_data"][uid]["nickname"]
        except KeyError:
            log.warning("User missing from user_data: {}".format(uid))
            self.memory["user_data"][uid] = _Schema.user({})
            nick = None
        if nick:
            log.debug("Got existing nickname: {} -> {}".format(uid, nick))
        else:
            while True:
                self.user_count += 1
                nick = "no-name-{}".format(self.user_count)
                if not any(user.get("nickname") and user["nickname"] == nick
                           for user in self.memory["user_data"].values()):
                    break
            # Apply it to our copy of memory for later lookups of the same user.
            self.memory["user_data"][uid]["nickname"] = nick
            log.debug("Assigned new nickname: {} -> {}".format(uid, nick))
        return nick

    def format_title(self, prefix, title):
        if not title:
            return None
        formatted = re.sub(r"[^a-z0-9]+", "-", title.replace("'", ""), flags=re.I).strip("-")
        return "{}:{}".format(prefix, formatted)

    def hangout_title(self, chat):
        # Prefer a name based on the current conv title.
        return (self.format_title("HO", self.memory["convmem"][chat]["title"])
                if chat in self.memory["convmem"] else None)

    def add_channel(self, plug, source, name_getter=None, name_fallback=None):
        if (plug, source) in self.channels.inverse:
            # Already exists under another name.
            name = self.channels.inverse[(plug, source)]
            log.debug("Preferring existing channel: {} -> {}/{}".format(name, plug, source))
        else:
            name = (name_getter(source) if name_getter else None) or name_fallback
            unique = name
            count = 0
            while name in self.channels:
                count += 1
                name = "{}-{}".format(unique, count)
            self.channels[name] = (plug, source)
            log.debug("Assigned new channel: {} -> {}/{}".format(name, plug, source))
        return name

    def add_sync(self, label, *channels):
        for channel in channels:
            if channel in self.syncs:
                # We've already got a sync for that channel.
                # Merge all of those channels into this sync.
                dupe = self.syncs[channel]
                log.debug("Channel already in sync: {} -> {}".format(channel, dupe))
                # Copy the list as it will be emptied during iteration.
                for synced in list(self.syncs.inverse[dupe]):
                    log.debug("Merging channel into sync: {} -> {}".format(synced, label))
                    self.syncs[synced] = label
                log.debug("Removing now-empty sync: {}".format(dupe))
                del self.syncs.inverse[dupe]
            else:
                log.debug("Adding channel to sync: {} -> {}".format(channel, label))
                self.syncs[channel] = label

    def get_synced(self, plug, source):
        try:
            name = self.channels.inverse[(plug, source)]
        except KeyError:
            return (plug, source)
        # Get the containing sync channel if one exists.
        if name in self.syncs:
            return ("sync", self.syncs[name])
        else:
            return (plug, source)

    def add_forward(self, source, *targets):
        log.debug("Adding channels to forwards: {} -> {}".format(source, ", ".join(targets)))
        self.forwards[source].extend(targets)

    # Migrate Hangouts identities for anyone with a nickname set.

    def ho_identities(self):
        for uid, user in self.memory["user_data"].items():
            if user["nickname"]:
                self.identities[(self.network_id, uid)] = user["nickname"]

    # Migrate syncrooms syncs.

    def syncrooms_syncs(self):
        for i, synced in enumerate(self.config["sync_rooms"]):
            channels = [self.add_channel("hangouts", ho, self.hangout_title,
                                         "HO:syncrooms:{}-{}".format(i, j))
                        for j, ho in enumerate(synced)]
            log.debug("Adding Hangouts sync: {}".format(", ".join(channels)))
            self.add_sync("syncrooms-{}".format(i), *channels)

    # Migrate SlackRTM syncs and identities.

    def slackrtm_api(self, name, endpoint, **kwargs):
        kwargs["token"] = self.config["slackrtm"]["teams"][name]["token"]
        data = self.session.get("https://slack.com/api/{}".format(endpoint), params=kwargs).json()
        if data["ok"]:
            return data
        else:
            raise ValueError(data)

    def slackrtm_network_id(self, name):
        data = self.slackrtm_api(name, "auth.test")
        id = "slack:{}:{}".format(data["team_id"], data["user_id"])
        log.debug("Generated Slack network ID: {}".format(id))
        return id

    def slackrtm_title(self, name, channel):
        try:
            data = self.slackrtm_api(name, "conversations.info", channel=channel)
        except ValueError:
            return None
        else:
            return self.format_title("Slack", data["channel"].get("name"))

    def slackrtm_syncs(self):
        for name, team in self.config["slackrtm"]["teams"].items():
            plug = "slack-{}".format(name)
            self.plugs[plug] = {"path": "immp.plug.slack.SlackPlug",
                                "config": {"token": team["token"]}}
            # IMMP requires channel names, but we don't have much to go on.
            for i, sync in enumerate(self.config["slackrtm"]["syncs"]):
                team, channel = sync["channel"]
                hname = self.add_channel("hangouts", sync["hangout"], self.hangout_title,
                                         "HO:SlackRTM:{}".format(i))
                sname = self.add_channel(plug, channel, partial(self.slackrtm_title, name),
                                         "Slack:SlackRTM:{}".format(i))
                log.debug("Adding Slack sync: {} <-> {}".format(hname, sname))
                self.add_sync("Sync:{}".format(hname.replace("HO:", "")), hname, sname)

    def slackrtm_identities(self):
        for name, data in self.memory["slackrtm"].items():
            network = self.slackrtm_network_id(name)
            for ho, sk in data["identities"]["hangouts"].items():
                if data["identities"]["slack"].get(sk) == ho:
                    nick = self.get_nickname(ho)
                    log.debug("Adding Slack identity: {} -> {}, {}/{}".format(nick, ho, name, sk))
                    self.identities[(self.network_id, ho)] = self.identities[(network, sk)] = nick

    # Migrate telesync syncs and identities.

    def telegram_api(self, endpoint, **kwargs):
        # Calls may be rate-limited and hang for 10-15 seconds after a large number of requests.
        data = self.session.get("https://api.telegram.org/bot{}/{}"
                                .format(self.config["telesync"]["api_key"], endpoint),
                                params=kwargs).json()
        if data["ok"]:
            return data
        else:
            raise ValueError(data)

    def telegram_network_id(self):
        data = self.telegram_api("getMe")
        id = "telegram:{}".format(data["result"]["id"])
        log.debug("Generated Telegram network ID: {}".format(id))
        return id

    def telegram_title(self, chat):
        if int(chat) > 0:
            return None  # Private chat, no title.
        try:
            data = self.telegram_api("getChat", chat_id=chat)
        except ValueError:
            return None
        else:
            return self.format_title("TG", data["result"].get("title"))

    def telesync_syncs(self):
        self.plugs["telegram"] = {"path": "immp.plug.telegram.TelegramPlug",
                                  "config": {"token": self.config["telesync"]["api_key"]}}
        for i, (ho, tg) in enumerate(self.memory["telesync"]["ho2tg"].items()):
            hname = self.add_channel("hangouts", ho, self.hangout_title,
                                     "HO:telesync:{}".format(i))
            tname = self.add_channel("telegram", tg, self.telegram_title,
                                     "TG:telesync:{}".format(i))
            log.debug("Adding Telegram sync: {} <-> {}".format(hname, tname))
            self.add_sync("Sync:{}".format(hname.replace("HO:", "")), hname, tname)

    def telesync_identities(self):
        network = self.telegram_network_id()
        for tg, profile in self.memory["profilesync"]["tg2ho"].items():
            # Ignore incomplete profile syncs.
            if isinstance(profile, str) or not profile["chat_id"]:
                continue
            ho = profile["chat_id"]
            nick = self.get_nickname(ho)
            log.debug("Adding Telegram identity: {} -> {}, {}".format(nick, ho, tg))
            self.identities[(self.network_id, ho)] = self.identities[(network, tg)] = nick

    # Migrate Hangouts forwarding.

    def forwarding(self):
        for i, (source, forward) in enumerate(self.config["forwarding"].items()):
            hname = self.add_channel("hangouts", source, self.hangout_title,
                                     "HO:forward:{}-source".format(i))
            hangouts = [self.add_channel("hangouts", target, self.hangout_title,
                                         "HO:forward:{}-{}".format(i, j))
                        for j, target in enumerate(forward["targets"])]
            channels = []
            for ho in hangouts:
                channel = self.syncs.get(ho, ho)
                if channel not in channels:
                    channels.append(channel)
            log.debug("Adding forward: {} -> {} channel(s)".format(hname, len(channels)))
            self.add_forward(hname, *channels)

    # Migrate subscription keywords to Hangouts users.

    def keywords(self):
        tg_network_id = self.telegram_network_id()
        for uid, user in self.memory["user_data"].items():
            if not user["keywords"]:
                continue
            key = (self.network_id, uid)
            try:
                nick = self.identities[key]
            except KeyError:
                identities = [key]
            else:
                identities = [(network, uid) for network, uid in self.identities.inverse[nick]
                              if network in (self.network_id, tg_network_id)]
            for identity in identities:
                self.subs[identity] = filter(None, (re.sub(r"[^\w ]", "", sub).lower()
                                                    for sub in user["keywords"]))

    # Migrate tldrs, assign to synced conversations if relevant.

    def tldr(self):
        self.hooks["notes"] = {"path": "immp.hook.notes.NotesHook"}
        self.database.drop_tables([Note], safe=True)
        self.database.create_tables([Note], safe=True)
        syncs = set()
        for ho, tldr in self.memory["tldr"].items():
            if not tldr:
                continue
            plug, source = self.get_synced("hangouts", ho)
            if plug == "hangouts":
                network = self.network_id
            else:
                # Using a synced channel.
                if source in syncs:
                    # Already processed this tldr via syncrooms sharing.
                    log.debug("Skipping duplicated synced tldr: {}".format(source))
                    continue
                network = "sync:sync"
                syncs.add(source)
            log.debug("Adding {} note(s) to channel: {}/{}".format(len(tldr), plug, source))
            with self.database.atomic():
                for ts, note in sorted(tldr.items()):
                    Note.create(timestamp=int(float(ts)), network=network,
                                channel=source, text=note)

    # Putting it all together now.

    def migrate_all(self):
        self.ho_identities()
        if self.config["sync_rooms"]:
            self.syncrooms_syncs()
        if self.config["slackrtm"]:
            self.slackrtm_syncs()
        if self.memory["slackrtm"]:
            self.slackrtm_identities()
        if self.config["telesync"] and self.memory["telesync"]["ho2tg"]:
            self.telesync_syncs()
        if self.memory["profilesync"]["ho2tg"]:
            self.telesync_identities()
        if self.config["forwarding"]:
            self.forwarding()
        self.keywords()
        if self.memory["tldr"]:
            self.tldr()

    def compile_identities(self):
        self.database.drop_tables([IdentityGroup, IdentityLink], safe=True)
        self.database.create_tables([IdentityGroup, IdentityLink], safe=True)
        self.hooks["identity"] = {"path": "immp.hook.identitylocal.LocalIdentityHook",
                                  "config": {"instance": 1, "plugs": list(self.plugs)}}
        with self.database.atomic():
            for nick, links in self.identities.inverse.items():
                # Invalid password hash by default.
                # Users must `id-password` before they can manage their identities.
                group = IdentityGroup.create(instance=1, name=nick, pwd="")
                for network, user in links:
                    IdentityLink.create(group=group, network=network, user=user)

    def compile_syncs(self):
        identities = "identity" if self.identities else None
        if self.syncs:
            self.hooks["sync"] = {"path": "immp.hook.sync.SyncHook",
                                  "config": {"plug": "sync-migrated",
                                             "channels": dict(self.syncs.inverse),
                                             "identities": identities}}
        if self.forwards:
            self.hooks["forward"] = {"path": "immp.hook.sync.ForwardHook",
                                     "config": {"channels": dict(self.forwards),
                                                "identities": identities}}

    def compile_subs(self):
        self.database.drop_tables([SubTrigger], safe=True)
        self.database.create_tables([SubTrigger], safe=True)
        self.hooks["subs"] = {"path": "immp.hook.alerts.SubscriptionsHook",
                              "config": {"groups": ["migrated"]}}
        with self.database.atomic():
            for (network, user), subs in self.subs.items():
                for text in subs:
                    SubTrigger.create(network=network, user=user, text=text)

    def compile_commands(self):
        commands = {"groups": ["migrated"],
                    "hooks": ["commands"] + list(hook for hook in self.hooks if not hook == "db")}
        self.hooks["commands"] = {"path": "immp.hook.command.CommandHook",
                                  "config": {"prefix": ["/bot "],
                                             "mapping": {"migrated": commands}}}

    def make_config(self):
        if self.identities:
            self.compile_identities()
        if self.syncs or self.forwards:
            self.compile_syncs()
        if self.subs:
            self.compile_subs()
        self.compile_commands()
        return {"plugs": self.plugs,
                "channels": {name: {"plug": plug, "source": source}
                             for name, (plug, source) in self.channels.items()},
                "groups": {"migrated": {"anywhere": list(self.plugs.keys())}},
                "hooks": self.hooks}


def main(args):
    config = _Schema.config(json.load(args.config))
    memory = _Schema.memory(json.load(args.memory))
    data = Data(config, memory, args.database, os.path.dirname(args.config.name))
    data.migrate_all()
    anyconfig.dump(data.make_config(), args.output)


def entrypoint():
    logging.basicConfig(level=logging.DEBUG)
    parser = ArgumentParser(add_help=False)
    parser.add_argument("config", type=FileType("r"))
    parser.add_argument("memory", type=FileType("r"))
    parser.add_argument("output")
    parser.add_argument("database")
    args = parser.parse_args()
    main(args)


if __name__ == "__main__":
    entrypoint()
