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
import json
import logging
import os.path
import re

import anyconfig
from playhouse.db_url import connect
import requests
from voluptuous import REMOVE_EXTRA, Any, Optional, Schema

from immp.hook.database import BaseModel
from immp.hook.identity import IdentityGroup, IdentityLink
from immp.hook.notes import Note


log = logging.getLogger(__name__)


class _Schema:

    config = Schema({
            "plugins": [str],
            Optional("sync_rooms", default=list): Any([[str]], []),
            Optional("slackrtm", default=None): Any({  # rewrite
                "syncs": [{
                    "channel": [str, str],
                    "hangout": str
                }],
                "teams": {str: {
                    "token": str,
                    "admins": [str]
                }}
            }, None),
            Optional("telesync", default=None): Any({
                "api_key": str
            }, None)
        }, extra=REMOVE_EXTRA, required=True)

    memory = Schema({
            "convmem": {str: {
                "title": str
            }},
            "user_data": {str: {
                Optional("_hangups", default=lambda: {"is_self": False}): {"is_self": bool},
                Optional("nickname", default=""): str
            }},
            Optional("slackrtm", default=dict): Any({str: {
                "identities": {
                    "hangouts": {str: str},
                    "slack": {str: str}
                }
            }}, {}),
            Optional("profilesync", default=lambda: {"ho2tg": {}}): {  # telesync
                "ho2tg": Any({str: str}, {})  # HO: TG or HO: "VERIFY000..."
            },
            Optional("telesync", default=lambda: {"ho2tg": {}}): {
                "ho2tg": Any({str: str}, {})  # HO: TG
            },
            Optional("tldr", default=dict): Any({  # HO: timestamp: text
                str: Any({str: str}, {})
            }, {})
        }, extra=REMOVE_EXTRA, required=True)


class RevDict(dict):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.inverse = {}
        for key, value in self.items():
            if value in self.inverse:
                raise KeyError(value)
            self.inverse[value] = key

    def __setitem__(self, key, value):
        if value in self.inverse and not self.inverse[value] == key:
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
        self.inverse = {}
        for key, value in self.items():
            self.inverse.setdefault(value, []).append(key)

    def __setitem__(self, key, value):
        if key in self:
            self.inverse[self[key]].remove(key)
        super().__setitem__(key, value)
        self.inverse.setdefault(value, []).append(key)

    def __delitem__(self, key):
        self.inverse.setdefault(self[key], []).remove(key)
        if self[key] in self.inverse and not self.inverse[self[key]]:
            del self.inverse[self[key]]
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
        # Internal counters for unique name generation.
        self._count = 0

    # Assorted utility methods used during the migration.

    def get_nickname(self, uid):
        try:
            nick = self.memory["user_data"][uid]["nickname"]
        except KeyError:
            log.warning("User missing from user_data: {}".format(uid))
            self.memory["user_data"][uid] = {}
            nick = None
        if nick:
            log.debug("Got existing nickname: {} -> {}".format(uid, nick))
        else:
            while True:
                self._count += 1
                nick = "no-name-{}".format(self._count)
                if not any(user.get("nickname") and user["nickname"] == nick
                           for user in self.memory["user_data"].values()):
                    break
            # Apply it to our copy of memory for later lookups of the same user.
            self.memory["user_data"][uid]["nickname"] = nick
            log.debug("Assigned new nickname: {} -> {}".format(uid, nick))
        return nick

    def add_channel(self, plug, source, name=None):
        if (plug, source) in self.channels.inverse:
            # Already exists under another name.
            name = self.channels.inverse[(plug, source)]
            log.debug("Preferring existing channel: {} -> {}/{}".format(name, plug, source))
        else:
            if plug == "hangouts" and source in self.memory["convmem"]:
                # Prefer a name based on the current conv title.
                title = self.memory["convmem"][source]["title"]
                name = re.sub(r"[^a-z0-9]+", "-", title, flags=re.I).strip("-")
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

    # Migrate Hangouts identities for anyone with a nickname set.

    def ho_identities(self):
        for uid, user in self.memory["user_data"].items():
            if user["nickname"]:
                self.identities[(self.network_id, uid)] = user["nickname"]

    # Migrate syncrooms syncs.

    def syncrooms_syncs(self):
        for i, synced in enumerate(self.config["sync_rooms"]):
            channels = [self.add_channel("hangouts", ho, "hangouts-syrm-{}-{}".format(i, j))
                        for j, ho in enumerate(synced)]
            log.debug("Adding Hangouts sync: {}".format(", ".join(channels)))
            self.add_sync("syncrooms-{}".format(i), *channels)

    # Migrate SlackRTM syncs and identities.

    def _slackrtm_network_id(self, name):
        token = self.config["slackrtm"]["teams"][name]["token"]
        url = "https://slack.com/api/auth.test?token={}".format(token)
        data = requests.get(url).json()
        id = "slack:{}:{}".format(data["team_id"], data["user_id"])
        log.debug("Generated Slack network ID: {}".format(id))
        return id

    def slackrtm_syncs(self):
        for name, team in self.config["slackrtm"]["teams"].items():
            plug = "slack-{}".format(name)
            self.plugs[plug] = {"path": "immp.plug.slack.SlackPlug",
                                "config": {"token": team["token"]}}
            # IMMP requires channel names, but we don't have much to go on.
            for i, sync in enumerate(self.config["slackrtm"]["syncs"]):
                team, channel = sync["channel"]
                hname = self.add_channel("hangouts", sync["hangout"], "hangouts-srtm-{}".format(i))
                sname = self.add_channel(plug, channel, "slack-srtm-{}".format(i))
                log.debug("Adding Slack sync: {} <-> {}".format(hname, sname))
                self.add_sync("slack-{}-{}".format(name, i), hname, sname)

    def slackrtm_identities(self):
        for name, data in self.memory["slackrtm"].items():
            network = self._slackrtm_network_id(name)
            for ho, sk in data["identities"]["hangouts"].items():
                if data["identities"]["slack"].get(sk) == ho:
                    nick = self.get_nickname(ho)
                    log.debug("Adding Slack identity: {} -> {}, {}/{}".format(nick, ho, name, sk))
                    self.identities[(self.network_id, ho)] = self.identities[(network, sk)] = nick

    # Migrate telesync syncs and identities.

    def _telegram_network_id(self):
        url = "https://api.telegram.org/bot{}/getMe".format(self.config["telesync"]["api_key"])
        id = "telegram:{}".format(requests.get(url).json()["result"]["id"])
        log.debug("Generated Telegram network ID: {}".format(id))
        return id

    def telesync_syncs(self):
        self.plugs["telegram"] = {"path": "immp.plug.telegram.TelegramPlug",
                                  "config": {"token": self.config["telesync"]["api_key"]}}
        for i, (ho, tg) in enumerate(self.memory["telesync"]["ho2tg"].items()):
            hname = self.add_channel("hangouts", ho, "hangouts-tlsy-{}".format(i))
            tname = self.add_channel("telegram", int(tg), "telegram-tlsy-{}".format(i))
            log.debug("Adding Telegram sync: {} <-> {}".format(hname, tname))
            self.add_sync("telegram-{}".format(i), hname, tname)

    def telesync_identities(self):
        network = self._telegram_network_id()
        for ho, tg in self.memory["profilesync"]["ho2tg"].items():
            # Ignore incomplete profile syncs.
            if not (ho.startswith("VERIFY") or tg.startswith("VERIFY")):
                nick = self.get_nickname(ho)
                log.debug("Adding Telegram identity: {} -> {}, {}".format(nick, ho, tg))
                self.identities[(self.network_id, ho)] = self.identities[(network, tg)] = nick

    # Migrate tldrs, assign to synced conversations if relevant.

    def tldr(self):
        self.hooks["notes"] = {"path": "immp.hook.notes.NotesHook"}
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
        if self.memory["tldr"]:
            self.tldr()

    def compile_identities(self):
        self.database.create_tables([IdentityGroup, IdentityLink], safe=True)
        self.hooks["identity"] = {"path": "immp.hook.identity.IdentityHook",
                                  "config": {"database": "db", "plugs": list(self.plugs)}}
        with self.database.atomic():
            for nick, links in self.identities.inverse.items():
                # Invalid password hash by default.
                # Users must `id-password` before they can manage their identities.
                group = IdentityGroup.create(name=nick, pwd="")
                for network, user in links:
                    IdentityLink.create(group=group, network=network, user=user)

    def compile_syncs(self):
        self.hooks["sync"] = {"path": "immp.hook.sync.SyncHook",
                              "config": {"plug": "sync", "channels": self.syncs.inverse,
                                         "identities": "identity" if self.identities else None}}

    def make_config(self):
        if self.identities:
            self.compile_identities()
        if self.syncs:
            self.compile_syncs()
        self.hooks["commands"] = {"path": "immp.hook.command.CommandHook",
                                  "config": {"prefix": "/bot ", "plugs": list(self.plugs),
                                             "hooks": list(hook for hook in self.hooks
                                                           if not hook == "db")}}
        return {"plugs": self.plugs,
                "channels": {name: {"plug": plug, "source": source}
                             for name, (plug, source) in self.channels.items()},
                "hooks": self.hooks}


def main(args):
    config = _Schema.config(json.load(args.config))
    memory = _Schema.memory(json.load(args.memory))
    data = Data(config, memory, args.database, os.path.dirname(args.config.name))
    data.migrate_all()
    anyconfig.dump(data.make_config(), args.output)


def parse():
    parser = ArgumentParser(add_help=False)
    parser.add_argument("config", type=FileType("r"))
    parser.add_argument("memory", type=FileType("r"))
    parser.add_argument("output")
    parser.add_argument("database")
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    args = parse()
    main(args)
