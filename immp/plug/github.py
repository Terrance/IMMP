import hashlib
import hmac
import logging
import re

from aiohttp import web
from voluptuous import ALLOW_EXTRA, Any, Optional, Schema

import immp
from immp.hook.web import WebHook


log = logging.getLogger(__name__)


class _Schema:

    config = Schema({"route": str,
                     Optional("secret", default=None): Any(str, None)},
                    extra=ALLOW_EXTRA, required=True)

    _sender = {"id": int,
               "login": str,
               "avatar_url": str,
               "html_url": str}

    event = Schema({"repository": {"full_name": str},
                    "sender": _sender},
                   extra=ALLOW_EXTRA, required=True)


class GitHubMessage(immp.Message):

    @classmethod
    def _url_to_source(self, url):
        match = re.match("https://api.github.com/repos/([^/]+)/([^/]+)", url)
        return match.group(1) if match else None

    @classmethod
    def from_event(cls, github, type, id, event):
        repo = event["repository"]["full_name"]
        channel = immp.Channel(github, repo)
        sender = event["sender"]
        user = immp.User(id=sender["id"],
                         plug=github,
                         username=sender["login"],
                         avatar=sender["avatar_url"],
                         link=sender["html_url"],
                         raw=sender)
        text = None
        if type == "push":
            count = len(event["commits"])
            desc = "{} commits".format(count) if count > 1 else event["after"][:7]
            ref = event["ref"].split("/")[1:]
            target = "/".join(ref[1:])
            if ref[0] == "tags":
                action, join = "tagged", "as"
            elif ref[0] == "heads":
                action, join = "pushed", "to"
            else:
                raise NotImplementedError
            text = immp.RichText([immp.Segment("{} ".format(action)),
                                  immp.Segment(desc, link=event["compare"]),
                                  immp.Segment(" {} {} {}".format(join, repo, target))])
        elif type == "release":
            release = event["release"]
            desc = ("{} ({} {})".format(release["name"], repo, release["tag_name"])
                    if release["name"] else release["tag_name"])
            text = immp.RichText([immp.Segment("{} release ".format(event["action"])),
                                  immp.Segment(desc, link=release["html_url"])])
        elif type == "issues":
            issue = event["issue"]
            desc = "{} ({}#{})".format(issue["title"], repo, issue["number"])
            text = immp.RichText([immp.Segment("{} issue ".format(event["action"])),
                                  immp.Segment(desc, link=issue["html_url"])])
        elif type == "pull_request":
            pull = event["pull_request"]
            desc = "{} ({}#{})".format(pull["title"], repo, pull["number"])
            text = immp.RichText([immp.Segment("{} pull request ".format(event["action"])),
                                  immp.Segment(desc, link=pull["html_url"])])
        if text:
            return immp.SentMessage(id=id,
                                    channel=channel,
                                    user=user,
                                    text=text,
                                    action=True,
                                    raw=event)
        else:
            raise NotImplementedError


class GitHubPlug(immp.Plug):

    network_name = "GitHub"
    network_id = "github"

    def __init__(self, name, config, host):
        super().__init__(name, _Schema.config(config), host)
        self._session = None

    def on_load(self):
        log.debug("Registering webhook route")
        self.ctx = self.host.resources[WebHook].context(__name__, self.config["route"])
        self.ctx.route("POST", "", self.handle)

    async def handle(self, request):
        if self.config["secret"]:
            try:
                body = await request.read()
            except ValueError:
                raise web.HTTPBadRequest
            try:
                alg, sig = request.headers["X-Hub-Signature"].split("=", 1)
            except (KeyError, ValueError):
                log.warning("No signature on event, secret needs configuring on webhook")
                raise web.HTTPUnauthorized
            match = hmac.new(self.config["secret"].encode("utf-8"), body, hashlib.sha1).hexdigest()
            if alg != "sha1" or sig != match:
                log.warning("Bad signature on event")
                raise web.HTTPUnauthorized
        try:
            type = request.headers["X-GitHub-Event"]
            id = request.headers["X-GitHub-Delivery"]
            event = _Schema.event(await request.json())
        except (KeyError, ValueError):
            raise web.HTTPBadRequest
        if type == "ping":
            log.debug("Received ping event for %s", event["repository"]["full_name"])
        else:
            try:
                self.queue(GitHubMessage.from_event(self, type, id, event))
            except NotImplementedError:
                log.debug("Ignoring unrecognised event type %r", type)
                log.debug("%r", event)
        return web.Response()
