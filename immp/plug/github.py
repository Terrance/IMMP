"""
Listen for incoming GitHub webhooks.

Config:
    route (str):
        Path to expose the webhook request handler.
    secret (str):
        Shared string between the plug and GitHub's servers.  Optional but recommended, must be
        configured to match the webhook on GitHub.

Go to your repository > Settings > Webhooks > Add webhook, set the URL to match the configured
route, and choose the events you wish to handle.  Message summaries for each event will be emitted
on channels matching the full name of each repository (e.g. ``user/repo``).

.. note::
    This plug requires an active :class:`.WebHook` to receive incoming messages.
"""

import hashlib
import hmac
import logging

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
    """
    Repository event originating from GitHub.
    """

    @classmethod
    def from_event(cls, github, type, id, event):
        """
        Convert a `GitHub webhook <https://developer.github.com/webhooks/>`_ payload to a
        :class:`.Message`.

        Args:
            github (.GitHubPlug):
                Related plug instance that provides the event.
            type (str):
                Event type name from the ``X-GitHub-Event`` header.
            id (str):
                GUID of the event delivery from the ``X-GitHub-Delivery`` header.
            event (dict):
                GitHub webhook payload.

        Returns:
            .GitHubMessage:
                Parsed message object.
        """
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
            for commit in event["commits"]:
                text.append(immp.Segment("\n* "),
                            immp.Segment(commit["id"][:7], code=True),
                            immp.Segment(" - {}".format(commit["message"])))
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
        elif type == "project":
            project = event["project"]
            desc = "{} ({}#{})".format(project["name"], repo, project["number"])
            text = immp.RichText([immp.Segment("{} project ".format(event["action"])),
                                  immp.Segment(desc, link=project["html_url"])])
        elif type == "project_card":
            card = event["project_card"]
            text = immp.RichText([immp.Segment("{} ".format(event["action"])),
                                  immp.Segment("card", link=card["html_url"]),
                                  immp.Segment(" in project:\n"),
                                  immp.Segment(card["note"])])
        elif type == "gollum":
            text = immp.RichText()
            for i, page in enumerate(event["pages"]):
                if i:
                    text.append(immp.Segment(", "))
                text.append(immp.Segment("{} {} wiki page ".format(page["action"], repo)),
                            immp.Segment(page["title"], link=page["html_url"]))
        elif type == "fork":
            fork = event["forkee"]
            text = immp.RichText([immp.Segment("forked {} to ".format(repo)),
                                  immp.Segment(fork["full_name"], link=fork["html_url"])])
        elif type == "watch":
            text = immp.RichText([immp.Segment("starred {}".format(repo))])
        if text:
            return immp.SentMessage(id=id,
                                    channel=channel,
                                    text=text,
                                    user=user,
                                    action=True,
                                    raw=event)
        else:
            raise NotImplementedError


class GitHubPlug(immp.Plug):
    """
    Plug for incoming `GitHub <https://github.com>`_ notifications.
    """

    network_name = "GitHub"
    network_id = "github"

    def __init__(self, name, config, host):
        super().__init__(name, _Schema.config(config), host)
        self._session = None

    def on_load(self):
        log.debug("Registering webhook route")
        self.ctx = self.host.resources[WebHook].context(self.config["route"], __name__)
        self.ctx.route("POST", "", self.handle)

    async def channel_title(self, channel):
        return channel.source

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
        return web.Response()
