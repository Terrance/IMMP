"""
Listen for incoming GitHub webhooks.

Dependencies:
    :class:`.WebHook`

Config:
    route (str):
        Path to expose the webhook request handler.
    secret (str):
        Shared string between the plug and GitHub's servers.  Optional but recommended, must be
        configured to match the webhook on GitHub.

Go to your repository > Settings > Webhooks > Add webhook, set the URL to match the configured
route, and choose the events you wish to handle.  Message summaries for each event will be emitted
on channels matching the full name of each repository (e.g. ``user/repo``).
"""

import hashlib
import hmac
import logging

from aiohttp import web

import immp
from immp.hook.web import WebHook


log = logging.getLogger(__name__)


class _Schema:

    config = immp.Schema({"route": str,
                          immp.Optional("secret"): immp.Nullable(str),
                          immp.Optional("ignore", list): [str]})

    _linked = {"html_url": str}

    _sender = {"id": int, "login": str, "avatar_url": str}

    _repo = {"full_name": str}

    _project = {"name": str, "number": int, **_linked}
    _card = {"note": str, **_linked}

    _release = {"tag": str, immp.Optional("name"): immp.Nullable(str), **_linked}

    _issue = {"number": int, "title": str, **_linked}
    _pull = {immp.Optional("merged", False): bool, **_issue}

    _fork = {"full_name": str, **_linked}

    _page = {"action": str, "title": str, **_linked}

    push = immp.Schema({"ref": str,
                        "after": str,
                        "compare": str,
                        immp.Optional("created", False): bool,
                        immp.Optional("deleted", False): bool,
                        immp.Optional("commits", list): [{"id": str, "message": str}]})

    event = immp.Schema({"sender": _sender,
                         immp.Optional("organization"): immp.Nullable(_sender),
                         immp.Optional("repository"): immp.Nullable(_repo),
                         immp.Optional("project"): immp.Nullable(_project),
                         immp.Optional("project_card"): immp.Nullable(_card),
                         immp.Optional("release"): immp.Nullable(_release),
                         immp.Optional("issue"): immp.Nullable(_issue),
                         immp.Optional("pull_request"): immp.Nullable(_pull),
                         immp.Optional("review"): immp.Nullable(_linked),
                         immp.Optional("forkee"): immp.Nullable(_fork),
                         immp.Optional("pages"): immp.Nullable([_page])})


class GitHubUser(immp.User):
    """
    User present in GitHub.
    """

    @classmethod
    def from_sender(cls, github, sender):
        return cls(id_=sender["id"],
                   plug=github,
                   username=sender["login"],
                   avatar=sender["avatar_url"],
                   raw=sender)

    @property
    def link(self):
        return "https://github.com/{}".format(self.username)

    @link.setter
    def link(self, value):
        pass


class GitHubMessage(immp.Message):
    """
    Repository event originating from GitHub.
    """

    _ACTIONS = {"converted_to_draft": "drafted",
                "prereleased": "pre-released",
                "ready_for_review": "readied",
                "review_requested": "requested review of",
                "review_request_removed": "removed review request of",
                "synchronize": "updated"}

    @classmethod
    def _action_text(cls, github, action):
        if not action:
            return None
        elif action in github.config["ignore"]:
            raise NotImplementedError
        else:
            return cls._ACTIONS.get(action, action)

    @classmethod
    def _repo_text(cls, github, type_, event):
        text = None
        repo = event["repository"]
        name = repo["full_name"]
        name_seg = immp.Segment(name, link=repo["html_url"])
        action = cls._action_text(github, event.get("action"))
        issue = event.get("issue")
        pull = event.get("pull_request")
        if type_ == "repository":
            text = immp.RichText([immp.Segment("{} repository ".format(action)), name_seg])
        elif type_ == "push":
            push = _Schema.push(event)
            count = len(push["commits"])
            desc = "{} commits".format(count) if count > 1 else push["after"][:7]
            root, target = push["ref"].split("/", 2)[1:]
            join = None
            if root == "tags":
                tag = True
            elif root == "heads":
                tag = False
            else:
                raise NotImplementedError
            if push["deleted"]:
                action = "deleted {}".format("tag" if tag else "branch")
            elif tag:
                action, join = "tagged", "as"
            else:
                action, join = "pushed", ("to new branch" if push["created"] else "to")
            text = immp.RichText([immp.Segment("{} ".format(action))])
            if join:
                text.append(immp.Segment(desc, link=push["compare"]),
                            immp.Segment(" {} ".format(join)))
            text.append(immp.Segment("{} of {}".format(target, name)))
            for commit in push["commits"]:
                text.append(immp.Segment("\n\N{BULLET} {}: {}"
                                         .format(commit["id"][:7],
                                                 commit["message"].split("\n")[0])))
        elif type_ == "release":
            release = event["release"]
            desc = ("{} ({} {})".format(release["name"], name, release["tag_name"])
                    if release["name"] else release["tag_name"])
            text = immp.RichText([immp.Segment("{} release ".format(action)),
                                  immp.Segment(desc, link=release["html_url"])])
        elif type_ == "issues":
            desc = "{} ({}#{})".format(issue["title"], name, issue["number"])
            text = immp.RichText([immp.Segment("{} issue ".format(action)),
                                  immp.Segment(desc, link=issue["html_url"])])
        elif type_ == "issue_comment":
            comment = event["comment"]
            desc = "{} ({}#{})".format(issue["title"], name, issue["number"])
            text = immp.RichText([immp.Segment("{} a ".format(action)),
                                  immp.Segment("comment", link=comment["html_url"]),
                                  immp.Segment(" on issue "),
                                  immp.Segment(desc, link=issue["html_url"])])
        elif type_ == "pull_request":
            if action == "closed" and pull["merged"]:
                action = "merged"
            desc = "{} ({}#{})".format(pull["title"], name, pull["number"])
            text = immp.RichText([immp.Segment("{} pull request ".format(action)),
                                  immp.Segment(desc, link=pull["html_url"])])
        elif type_ == "pull_request_review":
            review = event["review"]
            desc = "{} ({}#{})".format(pull["title"], name, pull["number"])
            text = immp.RichText([immp.Segment("{} a ".format(action)),
                                  immp.Segment("review", link=review["html_url"]),
                                  immp.Segment(" on pull request "),
                                  immp.Segment(desc, link=pull["html_url"])])
        elif type_ == "pull_request_review_comment":
            comment = event["comment"]
            desc = "{} ({}#{})".format(pull["title"], name, pull["number"])
            text = immp.RichText([immp.Segment("{} a ".format(action)),
                                  immp.Segment("comment", link=comment["html_url"]),
                                  immp.Segment(" on pull request "),
                                  immp.Segment(desc, link=pull["html_url"])])
        elif type_ == "project":
            project = event["project"]
            desc = "{} ({}#{})".format(project["name"], name, project["number"])
            text = immp.RichText([immp.Segment("{} project ".format(action)),
                                  immp.Segment(desc, link=project["html_url"])])
        elif type_ == "project_card":
            card = event["project_card"]
            text = immp.RichText([immp.Segment("{} ".format(action)),
                                  immp.Segment("card", link=card["html_url"]),
                                  immp.Segment(" in project:\n"),
                                  immp.Segment(card["note"])])
        elif type_ == "gollum":
            text = immp.RichText()
            for i, page in enumerate(event["pages"]):
                if i:
                    text.append(immp.Segment(", "))
                text.append(immp.Segment("{} {} wiki page ".format(page["action"], name)),
                            immp.Segment(page["title"], link=page["html_url"]))
        elif type_ == "fork":
            fork = event["forkee"]
            text = immp.RichText([immp.Segment("forked {} to ".format(name)),
                                  immp.Segment(fork["full_name"], link=fork["html_url"])])
        elif type_ == "watch":
            text = immp.RichText([immp.Segment("starred "), name_seg])
        elif type_ == "public":
            text = immp.RichText([immp.Segment("made "), name_seg, immp.Segment(" public")])
        return text

    @classmethod
    def from_event(cls, github, type_, id_, event):
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
        text = None
        if event["repository"]:
            channel = immp.Channel(github, event["repository"]["full_name"])
            text = cls._repo_text(github, type_, event)
        if not text:
            raise NotImplementedError
        user = GitHubUser.from_sender(github, event["sender"])
        return immp.SentMessage(id_=id_,
                                channel=channel,
                                text=text,
                                user=user,
                                action=True,
                                raw=event)


class GitHubPlug(immp.Plug):
    """
    Plug for incoming `GitHub <https://github.com>`_ notifications.
    """

    schema = _Schema.config

    network_name = "GitHub"
    network_id = "github"

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        self.ctx = None

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
            data = await request.json()
        except ValueError:
            log.warning("Bad content type, webhook needs configuring as JSON")
            raise web.HTTPBadRequest
        try:
            type_ = request.headers["X-GitHub-Event"]
            id_ = request.headers["X-GitHub-Delivery"]
            event = _Schema.event(data)
        except (KeyError, ValueError):
            raise web.HTTPBadRequest
        if type_ == "ping":
            target = None
            if event["repository"]:
                target = "repository", event["repository"]["full_name"]
            elif event["organization"]:
                target = "organisation", event["organization"]["login"]
            if target:
                log.debug("Received ping event for %s %r", *target)
            else:
                log.warning("Received ping event for unknown target")
        else:
            try:
                self.queue(GitHubMessage.from_event(self, type_, id_, event))
            except NotImplementedError:
                log.debug("Ignoring unrecognised event type %r", type_)
        return web.Response()
