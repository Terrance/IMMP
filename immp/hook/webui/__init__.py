"""
Web-based management UI for a host instance.

Dependencies:
    :class:`.WebHook` with templating

    Extra name: ``webui``

    `Docutils <https://docutils.sourceforge.io>`_:
        Used to render module documentation if available.

Config:
    route (str):
        Path to expose the UI pages under.

This is a simple control panel to add or update plugs and hooks in a running system.

.. warning::
    The UI provides no authentication, and exposes all internals of the system (including any
    secret keys, as well as remote code execution via loading new plugs/hooks).  You should only
    run this hook on a trusted system, and bind it to localhost to avoid external access.

    If remotely accessible configuration is desired, use SSH with port forwarding to securely
    connect to your server.  Failing that, you could proxy via a webserver like nginx and enable
    HTTP authentication, or use a service layer like oauth2_proxy.
"""

from asyncio import ensure_future, gather
from collections import defaultdict
from datetime import datetime, timedelta
from importlib import import_module
from inspect import cleandoc
import json
import logging
import re
import sys

try:
    from pkg_resources import get_distribution
except ImportError:
    # Setuptools might not be available.
    get_distribution = None

from aiohttp import web

try:
    from docutils.core import publish_parts
except ImportError:
    publish_parts = None

import immp
from immp.hook.runner import RunnerHook
from immp.hook.web import WebHook


log = logging.getLogger(__name__)


def _render_module_doc(obj):
    doc = import_module(obj.__module__).__doc__
    if not doc:
        return (None, None)
    doc = re.sub(r":[a-z]+:`\.?(.+?)`", r"``\1``", cleandoc(doc))
    html = None
    if publish_parts:
        parts = publish_parts(doc, writer_name="html", settings_overrides={"report_level": 4})
        html = parts["body"]
    return (doc, html)


class WebUIHook(immp.ResourceHook):
    """
    Hook providing web-based configuration management for a running host instance.
    """

    schema = immp.Schema({"route": str})

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        self.ctx = None
        self._host_version = get_distribution(immp.__name__).version if get_distribution else None
        self._python_version = ".".join(str(v) for v in sys.version_info[:3])

    def on_load(self):
        log.debug("Registering routes")
        runner = self.host.resources.get(RunnerHook)
        self.ctx = self.host.resources[WebHook].context(self.config["route"], __name__,
                                                        env={"hook_url_for": self.hook_url_for,
                                                             "group_summary": self.group_summary,
                                                             "runner": runner,
                                                             # `zip` doesn't seem to work.
                                                             "zipped": zip})
        # Home:
        self.ctx.route("GET", "", self.main, "main.j2", "main")
        # Add:
        self.ctx.route("GET", "add", self.noop, "add.j2", "add")
        self.ctx.route("POST", "add", self.add, "add.j2", "add:post")
        # Plugs:
        self.ctx.route("GET", "plug/{name}", self.plug, "plug.j2")
        self.ctx.route("POST", "plug/{name}/disable", self.plug_disable)
        self.ctx.route("POST", "plug/{name}/enable", self.plug_enable)
        self.ctx.route("POST", "plug/{name}/stop", self.plug_stop)
        self.ctx.route("POST", "plug/{name}/start", self.plug_start)
        self.ctx.route("POST", "plug/{name}/config", self.plug_config)
        self.ctx.route("GET", "plug/{name}/channels", self.plug_channels, "plug_channels.j2")
        self.ctx.route("GET", "plug/{name}/remove", self.plug, "plug_remove.j2", "plug_remove")
        self.ctx.route("POST", "plug/{name}/remove", self.plug_remove, name="plug_remove:post")
        # Channels:
        self.ctx.route("GET", "channel/{name}", self.named_channel, "channel.j2")
        self.ctx.route("POST", "channel", self.named_channel_add)
        self.ctx.route("POST", "channel/{name}/remove", self.named_channel_remove)
        self.ctx.route("GET", "plug/{plug}/channel/{source}", self.channel, "channel.j2")
        self.ctx.route("POST", "plug/{plug}/channel/{source}/migrate", self.channel_migration)
        self.ctx.route("POST", "plug/{plug}/channel/{source}/invite", self.channel_invite)
        self.ctx.route("POST", "plug/{plug}/channel/{source}/kick/{user}", self.channel_kick)
        # Groups:
        self.ctx.route("GET", "group/{name}", self.group, "group.j2")
        self.ctx.route("POST", "group", self.group_add)
        self.ctx.route("POST", "group/{name}/remove", self.group_remove)
        self.ctx.route("POST", "group/{name}/config", self.group_config)
        # Hooks:
        self.ctx.route("GET", "resource/{cls}", self.hook, "hook.j2", "resource")
        self.ctx.route("POST", "resource/{cls}/disable", self.hook_disable, name="resource_disable")
        self.ctx.route("POST", "resource/{cls}/enable", self.hook_enable, name="resource_enable")
        self.ctx.route("POST", "resource/{cls}/stop", self.hook_stop, name="resource_stop")
        self.ctx.route("POST", "resource/{cls}/start", self.hook_start, name="resource_start")
        self.ctx.route("POST", "resource/{cls}/config", self.hook_config, name="resource_config")
        self.ctx.route("GET", "resource/{cls}/remove", self.hook,
                       "hook_remove.j2", "resource_remove")
        self.ctx.route("POST", "resource/{cls}/remove", self.hook_remove,
                       name="resource_remove:post")
        self.ctx.route("GET", "hook/{name}", self.hook, "hook.j2")
        self.ctx.route("POST", "hook/{name}/disable", self.hook_disable)
        self.ctx.route("POST", "hook/{name}/enable", self.hook_enable)
        self.ctx.route("POST", "hook/{name}/stop", self.hook_stop)
        self.ctx.route("POST", "hook/{name}/start", self.hook_start)
        self.ctx.route("POST", "hook/{name}/config", self.hook_config)
        self.ctx.route("GET", "hook/{name}/remove", self.hook, "hook_remove.j2", "hook_remove")
        self.ctx.route("POST", "hook/{name}/remove", self.hook_remove, name="hook_remove:post")

    async def noop(self, request):
        return {}

    async def main(self, request):
        loggers = ([("<root>", logging.getLevelName(logging.root.level))] +
                   [(module, logging.getLevelName(logger.level))
                    for module, logger in sorted(logging.root.manager.loggerDict.items())
                    if isinstance(logger, logging.Logger) and logger.level != logging.NOTSET])
        uptime = None
        if self.host.started:
            uptime = datetime.now() - self.host.started
            # Drop microseconds from the delta (no datetime.replace equivalent for timedelta).
            uptime = timedelta(days=uptime.days, seconds=uptime.seconds)
        return {"uptime": uptime,
                "loggers": loggers,
                "versions": [("Python", self._python_version), ("IMMP", self._host_version)]}

    async def add(self, request):
        post = await request.post()
        try:
            path = post["path"]
        except KeyError:
            raise web.HTTPBadRequest
        if not path:
            raise web.HTTPBadRequest
        try:
            cls = immp.resolve_import(path)
        except ImportError:
            raise web.HTTPNotFound
        if "schema" in post:
            config = post.get("config") or ""
            doc, doc_html = _render_module_doc(cls)
            return {"path": path,
                    "config": config,
                    "class": cls,
                    "doc": doc,
                    "doc_html": doc_html,
                    "hook": issubclass(cls, immp.Hook)}
        try:
            name = post["name"]
        except KeyError:
            raise web.HTTPBadRequest
        if not name:
            raise web.HTTPBadRequest
        elif name in self.host:
            raise web.HTTPConflict
        if cls.schema:
            try:
                config = json.loads(post["config"])
            except (KeyError, ValueError):
                raise web.HTTPBadRequest
        else:
            config = None
        if not issubclass(cls, (immp.Plug, immp.Hook)):
            raise web.HTTPNotFound
        try:
            inst = cls(name, config, self.host)
        except immp.Invalid:
            raise web.HTTPNotAcceptable
        if issubclass(cls, immp.Plug):
            self.host.add_plug(inst)
            raise web.HTTPFound(self.ctx.url_for("plug", name=name))
        elif issubclass(cls, immp.Hook):
            try:
                priority = int(post["priority"]) if post["priority"] else None
            except (KeyError, ValueError):
                raise web.HTTPBadRequest
            self.host.add_hook(inst, priority)
            if issubclass(cls, immp.ResourceHook):
                raise web.HTTPFound(self.ctx.url_for("resource", cls=path))
            else:
                raise web.HTTPFound(self.ctx.url_for("hook", name=name))

    def _resolve_plug(self, request):
        try:
            return self.host.plugs[request.match_info["name"]]
        except KeyError:
            raise web.HTTPNotFound

    async def plug(self, request):
        plug = self._resolve_plug(request)
        name = None
        source = request.query.get("source")
        if source:
            title = await immp.Channel(plug, source).title()
            name = re.sub(r"[^a-z0-9]+", "-", title, flags=re.I).strip("-") if title else ""
        doc, doc_html = _render_module_doc(plug.__class__)
        return {"plug": plug,
                "doc": doc,
                "doc_html": doc_html,
                "add_name": name,
                "add_source": source,
                "channels": {name: channel for name, channel in self.host.channels.items()
                             if channel.plug == plug}}

    async def plug_disable(self, request):
        plug = self._resolve_plug(request)
        plug.disable()
        raise web.HTTPFound(self.ctx.url_for("plug", name=plug.name))

    async def plug_enable(self, request):
        plug = self._resolve_plug(request)
        plug.enable()
        raise web.HTTPFound(self.ctx.url_for("plug", name=plug.name))

    async def plug_stop(self, request):
        plug = self._resolve_plug(request)
        ensure_future(plug.close())
        raise web.HTTPFound(self.ctx.url_for("plug", name=plug.name))

    async def plug_start(self, request):
        plug = self._resolve_plug(request)
        ensure_future(plug.open())
        raise web.HTTPFound(self.ctx.url_for("plug", name=plug.name))

    async def plug_config(self, request):
        plug = self._resolve_plug(request)
        if not plug.schema:
            raise web.HTTPNotFound
        post = await request.post()
        if "config" not in post:
            raise web.HTTPBadRequest
        try:
            config = json.loads(post["config"])
        except ValueError:
            raise web.HTTPNotAcceptable
        try:
            plug.config = plug.schema(config)
        except immp.Invalid:
            raise web.HTTPNotAcceptable
        raise web.HTTPFound(self.ctx.url_for("plug", name=plug.name))

    async def plug_channels(self, request):
        plug = self._resolve_plug(request)
        public, private = await gather(plug.public_channels(), plug.private_channels())
        titles = await gather(*(channel.title() for channel in public)) if public else []
        all_members = await gather(*(channel.members() for channel in private)) if private else []
        users = []
        for members in all_members:
            if not members:
                users.append([])
                continue
            systems = await gather(*(member.is_system() for member in members)) if members else []
            users.append([member for member, system in zip(members, systems) if not system])
        channels = defaultdict(list)
        for name, channel in self.host.channels.items():
            if channel.plug == plug:
                channels[channel].append(name)
        return {"plug": plug,
                "channels": channels,
                "public": public,
                "titles": titles,
                "private": private,
                "users": users}

    async def plug_remove(self, request):
        plug = self._resolve_plug(request)
        await plug.stop()
        self.host.remove_plug(plug.name)
        raise web.HTTPFound(self.ctx.url_for("main"))

    def _resolve_channel(self, request):
        try:
            if "name" in request.match_info:
                name = request.match_info["name"]
                return name, self.host.channels[name]
            elif "plug" in request.match_info:
                plug = self.host.plugs[request.match_info["plug"]]
                return None, immp.Channel(plug, request.match_info["source"])
            else:
                raise web.HTTPBadRequest
        except KeyError:
            raise web.HTTPNotFound

    async def named_channel(self, request):
        name, channel = self._resolve_channel(request)
        raise web.HTTPFound(self.ctx.url_for("channel", plug=channel.plug.name,
                                             source=channel.source))

    async def named_channel_add(self, request):
        post = await request.post()
        try:
            plug = post["plug"]
            name = post["name"]
            source = post["source"]
        except KeyError:
            raise web.HTTPBadRequest
        if not (plug and name and source):
            raise web.HTTPBadRequest
        if name in self.host:
            raise web.HTTPConflict
        if plug not in self.host.plugs:
            raise web.HTTPNotFound
        self.host.add_channel(name, immp.Channel(self.host.plugs[plug], source))
        raise web.HTTPFound(self.ctx.url_for("plug", name=plug))

    async def named_channel_remove(self, request):
        name, channel = self._resolve_channel(request)
        self.host.remove_channel(name)
        raise web.HTTPFound(self.ctx.url_for("plug", name=channel.plug.name))

    async def channel(self, request):
        name, channel = self._resolve_channel(request)
        private = await channel.is_private()
        title = await channel.title()
        link = await channel.link()
        members = await channel.members()
        return {"name": name,
                "channel": channel,
                "private": private,
                "title_": title,
                "link": link,
                "members": members}

    async def channel_migration(self, request):
        _, old = self._resolve_channel(request)
        post = await request.post()
        if "name" in post:
            new = self.host.channels[post["name"]]
        elif "plug" in post and "source" in post:
            new = immp.Channel(self.host.plugs[post["plug"]], post["source"])
        else:
            raise web.HTTPBadRequest
        await self.host.channel_migrate(old, new)
        raise web.HTTPFound(self.ctx.url_for("channel", plug=old.plug.name, source=old.source))

    async def channel_invite(self, request):
        name, channel = self._resolve_channel(request)
        if channel.plug.virtual:
            raise web.HTTPBadRequest
        post = await request.post()
        try:
            id_ = post["user"]
        except KeyError:
            raise web.HTTPBadRequest
        members = await channel.members()
        if members is None:
            raise web.HTTPBadRequest
        elif id_ in (member.id for member in members):
            raise web.HTTPBadRequest
        user = await channel.plug.user_from_id(id_)
        if user is None:
            raise web.HTTPBadRequest
        await channel.invite(user)
        raise web.HTTPFound(self.ctx.url_for("channel", plug=channel.plug.name,
                                             source=channel.source))

    async def channel_kick(self, request):
        _, channel = self._resolve_channel(request)
        if channel.plug.virtual:
            raise web.HTTPBadRequest
        id_ = request.match_info["user"]
        members = await channel.members()
        if members is None:
            raise web.HTTPBadRequest
        elif id_ not in (member.id for member in members):
            raise web.HTTPBadRequest
        user = await channel.plug.user_from_id(id_)
        if user is None:
            raise web.HTTPBadRequest
        await channel.remove(user)
        raise web.HTTPFound(self.ctx.url_for("channel", plug=channel.plug.name,
                                             source=channel.source))

    def _resolve_group(self, request):
        try:
            return self.host.groups[request.match_info["name"]]
        except KeyError:
            raise web.HTTPNotFound

    @staticmethod
    def group_summary(group):
        summary = []
        for key in ("anywhere", "private", "shared", "named", "channels"):
            count = len(group.config[key])
            if count:
                summary.append("{} {}".format(count, key))
        return ", ".join(summary) if summary else "Empty group"

    async def group(self, request):
        group = self._resolve_group(request)
        return {"group": group}

    async def group_add(self, request):
        post = await request.post()
        try:
            name = post["name"]
        except KeyError:
            raise web.HTTPBadRequest
        if not name:
            raise web.HTTPBadRequest
        if name in self.host:
            raise web.HTTPConflict
        self.host.add_group(immp.Group(name, {}, self.host))
        raise web.HTTPFound(self.ctx.url_for("group", name=name))

    async def group_remove(self, request):
        group = self._resolve_group(request)
        self.host.remove_group(group.name)
        raise web.HTTPFound(self.ctx.url_for("main"))

    async def group_config(self, request):
        group = self._resolve_group(request)
        post = await request.post()
        if "config" not in post:
            raise web.HTTPBadRequest
        try:
            config = json.loads(post["config"])
        except ValueError:
            raise web.HTTPBadRequest
        try:
            group.config = group.schema(config)
        except immp.Invalid:
            raise web.HTTPNotAcceptable
        raise web.HTTPFound(self.ctx.url_for("group", name=group.name))

    def _resolve_hook(self, request):
        if "name" in request.match_info:
            try:
                return self.host.hooks[request.match_info["name"]]
            except KeyError:
                pass
        elif "cls" in request.match_info:
            for cls, hook in self.host.resources.items():
                if request.match_info["cls"] == "{}.{}".format(cls.__module__, cls.__name__):
                    return hook
        raise web.HTTPNotFound

    def hook_url_for(self, hook, name_=None, **kwargs):
        if isinstance(hook, immp.ResourceHook):
            cls = "{}.{}".format(hook.__class__.__module__, hook.__class__.__name__)
            route = "resource_{}".format(name_) if name_ else "resource"
            return self.ctx.url_for(route, cls=cls, **kwargs)
        else:
            route = "hook_{}".format(name_) if name_ else "hook"
            return self.ctx.url_for(route, name=hook.name, **kwargs)

    async def hook(self, request):
        hook = self._resolve_hook(request)
        can_stop = not isinstance(hook, (WebHook, WebUIHook))
        doc, doc_html = _render_module_doc(hook)
        return {"hook": hook,
                "doc": doc,
                "doc_html": doc_html,
                "resource": isinstance(hook, immp.ResourceHook),
                "priority": self.host.priority.get(hook.name),
                "can_stop": can_stop}

    async def hook_disable(self, request):
        hook = self._resolve_hook(request)
        hook.disable()
        raise web.HTTPFound(self.hook_url_for(hook, None))

    async def hook_enable(self, request):
        hook = self._resolve_hook(request)
        hook.enable()
        raise web.HTTPFound(self.hook_url_for(hook, None))

    async def hook_stop(self, request):
        hook = self._resolve_hook(request)
        if isinstance(hook, (WebHook, WebUIHook)):
            # This will hang due to trying to serve this request at the same time.
            raise web.HTTPBadRequest
        ensure_future(hook.close())
        raise web.HTTPFound(self.hook_url_for(hook, None))

    async def hook_start(self, request):
        hook = self._resolve_hook(request)
        ensure_future(hook.open())
        raise web.HTTPFound(self.hook_url_for(hook, None))

    async def hook_config(self, request):
        hook = self._resolve_hook(request)
        post = await request.post()
        try:
            priority = int(post["priority"]) if post["priority"] else None
        except (KeyError, ValueError):
            raise web.HTTPBadRequest
        self.host.prioritise_hook(hook.name, priority)
        if hook.schema:
            if "config" not in post:
                raise web.HTTPBadRequest
            try:
                config = json.loads(post["config"])
            except ValueError:
                raise web.HTTPNotAcceptable
            try:
                hook.config = hook.schema(config)
            except immp.Invalid:
                raise web.HTTPNotAcceptable
        raise web.HTTPFound(self.hook_url_for(hook, None))

    async def hook_remove(self, request):
        hook = self._resolve_hook(request)
        await hook.stop()
        self.host.remove_hook(hook.name)
        raise web.HTTPFound(self.ctx.url_for("main"))
