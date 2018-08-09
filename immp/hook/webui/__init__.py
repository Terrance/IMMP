"""
Web-based management UI for a host instance.

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

from functools import wraps
import json
import logging

from aiohttp import web
import aiohttp_jinja2
from voluptuous import ALLOW_EXTRA, Schema

import immp
from immp.hook.runner import RunnerHook
from immp.hook.web import WebHook


log = logging.getLogger(__name__)


class _Schema:

    config = Schema({"route": str}, extra=ALLOW_EXTRA, required=True)


class WebUIHook(immp.ResourceHook):
    """
    Hook providing web-based configuration management for a running host instance.
    """

    def __init__(self, name, config, host):
        super().__init__(name, _Schema.config(config), host)

    def on_load(self):
        log.debug("Registering routes")
        runner = self.host.resources.get(RunnerHook)
        self.ctx = self.host.resources[WebHook].context(__name__, self.config["route"],
                                                        {"hook_url_for": self.hook_url_for,
                                                         "runner": runner})
        # Home:
        self.ctx.route("GET", "", self.noop, "main.j2", "main")
        # Add:
        self.ctx.route("GET", "add", self.noop, "add.j2", "add")
        self.ctx.route("POST", "add", self.add, name="add:post")
        # Plugs:
        self.ctx.route("GET", "plug/{name}", self.plug, "plug.j2")
        self.ctx.route("POST", "plug/{name}/stop", self.plug_stop)
        self.ctx.route("POST", "plug/{name}/start", self.plug_start)
        self.ctx.route("POST", "plug/{name}/config", self.plug_config)
        self.ctx.route("GET", "plug/{name}/remove", self.plug, "plug_remove.j2", "plug_remove")
        self.ctx.route("POST", "plug/{name}/remove", self.plug_remove, name="plug_remove:post")
        # Channels:
        self.ctx.route("GET", "channel/{name}", self.channel, "channel.j2")
        self.ctx.route("POST", "channel", self.channel_add)
        self.ctx.route("POST", "channel/{name}/remove", self.channel_remove)
        self.ctx.route("GET", "plug/{plug}/channel/{source}", self.plug_channel, "channel.j2")
        # Hooks:
        self.ctx.route("GET", "resource/{cls}", self.hook, "hook.j2", "resource")
        self.ctx.route("POST", "resource/{cls}/stop", self.hook_stop, name="resource_stop")
        self.ctx.route("POST", "resource/{cls}/start", self.hook_start, name="resource_start")
        self.ctx.route("POST", "resource/{cls}/config", self.hook_config, name="resource_config")
        self.ctx.route("GET", "resource/{cls}/remove", self.hook, "hook_remove.j2", "resource_remove")
        self.ctx.route("POST", "resource/{cls}/remove", self.hook_remove, name="resource_remove:post")
        self.ctx.route("GET", "hook/{name}", self.hook, "hook.j2")
        self.ctx.route("POST", "hook/{name}/stop", self.hook_stop)
        self.ctx.route("POST", "hook/{name}/start", self.hook_start)
        self.ctx.route("POST", "hook/{name}/config", self.hook_config)
        self.ctx.route("GET", "hook/{name}/remove", self.hook, "hook_remove.j2", "hook_remove")
        self.ctx.route("POST", "hook/{name}/remove", self.hook_remove, name="hook_remove:post")

    async def noop(self, request):
        return {}

    async def add(self, request):
        post = await request.post()
        try:
            path = post["path"]
            name = post["name"]
            config = post["config"] or "{}"
        except KeyError:
            raise web.HTTPBadRequest
        if not path or not name:
            raise web.HTTPBadRequest
        try:
            config = json.loads(config)
        except ValueError:
            raise web.HTTPBadRequest
        try:
            cls = immp.resolve_import(path)
        except ImportError:
            raise web.HTTPNotFound
        path = "{}.{}".format(cls.__module__, cls.__name__)
        if issubclass(cls, immp.Plug):
            inst = cls(name, config, self.host)
            self.host.add_plug(inst)
            raise web.HTTPFound(self.ctx.url_for("plug", name=name))
        elif issubclass(cls, immp.Hook):
            inst = cls(name, config, self.host)
            self.host.add_hook(inst)
            if issubclass(cls, immp.ResourceHook):
                raise web.HTTPFound(self.ctx.url_for("resource", cls=path))
            else:
                raise web.HTTPFound(self.ctx.url_for("hook", name=name))
        else:
            raise web.HTTPNotFound

    def _resolve_plug(self, request):
        try:
            return self.host.plugs[request.match_info["name"]]
        except KeyError:
            raise web.HTTPNotFound

    async def plug(self, request):
        plug = self._resolve_plug(request)
        return {"plug": plug,
                "channels": {name: channel for name, channel in self.host.channels.items()
                             if channel.plug == plug}}

    async def plug_stop(self, request):
        plug = self._resolve_plug(request)
        await plug.close()
        raise web.HTTPFound(self.ctx.url_for("plug", name=plug.name))

    async def plug_start(self, request):
        plug = self._resolve_plug(request)
        await plug.open()
        raise web.HTTPFound(self.ctx.url_for("plug", name=plug.name))

    async def plug_config(self, request):
        plug = self._resolve_plug(request)
        post = await request.post()
        if "config" not in post:
            raise web.HTTPBadRequest
        try:
            config = json.loads(post["config"])
        except ValueError:
            raise web.HTTPBadRequest
        plug.config.clear()
        plug.config.update(config)
        raise web.HTTPFound(self.ctx.url_for("plug", name=plug.name))

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
        except KeyError:
            raise web.HTTPNotFound

    async def channel(self, request):
        name, channel = self._resolve_channel(request)
        raise web.HTTPFound(self.ctx.url_for("channel_source", plug=channel.plug.name,
                                             source=channel.source))

    async def channel_add(self, request):
        post = await request.post()
        try:
            plug = post["plug"]
            name = post["name"]
            source = post["source"]
        except KeyError:
            raise web.HTTPBadRequest
        if not (plug and name and source):
            raise web.HTTPBadRequest
        if name in self.host.channels:
            raise web.HTTPBadRequest
        if plug not in self.host.plugs:
            raise web.HTTPNotFound
        self.host.add_channel(name, immp.Channel(self.host.plugs[plug], source))
        raise web.HTTPFound(self.ctx.url_for("plug", name=plug))

    async def channel_remove(self, request):
        name, channel = self._resolve_channel(request)
        self.host.remove_channel(name)
        raise web.HTTPFound(self.ctx.url_for("plug", name=channel.plug.name))

    async def plug_channel(self, request):
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
        return {"hook": hook,
                "resource": isinstance(hook, immp.ResourceHook),
                "can_stop": can_stop}

    async def hook_stop(self, request):
        hook = self._resolve_hook(request)
        if isinstance(hook, (WebHook, WebUIHook)):
            # This will hang due to trying to serve this request at the same time.
            raise web.HTTPBadRequest
        await hook.close()
        raise web.HTTPFound(self.hook_url_for(hook, None))

    async def hook_start(self, request):
        hook = self._resolve_hook(request)
        await hook.open()
        raise web.HTTPFound(self.hook_url_for(hook, None))

    async def hook_config(self, request):
        hook = self._resolve_hook(request)
        post = await request.post()
        if "config" not in post:
            raise web.HTTPBadRequest
        try:
            config = json.loads(post["config"])
        except ValueError:
            raise web.HTTPBadRequest
        hook.config.clear()
        hook.config.update(config)
        raise web.HTTPFound(self.hook_url_for(hook, None))

    async def hook_remove(self, request):
        hook = self._resolve_hook(request)
        await hook.stop()
        self.host.remove_hook(hook.name)
        raise web.HTTPFound(self.ctx.url_for("main"))