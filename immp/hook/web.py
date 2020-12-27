"""
Run a webserver for other plugs or hooks to accept incoming HTTP requests.

Requirements:
    `aiohttp <https://aiohttp.readthedocs.io/en/latest/>`_

    `aiohttp_jinja2 <https://aiohttp-jinja2.readthedocs.io/en/latest/>`_:
        Required for page templating.

Config:
    host (str):
        Hostname or IP address to bind to.
    port (int):
        Port number to bind to.

As the server is unauthenticated, you will typically want to bind it to ``localhost``, and proxy it
behind a full web server like nginx to separate out routes, lock down access and so on.
"""

from functools import wraps
import json
import logging
import os.path

from aiohttp import web

import immp


try:
    import aiohttp_jinja2
    from jinja2 import PackageLoader, PrefixLoader
except ImportError:
    aiohttp_jinja2 = PackageLoader = PrefixLoader = None


log = logging.getLogger(__name__)


class WebContext:
    """
    Abstraction from :mod:`aiohttp` to provide routing and templating to other hooks.

    Attributes:
        hook (.WebHook):
            Parent hook instance providing the webserver.
        prefix (str):
            URL prefix acting as the base path.
        module (str):
            Dotted module name of the Python module using this context (``__name__``).
        path (str):
            Base path of the module (``os.path.dirname(__file__)``), needed for static routes.
        env (dict):
            Additional variables to make available in the Jinja context.  The default context is:

            * :data:`immp`, the top-level :mod:`immp` module
            * :data:`host`, the running :class:`.Host` instance
            * :data:`ctx`, this :class:`.WebContext` instance
            * :data:`request`, the current :class:`aiohttp.web.Request` instance
    """

    def __init__(self, hook, prefix, module, path=None, env=None):
        self.hook = hook
        self.prefix = prefix
        self.module = module
        self.path = path
        self.env = {"ctx": self}
        if env:
            self.env.update(env)
        self._routes = {}
        self.hook.add_loader(self.module)

    def route(self, method, route, fn, template=None, name=None):
        """
        Add a new route to the webserver.

        Args:
            method (str):
                HTTP verb for the route (``GET``, ``POST`` etc.).
            route (str):
                URL pattern to match.
            fn (function):
                Callable to render the response, accepting a :class:`aiohttp.web.Request` argument.
            template (str):
                Optional template path, relative to the module path.  If specified, the view
                callable should return a context :class:`dict` which is passed to the template.
            name (str):
                Custom name for the route, defaulting to the function name if not specified.
        """
        name = name or fn.__name__
        if name in self._routes:
            raise KeyError(name)
        if template:
            fn = self._jinja(fn, template)
        route = self.hook.add_route(method, "{}/{}".format(self.prefix, route), fn,
                                    name="{}:{}".format(self.module, name))
        self._routes[name] = route

    def static(self, route, path, name=None):
        """
        Add a new route to the webserver.

        Args:
            route (str):
                URL pattern to match.
            path (str):
                Filesystem location relative to the base path.
            name (str):
                Custom name for the route, defaulting to the function name if not specified.
        """
        name = name or path
        if name in self._routes:
            raise KeyError(name)
        route = self.hook.add_static("{}/{}".format(self.prefix, route),
                                     os.path.join(self.path, path),
                                     name="{}:{}".format(self.module, name))
        self._routes[name] = route

    def _jinja(self, fn, path):
        @wraps(fn)
        async def inner(request):
            env = dict(self.env)
            env["request"] = request
            env.update(await fn(request))
            return env
        if not aiohttp_jinja2:
            raise immp.HookError("Templating requires Jinja2 and aiohttp_jinja2")
        outer = aiohttp_jinja2.template("{}/{}".format(self.module, path))
        return outer(inner)

    def url_for(self, name_, **kwargs):
        """
        Generate an absolute URL for the named route.

        Args:
            name (str):
                Route name, either the function name or the custom name given during registration.

        Returns:
            str:
                Relative URL to the corresponding page.
        """
        return self._routes[name_].url_for(**{k: v.replace("/", "%2F") for k, v in kwargs.items()})

    def __repr__(self):
        return "<{}: {}>".format(self.__class__.__name__, self.prefix)


class WebHook(immp.ResourceHook):
    """
    Hook that provides a generic webserver, which other hooks can bind routes to.

    Attributes:
        app (aiohttp.web.Application):
            Web application instance, used to add new routes.
    """

    schema = immp.Schema(immp.Any({immp.Optional("host"): immp.Nullable(str),
                                   "port": int},
                                  {"path": str}))

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        self.app = web.Application()
        if aiohttp_jinja2:
            # Empty mapping by default, other hooks can add to this via add_loader().
            self._loader = PrefixLoader({})
            self._jinja = aiohttp_jinja2.setup(self.app, loader=self._loader)
            self._jinja.filters["json"] = json.dumps
            self._jinja.globals["immp"] = immp
            self._jinja.globals["host"] = self.host
        self._runner = web.AppRunner(self.app)
        self._site = None
        self._contexts = {}

    def context(self, prefix, module, path=None, env=None):
        """
        Retrieve a context for the current module.

        Args:
            prefix (str):
                URL prefix acting as the base path.
            module (str):
                Dotted module name of the Python module using this context.  Callers should use
                :data:`__name__` from the root of their module.
            path (str):
                Base path of the module, needed for static routes.  Callers should use
                ``os.path.dirname(__file__)`` from the root of their module.
            env (dict):
                Additional variables to make available in the Jinja context.  See
                :attr:`.WebContext.env` for details.

        Returns:
            .WebContext:
                Linked context instance for that module.
        """
        self._contexts[module] = WebContext(self, prefix, module, path, env)
        return self._contexts[module]

    def add_loader(self, module):
        """
        Register a Jinja2 package loader for the given module.

        Args:
            module (str):
                Module name to register.
        """
        if not aiohttp_jinja2:
            raise immp.HookError("Loaders require Jinja2 and aiohttp_jinja2")
        self._loader.mapping[module] = PackageLoader(module)

    def add_route(self, *args, **kwargs):
        """
        Equivalent to :meth:`aiohttp.web.UrlDispatcher.add_route`.
        """
        return self.app.router.add_route(*args, **kwargs)

    def add_static(self, *args, **kwargs):
        """
        Equivalent to :meth:`aiohttp.web.UrlDispatcher.add_static`.
        """
        return self.app.router.add_static(*args, **kwargs)

    async def start(self):
        await self._runner.setup()
        if "path" in self.config:
            log.debug("Starting server on socket %s", self.config["path"])
            self._site = web.UnixSite(self._runner, self.config["path"])
        else:
            log.debug("Starting server on host %s:%d", self.config["host"], self.config["port"])
            self._site = web.TCPSite(self._runner, self.config["host"], self.config["port"])
        await self._site.start()

    async def stop(self):
        if self._site:
            log.debug("Stopping server")
            await self._runner.cleanup()
            self._site = None
