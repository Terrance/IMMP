"""
Run a webserver for other plugs or hooks to accept incoming HTTP requests.

Config:
    host (str):
        Hostname or IP address to bind to.
    port (int):
        Port number to bind to.

As the server is unauthenticated, you will typically want to bind it to localhost, and proxy it
behind a full webserver like nginx to separate out routes, lock down access and so on.
"""


import logging

from aiohttp import web
from voluptuous import ALLOW_EXTRA, Schema

import immp


log = logging.getLogger(__name__)


class _Schema:

    config = Schema({"host": str, "port": int}, extra=ALLOW_EXTRA, required=True)


class WebHook(immp.ResourceHook):
    """
    Hook that provides a generic webserver, which other hooks can bind routes to.

    Attributes:
        app (aiohttp.web.Application):
            Web application instance, used to add new routes.
    """

    def __init__(self, name, config, host):
        super().__init__(name, _Schema.config(config), host)
        self.app = web.Application()
        self._runner = web.AppRunner(self.app)
        self._site = None

    async def start(self):
        log.debug("Starting server on {}:{}".format(self.config["host"], self.config["port"]))
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, self.config["host"], self.config["port"])
        await self._site.start()

    async def stop(self):
        if self._site:
            log.debug("Stopping server")
            await self._runner.cleanup()
            self._site = None
