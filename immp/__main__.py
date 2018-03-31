import asyncio
import logging
import sys

import anyconfig
from voluptuous import REMOVE_EXTRA, Optional, Schema

from immp import Channel, Host, resolve_import


logging.getLogger("anyconfig").setLevel(logging.WARNING)

log = logging.getLogger(__name__)


_schema = Schema({"plugs": {str: {"path": str, Optional("config", default=dict): dict}},
                  "channels": {str: {"plug": str, "source": object}},
                  "hooks": {str: {"path": str, Optional("config", default=dict): dict}}},
                 extra=REMOVE_EXTRA, required=True)


def main(config):
    host = Host()
    for name, spec in config["plugs"].items():
        cls = resolve_import(spec["path"])
        host.add_plug(cls(name, spec["config"], host))
    for name, spec in config["channels"].items():
        plug = host.plugs[spec["plug"]]
        host.add_channel(Channel(name, plug, spec["source"]))
    for name, spec in config["hooks"].items():
        cls = resolve_import(spec["path"])
        host.add_hook(cls(name, spec["config"], host))
    loop = asyncio.get_event_loop()
    task = loop.create_task(host.run())
    try:
        log.debug("Starting host")
        loop.run_until_complete(task)
    except KeyboardInterrupt:
        log.debug("Interrupt received")
        task.cancel()
        loop.run_until_complete(task)
    finally:
        loop.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    if len(sys.argv) < 2:
        exit("Usage: python -m immp <config file>")
    main(_schema(anyconfig.load(sys.argv[1])))
