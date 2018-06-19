import asyncio
import logging
import logging.config
import sys

import anyconfig
from voluptuous import REMOVE_EXTRA, Any, Optional, Schema

from immp import Channel, Host, resolve_import


_schema = Schema({Optional("path", default=list): [str],
                  "plugs": {str: {"path": str, Optional("config", default=dict): dict}},
                  "channels": {str: {"plug": str, "source": object}},
                  "hooks": {str: {"path": str, Optional("config", default=dict): dict}},
                  Optional("logging", default=None): Any(dict, None)},
                 extra=REMOVE_EXTRA, required=True)


class LocalFilter(logging.Filter):

    def filter(self, record):
        return record.name == "__main__" or record.name.startswith("immp.")


def main(config):
    for path in config["path"]:
        sys.path.append(path)
    if config["logging"]:
        logging.config.dictConfig(config["logging"])
    else:
        logging.basicConfig(level=logging.INFO)
        for handler in logging.root.handlers:
            handler.addFilter(LocalFilter())
    log = logging.getLogger(__name__)
    log.info("Creating plugs and hooks")
    host = Host()
    for name, spec in config["plugs"].items():
        cls = resolve_import(spec["path"])
        host.add_plug(cls(name, spec["config"], host))
    for name, spec in config["channels"].items():
        plug = host.plugs[spec["plug"]]
        host.add_channel(name, Channel(plug, spec["source"]))
    for name, spec in config["hooks"].items():
        cls = resolve_import(spec["path"])
        host.add_hook(cls(name, spec["config"], host))
    loop = asyncio.get_event_loop()
    task = loop.create_task(host.run())
    try:
        log.info("Starting host")
        loop.run_until_complete(task)
    except KeyboardInterrupt:
        log.info("Closing on interrupt")
        task.cancel()
        loop.run_until_complete(task)
    finally:
        loop.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        exit("Usage: python -m immp <config file>")
    main(_schema(anyconfig.load(sys.argv[1])))
