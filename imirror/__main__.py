import asyncio
import logging
import sys

import anyconfig
from voluptuous import Schema, Optional, REMOVE_EXTRA

from imirror import Host


logging.getLogger("anyconfig").setLevel(logging.WARNING)

log = logging.getLogger(__name__)


_schema = Schema({"transports": {str: {"path": str, Optional("config", default=dict): dict}},
                  "channels": {str: {"transport": str, "source": object}},
                  "receivers": {str: {"path": str, Optional("config", default=dict): dict}}},
                 extra=REMOVE_EXTRA, required=True)


def main(config):
    host = Host()
    for name, spec in config["transports"].items():
        host.add_transport(name, spec["path"], spec.get("config") or {})
    for name, spec in config["channels"].items():
        host.add_channel(name, spec["transport"], spec["source"])
    for name, spec in config["receivers"].items():
        host.add_receiver(name, spec["path"], spec.get("config") or {})
    loop = asyncio.get_event_loop()
    try:
        log.debug("Starting host")
        loop.run_until_complete(host.run())
    except KeyboardInterrupt:
        log.debug("Interrupt received")
    finally:
        log.debug("Closing host")
        loop.run_until_complete(host.close())
        loop.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    if len(sys.argv) < 2:
        exit("Usage: python -m imirror <config file>")
    main(_schema(anyconfig.load(sys.argv[1])))
