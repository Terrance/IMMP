import asyncio
import logging
import sys

import anyconfig

from imirror import Host


logging.getLogger("anyconfig").setLevel(logging.WARNING)


async def main(config):
    host = Host()
    for name, spec in config.get("transports", []).items():
        host.add_transport(name, spec["path"], spec.get("config") or {})
    for name, spec in config.get("channels", []).items():
        host.add_channel(name, spec["transport"], spec["source"])
    for name, spec in config.get("receivers", []).items():
        host.add_receiver(name, spec["path"], spec.get("config") or {})
    try:
        await host.run()
    finally:
        await host.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    if len(sys.argv) < 2:
        exit("Usage: python -m imirror <config file>")
    config = anyconfig.load(sys.argv[1])
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main(config))
    finally:
        loop.close()
