from argparse import ArgumentParser
import asyncio
import logging
import logging.config
import sys

import anyconfig
from voluptuous import ALLOW_EXTRA, REMOVE_EXTRA, Any, Optional, Schema

from immp import Channel, ConfigError, Group, Host, resolve_import
from immp.hook.runner import RunnerHook


class _Schema:

    _plugs = Any({str: {"path": str, Optional("config", default=dict): dict}}, {})

    _hooks = Any({str: {"path": str,
                        Optional("priority", default=None): Any(int, None),
                        Optional("config", default=dict): dict}}, {})

    _channels = Any({str: {"plug": str, "source": str}}, {})

    _logging = Schema({Optional("disable_existing_loggers", default=False): bool},
                      extra=ALLOW_EXTRA)

    config = Schema({Optional("path", default=list): [str],
                     Optional("plugs", default=dict): _plugs,
                     Optional("channels", default=dict): _channels,
                     Optional("groups", default=dict): Any({str: dict}, {}),
                     Optional("hooks", default=dict): _hooks,
                     Optional("logging", default=None): Any(_logging, None)},
                    extra=REMOVE_EXTRA, required=True)


class LocalFilter(logging.Filter):

    def filter(self, record):
        return record.name == "__main__" or record.name.split(".", 1)[0] == "immp"


def config_to_host(config, path, write):
    host = Host()
    for name, spec in config["plugs"].items():
        cls = resolve_import(spec["path"])
        host.add_plug(cls(name, spec["config"], host))
    for name, spec in config["channels"].items():
        plug = host.plugs[spec["plug"]]
        host.add_channel(name, Channel(plug, spec["source"]))
    for name, group in config["groups"].items():
        host.add_group(Group(name, group, host))
    for name, spec in config["hooks"].items():
        cls = resolve_import(spec["path"])
        host.add_hook(cls(name, spec["config"], host), spec["priority"])
    try:
        host.add_hook(RunnerHook("runner", {}, host))
    except ConfigError:
        # Prefer existing hook defined within the config itself.
        pass
    host.resources[RunnerHook].load(config, path, write)
    host.loaded()
    return host


def init(logs, paths):
    if logs:
        logging.config.dictConfig(logs)
    else:
        logging.basicConfig(level=logging.INFO)
        for handler in logging.root.handlers:
            handler.addFilter(LocalFilter())
    for search in paths:
        sys.path.append(search)


def main(path, write=False):
    config = _Schema.config(anyconfig.load(path))
    init(config["logging"], config["path"])
    log = logging.getLogger(__name__)
    log.info("Creating plugs and hooks")
    host = config_to_host(config, path, write)
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
        if write:
            host.resources[RunnerHook].write_config()


def entrypoint():
    parser = ArgumentParser(prog="python -m immp", add_help=False)
    parser.add_argument("-w", "--write", action="store_true")
    parser.add_argument("file", metavar="FILE")
    args = parser.parse_args()
    main(args.file, args.write)


if __name__ == "__main__":
    entrypoint()
