from argparse import ArgumentParser
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


def config_to_host(config):
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
    return host


def host_to_config(host):
    config = {"plugs": {}, "channels": {}, "hooks": {}}
    for name, plug in host.plugs.items():
        if plug.virtual:
            continue
        path = "{}.{}".format(plug.__class__.__module__, plug.__class__.__name__)
        config["plugs"][name] = {"path": path, "config": plug.config}
    for name, channel in host.channels.items():
        if channel.plug.virtual:
            continue
        config["channels"][name] = {"plug": channel.plug.name, "source": channel.source}
    for cls, hook in host.resources.items():
        if hook.virtual:
            continue
        path = "{}.{}".format(cls.__module__, cls.__name__)
        config["hooks"][hook.name] = {"path": path, "config": hook.config}
    for name, hook in host.hooks.items():
        if hook.virtual:
            continue
        path = "{}.{}".format(hook.__class__.__module__, hook.__class__.__name__)
        config["hooks"][name] = {"path": path, "config": hook.config}
    return config


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
    config = _schema(anyconfig.load(path))
    init(config["logging"], config["path"])
    log = logging.getLogger(__name__)
    host = config_to_host(config)
    log.info("Creating plugs and hooks")
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
            log.info("Writing config file")
            config.update(host_to_config(host))
            anyconfig.dump(config, path)


def parse():
    parser = ArgumentParser(prog="python -m immp", add_help=False)
    parser.add_argument("-w", "--write", action="store_true")
    parser.add_argument("file", metavar="FILE")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse()
    main(args.file, args.write)
