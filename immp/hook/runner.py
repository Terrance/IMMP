"""
Utility methods related to loading and saving config files, as used by the console entry point to
save the config on exit.  Creating and running :class:`.Host` via ``immp`` or ``python -m immp``
will attach a :class:`RunnerHook` to its resources.

Requirements:
    Extra name: ``runner``

    `anyconfig <https://python-anyconfig.readthedocs.io/en/latest/>`_

    Any format-specific libraries for config files (e.g. PyYAML for YAML files)

Commands:
    run-write:
        Force a write of the live config out to the configured file.
"""

from asyncio import get_event_loop
import json
import logging
import logging.config
import signal
import sys

try:
    import anyconfig
except ImportError:
    anyconfig = None

import immp


log = logging.getLogger(__name__)


class _Schema:

    _openable = {"path": str,
                 immp.Optional("enabled", True): bool,
                 immp.Optional("config", dict): dict}

    _plugs = {str: _openable}

    _hooks = {str: immp.Schema({immp.Optional("priority"): immp.Nullable(int)}, _openable)}

    _channels = {str: {"plug": str, "source": str}}

    _logging = {immp.Optional("disable_existing_loggers", False): bool}

    config = immp.Schema({immp.Optional("path", list): [str],
                          immp.Optional("plugs", dict): _plugs,
                          immp.Optional("channels", dict): _channels,
                          immp.Optional("groups", dict): {str: dict},
                          immp.Optional("hooks", dict): _hooks,
                          immp.Optional("logging"): immp.Nullable(_logging)})


def _load_file(path):
    if anyconfig:
        # Note: anyconfig v0.11.0 parses ac_template=False as positive (fixed in v0.11.1).
        # https://github.com/ssato/python-anyconfig/pull/126
        return anyconfig.load(path, ac_template=False)
    else:
        with open(path, "r") as reader:
            return json.load(reader)


def _save_file(data, path):
    if anyconfig:
        anyconfig.dump(data, path)
    else:
        with open(path, "w") as writer:
            json.dump(data, writer)


def config_to_host(config, path, write):
    host = immp.Host()
    base = dict(config)
    for name, spec in base.pop("plugs").items():
        cls = immp.resolve_import(spec["path"])
        host.add_plug(cls(name, spec["config"], host), spec["enabled"])
    for name, spec in base.pop("channels").items():
        plug = host.plugs[spec["plug"]]
        host.add_channel(name, immp.Channel(plug, spec["source"]))
    for name, group in base.pop("groups").items():
        host.add_group(immp.Group(name, group, host))
    for name, spec in base.pop("hooks").items():
        cls = immp.resolve_import(spec["path"])
        host.add_hook(cls(name, spec["config"], host), spec["enabled"], spec["priority"])
    try:
        host.add_hook(RunnerHook("runner", {}, host))
    except immp.ConfigError:
        # Prefer existing hook defined within the config itself.
        pass
    host.resources[RunnerHook].load(base, path, write)
    host.loaded()
    return host


def _handle_signal(signum, loop, task):
    # Gracefully accept a signal once, then revert to the default handler.
    def handler(_signum, _frame):
        log.info("Closing on signal")
        task.cancel()
        signal.signal(signum, original)
    original = signal.getsignal(signum)
    signal.signal(signum, handler)


def main(path, write=False):
    config = _Schema.config(_load_file(path))
    for search in config["path"]:
        sys.path.append(search)
    if config["logging"]:
        logging.config.dictConfig(config["logging"])
    else:
        logging.basicConfig(level=logging.INFO)
        logging.getLogger().setLevel(logging.WARNING)
        logging.getLogger(immp.__name__).setLevel(logging.INFO)
    log.info("Creating plugs and hooks")
    host = config_to_host(config, path, write)
    loop = get_event_loop()
    task = loop.create_task(host.run())
    for signum in (signal.SIGINT, signal.SIGTERM):
        _handle_signal(signum, loop, task)
    try:
        log.info("Starting host")
        loop.run_until_complete(task)
    finally:
        loop.close()
        if write:
            host.resources[RunnerHook].write_config()


class RunnerHook(immp.ResourceHook):
    """
    Virtual hook that handles reading and writing of config from/to a file.

    Attributes:
        writeable (bool):
            ``True`` if the file will be updated on exit, or ``False`` if being used read-only.
    """

    schema = None

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        self._base_config = None
        self._path = None
        self.writeable = None

    def load(self, base, path, writeable):
        """
        Initialise the runner with a full config and the file path.

        Args:
            base (dict):
                Parsed config file content, excluding object components.
            path (str):
                Target config file location.
            writeable (bool):
                ``True`` if changes to the live config may be written back to the file.
        """
        self._base_config = base
        self._path = path
        self.writeable = writeable

    @staticmethod
    def _config_feature(section, name, obj, priority=None):
        if obj.virtual:
            return
        feature = {"path": "{}.{}".format(obj.__class__.__module__, obj.__class__.__name__),
                   "enabled": obj.state != immp.OpenState.disabled}
        if obj.schema and obj.config:
            feature["config"] = immp.Watchable.unwrap(obj.config)
        if priority:
            feature["priority"] = priority
        section[name] = feature

    @property
    def config_features(self):
        config = {"plugs": {}, "channels": {}, "groups": {}, "hooks": {}}
        for name, plug in self.host.plugs.items():
            self._config_feature(config["plugs"], name, plug)
        for name, channel in self.host.channels.items():
            if not channel.plug.virtual:
                config["channels"][name] = {"plug": channel.plug.name, "source": channel.source}
        for name, group in self.host.groups.items():
            config["groups"][name] = immp.Watchable.unwrap(group.config)
        for name, hook in self.host.hooks.items():
            self._config_feature(config["hooks"], name, hook, self.host._priority.get(name))
        return config

    @property
    def config_full(self):
        config = self._base_config.copy()
        config.update(self.config_features)
        return config

    def write_config(self):
        """
        Write the live config out to the target config file, if writing is enabled.
        """
        if not self.writeable:
            raise immp.PlugError("Writing not enabled")
        log.info("Writing config file: %r", self._path)
        _save_file(self.config_full, self._path)

    def on_config_change(self, source):
        if self.writeable:
            self.write_config()
