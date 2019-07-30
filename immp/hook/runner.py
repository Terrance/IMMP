"""
Utility methods related to loading and saving config files, as used by the console entry point to
save the config on exit.  Creating and running :class:`.Host` via ``python -m immp`` will attach a
:class:`RunnerHook` to its resources.

Commands:
    run-write:
        Force a write of the live config out to the configured file.
"""

import logging

import anyconfig

import immp
from immp.hook.command import CommandRole, CommandScope, command


log = logging.getLogger(__name__)


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
        self._config = None
        self._path = None
        self.writeable = None

    def load(self, config, path, writeable):
        """
        Initialise the runner with a full config and the file path.

        Args:
            config (dict):
                Complete parsed config file content.
            path (str):
                Target config file location.
            writeable (bool):
                ``True`` if changes to the live config may be written back to the file.
        """
        self._config = config
        self._path = path
        self.writeable = writeable

    def _test(self, channel, user):
        return self.writeable

    @command("run-write", scope=CommandScope.private, role=CommandRole.admin, test=_test)
    async def write(self, msg):
        """
        Force a write of the live config out to the configured file.
        """
        self.write_config()
        await msg.channel.send(immp.Message(text="\N{WHITE HEAVY CHECK MARK} Written"))

    @staticmethod
    def _config_feature(section, name, obj, priority=None):
        if obj.virtual:
            return
        feature = {"path": "{}.{}".format(obj.__class__.__module__, obj.__class__.__name__)}
        if obj.schema and obj.config:
            feature["config"] = obj.config
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
            config["groups"][name] = group.config
        priorities = {hook: priority for priority, hook in self.host._priority.items()}
        for name, hook in self.host.hooks.items():
            self._config_feature(config["hooks"], name, hook, priorities.get(hook))
        return config

    @property
    def config_full(self):
        config = self._config.copy()
        config.update(self.config_features)
        return config

    def write_config(self):
        """
        Write the live config out to the target config file, if writing is enabled.
        """
        if not self.writeable:
            raise immp.PlugError("Writing not enabled")
        log.info("Writing config file")
        anyconfig.dump(self.config_full, self._path)
