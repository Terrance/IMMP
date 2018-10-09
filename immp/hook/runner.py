import logging

import anyconfig

import immp
from immp.hook.command import CommandRole, command


log = logging.getLogger(__name__)


class RunnerHook(immp.ResourceHook):

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        self._config = None
        self._path = None
        self._write = None

    def load(self, config, path, write):
        self._config = config
        self._path = path
        self._write = write

    @command("run-write", role=CommandRole.admin, test=lambda self: self._write)
    async def write(self, msg):
        """
        Write the live config out to the configured file.
        """
        self.write_config()
        await msg.channel.send(immp.Message(text="\N{WHITE HEAVY CHECK MARK} Written"))

    @staticmethod
    def _config_feature(section, name, obj):
        if obj.virtual:
            return
        feature = {"path": "{}.{}".format(obj.__class__.__module__, obj.__class__.__name__)}
        if obj.config:
            feature["config"] = obj.config
        section[name] = feature

    @property
    def config_features(self):
        config = {"plugs": {}, "channels": {}, "hooks": {}}
        for name, plug in self.host.plugs.items():
            self._config_feature(config["plugs"], name, plug)
        for name, channel in self.host.channels.items():
            if not channel.plug.virtual:
                config["channels"][name] = {"plug": channel.plug.name, "source": channel.source}
        for hook in self.host.resources.values():
            self._config_feature(config["hooks"], hook.name, hook)
        for name, hook in self.host.hooks.items():
            self._config_feature(config["hooks"], name, hook)
        return config

    @property
    def config_full(self):
        config = self._config.copy()
        config.update(self.config_features)
        return config

    def write_config(self):
        if not self._write:
            raise immp.PlugError("Writing not enabled")
        log.info("Writing config file")
        anyconfig.dump(self.config_full, self._path)
