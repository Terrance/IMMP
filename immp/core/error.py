class ConfigError(Exception):
    """
    Error for invalid configuration in a given context.
    """


class PlugError(Exception):
    """
    Error for plug-specific problems.
    """


class HookError(Exception):
    """
    Error for hook-specific problems.
    """
