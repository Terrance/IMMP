import importlib


def resolve_import(path):
    """
    Takes the qualified name of a Python class, and return the physical class object.

    Args:
        path (str):
            Dotted Python class name, e.g. ``<module path>.<class name>``.

    Returns:
        type:
            Class object imported from module.
    """
    module, class_ = path.rsplit(".", 1)
    return getattr(importlib.import_module(module), class_)


class Base(object):
    """
    Utility class to provide a default :meth:`__repr__` based on the contents of :attr:`__dict__`.
    """

    def __repr__(self):
        def nest_repr(obj):
            if isinstance(obj, dict):
                return "{...}" if obj else "{}"
            elif isinstance(obj, list):
                return "[...]" if obj else "[]"
            else:
                return repr(obj)
        return "{}({})".format(self.__class__.__name__,
                               ", ".join("{}={}".format(k, nest_repr(v))
                                         for k, v in self.__dict__.items()
                                         if not k.startswith("_")))
