class Any:
    """
    Allow values to match from a choice of schemas.  :class:`TypeError` will be raised if none of
    them are valid for a given value.

    If no schemas are defined, this acts as a wildcard, i.e. it will match *any* value given.

    Attributes:
        choices (.Schema list):
            Set of valid schemas, or an empty set to match any possible value.
    """

    __slots__ = ("_choices",)

    def __init__(self, *choices):
        self.choices = choices

    @property
    def choices(self):
        return list(self._choices)

    @choices.setter
    def choices(self, value):
        if None in value:
            raise SchemaError("Use Nullable() instead of None as a choice")
        self._choices = value

    def __repr__(self, seen=None):
        return "<{}{}>".format(self.__class__.__name__,
                               ": {}".format(", ".join(_render(choice, True, seen)
                                                       for choice in self.choices))
                               if self.choices else "")


class Nullable:
    """
    Allow values tested against the inner schema to hold a null value.  By default, ``None`` will
    not be accepted by a schema and will raise :class:`TypeError`, whereas ``Nullable(str)``
    matches both ``"foo"`` and ``None``.

    Attributes:
        schema (.Schema):
            Inner schema for non-null processing.
    """

    __slots__ = ("schema",)

    @classmethod
    def unwrap(cls, value):
        """
        Unpack a value, whether nullable or not.

        Args:
            value:
                Plain or :class:`.Nullable` schema value.

        Returns:
            Tuple of ``(schema, nullable)``, where :data:`nullable` is ``True`` if the source value
            was :class:`.Nullable`, ``False`` otherwise.
        """
        return (value.schema, True) if isinstance(value, cls) else (value, False)

    def __init__(self, schema):
        self.schema = schema

    def __repr__(self, seen=None):
        return "<{}: {}>".format(self.__class__.__name__, _render(self.schema, True, seen))


class Optional:
    """
    Allows keys matching the inner schema to not be present in the source :class:`dict`.  By
    default, :class:`KeyError` will be raised.

    Attributes:
        schema (.Schema):
            Inner schema for processing.
        default:
            Alternative value for when the key is missing.  This can be :data:`None` (the default),
            an instance of a static class (:class:`int`, :class:`float`, :class:`bool`), the
            :class:`list` or :class:`dict` constructor, or a lambda or function that produces the
            desired value (e.g. ``lambda: {"a": 1}``).

            When using ``None``, you should also mark the corresponding value as :class:`.Nullable`.
    """

    MISSING = object()

    __slots__ = ("schema", "_default")

    @classmethod
    def unwrap(cls, key):
        """
        Unpack a :class:`dict` key, whether optional or not.

        Args:
            key:
                Plain or :class:`.Optional` key.

        Returns:
            Tuple of ``(schema, default)``, where default is :attr:`.Optional.MISSING` if
            :data:`key` is not an :class:`.Optional` instance.
        """
        return (key.schema, key.default) if isinstance(key, cls) else (key, cls.MISSING)

    def __init__(self, schema, default=None):
        self.schema = schema
        self.default = default

    @property
    def default(self):
        return self._default() if callable(self._default) else self._default

    @default.setter
    def default(self, value):
        if isinstance(value, (list, dict)):
            raise SchemaError("Can't reuse {} object, pass class directly or a lambda to customise"
                              .format(type(value).__name__))
        self._default = value

    def __repr__(self, seen=None):
        return "<{}: {} -> {}>".format(self.__class__.__name__,
                                       _render(self.schema, True, seen),
                                       _render(self.default, True, seen))


class SchemaError(Exception):
    """
    Error with the definitiion of the schema itself, raised during validation.
    """


def _render(item, full=False, seen=None):
    if item is None:
        return "None"
    elif isinstance(item, type):
        return item.__name__
    elif isinstance(item, (list, dict)):
        if not full:
            return item.__class__.__name__
        if not seen:
            seen = []
        elif item in seen:
            return "(recursion: {})".format(_render(item))
        seen = seen + [item]
        if isinstance(item, dict):
            return "{{{}}}".format(", ".join("{}: {}".format(_render(key, full, seen),
                                                             _render(value, full, seen))
                                             for key, value in item.items()))
        else:
            return "[{}]".format(", ".join(_render(value, full, seen) for value in item))
    elif isinstance(item, (Any, Nullable, Optional)):
        return item.__repr__(seen)
    else:
        return "{} {!r}".format(type(item).__name__, item)


def _at_path(text, path):
    return "{}{}".format(text, " (path: {})".format(path) if path else "")


class Schema:
    """
    Validate JSON-like Python structures and provide defaults:

    .. code-block:: python

        config = Schema({
            "flag": bool,
            "numbers": [int],
            "nullable": Nullable(str),
            "nested": {
                Optional("maybe"): int,
                "multiple": Any(int, str)
            }
        })

        validated = config(data)

    Pass a structure representing the expected data format to the constructor, along with an
    optional :data:`base` to extend from, then validate some given data against the schema by
    calling the instance -- see :meth:`validate`.

    Attributes:
        json (dict):
            `JSON Schema <https://json-schema.org>`_ data corresponding to this schema -- see
            :meth:`to_json`.
    """

    STATIC = (int, float, bool, str)
    JSON_TYPES = {int: "number", float: "number", bool: "boolean", str: "string"}

    __slots__ = ("_raw",)

    @classmethod
    def _validate_static(cls, schema, data, path):
        # Don't allow the usual subclassing of ints as bools.
        if schema is int and isinstance(data, int) and not isinstance(data, bool):
            return data
        elif schema in (float, bool, str) and isinstance(data, schema):
            return data
        elif isinstance(schema, int) and schema == data and not isinstance(data, bool):
            return data
        elif isinstance(schema, (float, bool, str)) and schema == data:
            return data
        else:
            raise TypeError(_at_path("Expecting {} but got {}"
                                     .format(_render(schema), _render(data)), path))

    @classmethod
    def _validate_nullable(cls, schema, data, path):
        item = Nullable.unwrap(schema)[0]
        return None if data is None else cls.validate(item, data, path)

    @classmethod
    def _validate_any(cls, schema, data, path):
        if not schema.choices:
            return data
        excs = []
        for choice in schema.choices:
            if isinstance(choice, Nullable):
                raise SchemaError("Top-level Nullable() makes entire Any() nullable")
            try:
                return cls.validate(choice, data, path)
            except (KeyError, ValueError, TypeError) as e:
                excs.append(e)
        else:
            # No schemas matched the data.
            raise TypeError(_at_path("No matches for Any()", path), excs)

    @classmethod
    def _validate_list(cls, schema, data, path):
        if not isinstance(data, list):
            raise TypeError(_at_path("Expecting list but got {}"
                                     .format(_render(data)), path))
        if schema is list or not schema:
            return list(data)
        # [str, int] == [Any(str, int)]
        multi = Any(*schema) if len(schema) > 1 else schema[0]
        return [cls.validate(multi, item, "{}[{}]".format(path, i))
                for i, item in enumerate(data)]

    @classmethod
    def _validate_dict(cls, schema, data, path):
        if not isinstance(data, dict):
            raise ValueError(_at_path("Expecting dict but got {}"
                                      .format(_render(data)), path))
        if schema is dict or not schema:
            return dict(data)
        optional = dict(Optional.unwrap(key) for key in schema if isinstance(key, Optional))
        unwrapped = {Optional.unwrap(key)[0]: value for key, value in schema.items()}
        typed = tuple(key for key in unwrapped if isinstance(key, type))
        fixed = {key for key in unwrapped if key not in typed}
        parsed = {}
        for key in unwrapped:
            if isinstance(key, cls.STATIC) and key not in data:
                if key in optional:
                    parsed[key] = optional[key]
                else:
                    raise KeyError(_at_path("Missing key {!r}".format(key), path))
        for key, value in data.items():
            here = "{}.{}".format(path, key)
            if key in fixed:
                parsed[key] = cls.validate(unwrapped[key], value, here)
                continue
            for match in typed:
                if isinstance(key, match):
                    parsed[key] = cls.validate(unwrapped[match], value, here)
                    break
            else:
                # Unmatched keys are passed through without further validation.
                parsed[key] = value
        for key in optional:
            if key not in data:
                # Missing but optional keys are filled in and validated.
                parsed[key] = cls.validate(unwrapped[key], optional[key], "{}.{}".format(path, key))
        return parsed

    @classmethod
    def validate(cls, schema, data, path=""):
        """
        Validate the given data against a schema.

        Args:
            schema (.Schema):
                Description of the data format.
            data:
                Input data to validate.
            path (str):
                Route through the data structure, shown in error messages to trace violations.

        Raises:
            SchemaError:
                When the schema is misconfigured.
            KeyError:
                When a required dict value is missing, unless marked with :class:`.Optional`.
            TypeError:
                When a key or value doesn't match the accepted type for that field, unless the
                value is ``None`` and the key is marked with :class:`.Nullable`.

        Returns:
            Parsed data with optional values filled in.
        """
        if isinstance(data, type):
            raise TypeError(_at_path("Expecting instance but got {} type"
                                     .format(_render(data)), path))
        if isinstance(schema, Schema):
            schema = schema._raw
        if schema in cls.STATIC or isinstance(schema, cls.STATIC):
            return cls._validate_static(schema, data, path)
        elif isinstance(schema, Nullable):
            return cls._validate_nullable(schema, data, path)
        elif isinstance(schema, Any):
            return cls._validate_any(schema, data, path)
        elif schema is list or isinstance(schema, list):
            return cls._validate_list(schema, data, path)
        elif schema is dict or isinstance(schema, dict):
            return cls._validate_dict(schema, data, path)
        else:
            raise SchemaError(_at_path("Unknown schema type {}".format(_render(schema)), path))

    @classmethod
    def _make_anyof(cls, choices):
        types = []
        anys = []
        for choice in choices:
            if isinstance(choice, dict) and len(choice) == 1 and "type" in choice:
                types.append(choice["type"])
            else:
                anys.append(choice)
        if len(types) == 1:
            types = types[0]
        if types and anys:
            return {"anyOf": anys + [{"type": types}]}
        elif anys:
            return {"anyOf": anys}
        elif types:
            return {"type": types}
        else:
            return {}

    @classmethod
    def to_json(cls, schema, top=True):
        """
        Convert a :class:`.Schema` into a `JSON Schema <https://json-schema.org>`_ representation.

        Args:
            schema (.Schema):
                Input schema instance.

        Returns:
            dict:
                Equivalent JSON Schema data.
        """
        if isinstance(schema, Schema):
            schema = schema._raw
        if schema in Schema.STATIC:
            root = {"type": cls.JSON_TYPES[schema]}
        elif isinstance(schema, Schema.STATIC):
            root = {"type": cls.JSON_TYPES[type(schema)], "const": schema}
        elif isinstance(schema, Nullable):
            root = cls._make_anyof([{"type": "null"}, cls.to_json(schema.schema, False)])
        elif isinstance(schema, Any):
            root = cls._make_anyof(cls.to_json(choice, False) for choice in schema.choices)
        elif schema is list or isinstance(schema, list):
            root = {"type": "array"}
            if isinstance(schema, list):
                if len(schema) > 1:
                    root["items"] = cls._make_anyof([cls.to_json(item, False) for item in schema])
                elif schema:
                    root["items"] = cls.to_json(schema[0], False)
        elif schema is dict or isinstance(schema, dict):
            root = {"type": "object"}
            if isinstance(schema, dict) and schema:
                optional = dict(Optional.unwrap(key) for key in schema if isinstance(key, Optional))
                unwrapped = {Optional.unwrap(key)[0]: value for key, value in schema.items()}
                typed = tuple(key for key in unwrapped if isinstance(key, type))
                fixed = {key for key in unwrapped if key not in typed}
                if fixed:
                    root["properties"] = {}
                    for key in fixed:
                        prop = cls.to_json(unwrapped[key], False)
                        root["properties"][key] = prop
                        if key in optional:
                            prop["default"] = optional[key]
                    required = [key for key in fixed if key not in optional]
                    if required:
                        root["required"] = required
                for key in typed:
                    if key is not str:
                        raise SchemaError("Object keys must be str in JSON")
                    root["additonalItems"] = cls.to_json(unwrapped[key], False)
        else:
            raise SchemaError("Unknown schema type {}".format(_render(schema)))
        if top:
            root["$schema"] = "http://json-schema.org/schema#"
        return root

    def __init__(self, raw, base=None):
        if isinstance(base, Schema):
            merged = dict(base._raw)
            merged.update(raw)
        elif isinstance(base, dict):
            merged = dict(base)
            merged.update(raw)
        elif base:
            raise SchemaError("Base schema must be a dict")
        else:
            merged = raw
        self._raw = merged

    def __call__(self, data):
        return self.validate(self._raw, data)

    @property
    def json(self):
        return self.__class__.to_json(self)

    def __repr__(self):
        return "<{}: {}>".format(self.__class__.__name__, _render(self._raw, True))
