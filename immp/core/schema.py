from collections import defaultdict
from reprlib import recursive_repr


class Any:
    """
    Allow values to match from a choice of schemas.  :class:`Invalid` will be raised if none of
    them are valid for a given value.

    If no schemas are defined, this acts as a wildcard, i.e. it will match *any* value given.

    When used as a dictionary key, exactly one choice must be present.  To allow any number of keys,
    use :class:`.Optional` instead.

    Attributes:
        choices (.Schema list):
            Set of valid schemas, or an empty set to match any possible value.
    """

    __slots__ = ("choices",)

    def __init__(self, *choices):
        self.choices = list(choices)

    def __eq__(self, other):
        return (self.choices == other.choices if isinstance(other, self.__class__)
                else tuple(self.choices) == other)

    def __hash__(self):
        return hash(tuple(self.choices))

    @recursive_repr()
    def __repr__(self):
        return "<{}{}>".format(self.__class__.__name__,
                               ": {}".format(", ".join(repr(choice) for choice in self.choices))
                               if self.choices else "")


class Nullable:
    """
    Allow values tested against the inner schema to hold a null value.  Without this wrapper,
    ``None`` will not be accepted by a schema and will raise :class:`Invalid`, whereas
    ``Nullable(str)`` matches both ``"foo"`` and ``None``.

    This object evaluates equally to its inner schema.

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

    def __eq__(self, other):
        return self.schema == (other.schema if isinstance(other, self.__class__) else other)

    def __hash__(self):
        return hash(self.schema)

    @recursive_repr()
    def __repr__(self):
        return "<{}: {!r}>".format(self.__class__.__name__, self.schema)


class Optional:
    """
    Allows keys matching the inner schema to not be present in the source :class:`dict`.  Without
    this wrapper, :class:`Invalid` will be raised if the corresponding key is missing.

    This object evaluates equally to its inner schema.

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

    __slots__ = ("schema", "default")

    @classmethod
    def unwrap(cls, key):
        """
        Unpack a :class:`dict` key, whether optional or not.

        Args:
            key:
                Plain or :class:`.Optional` key.

        Returns:
            Tuple of ``(schema, default)``, where ``default`` is :attr:`.Optional.MISSING` if
            :data:`key` is not an :class:`.Optional` instance.
        """
        return (key.schema, key.default) if isinstance(key, cls) else (key, cls.MISSING)

    def __init__(self, schema, default=None):
        self.schema = schema
        self.default = default

    def __eq__(self, other):
        return self.schema == (other.schema if isinstance(other, self.__class__) else other)

    def __hash__(self):
        return hash(self.schema)

    @recursive_repr()
    def __repr__(self):
        return "<{}: {!r} -> {!r}>".format(self.__class__.__name__, self.schema, self.default)


class SchemaError(Exception):
    """
    Error with the definitiion of the schema itself, raised during validation.
    """


class Invalid(Exception):
    """
    Error with input data not matching the corresponding schema.
    """


class Walker:
    """
    Base class for stepping through all nodes of a :class:`.Schema` instance.
    """

    STATIC = (int, float, bool, str)

    RECURSE = object()

    @classmethod
    def _at_path(cls, text, path):
        return "{}{}".format(text, " (path: {})".format(path) if path else "")

    @classmethod
    def _has(cls, item, objs):
        return any(item is obj for obj in objs)

    @classmethod
    def recurse(cls, obj, path, seen, *args):
        """
        Handle recursion of the schema.  By default, raises :class:`RecursionError` when a node is
        discovered again during a single call.

        If :attr:`.Walker.RECURSE` is returned, the node will be processed again.  Otherwise, the
        resulting value (including ``None``) will be used for the inner repeat of this node without
        any further processing.

        Arguments:
            obj (.Schema):
                Schema node.
            path (str):
                Path taken through the schema from the root.
            seen (.Schema list):
                Nodes already visited in the schema.

        Raises:
            RecursionError:
                To block looping through the same portion of the schema.
        """
        raise RecursionError(cls._at_path(repr(obj), path))

    @classmethod
    def static(cls, obj, path, seen, *args):
        """
        Handle a static object or type, as defined by :attr:`.Walker.STATIC`.

        Arguments:
            obj (int, float, bool, str):
                Schema node.
            path (str):
                Path taken through the schema from the root.
            seen (.Schema list):
                Nodes already visited in the schema.
        """
        return obj

    @classmethod
    def nullable(cls, obj, path, seen, *args):
        """
        Handle a nullable wrapper.

        Arguments:
            obj (.Nullable):
                Schema node.
            path (str):
                Path taken through the schema from the root.
            seen (.Schema list):
                Nodes already visited in the schema.
        """
        return cls.dispatch(Nullable.unwrap(obj)[0], path, seen, *args)

    @classmethod
    def any(cls, obj, path, seen, *args):
        """
        Handle a multiple-choice wrapper.

        Arguments:
            obj (.Nullable):
                Schema node.
            path (str):
                Path taken through the schema from the root.
            seen (.Schema list):
                Nodes already visited in the schema.
        """
        static = tuple(choice for choice in obj.choices if choice in cls.STATIC)
        for choice in obj.choices:
            if choice is None or isinstance(choice, Nullable):
                raise SchemaError(cls._at_path("Use outer Nullable() instead of None", path))
            elif isinstance(choice, Optional):
                raise SchemaError(cls._at_path("Use Optional() outside of Any()", path))
            elif isinstance(choice, static):
                if isinstance(choice, bool) and bool not in static:
                    continue
                raise SchemaError(cls._at_path("Useless static value duplicated by type", path))
        return Any(*(cls.dispatch(item, "{}:any({})".format(path, pos), seen, *args)
                     for pos, item in enumerate(obj.choices)))

    @classmethod
    def list(cls, obj, path, seen, *args):
        """
        Handle a list.  Multiple list members are considered equivalent to an :class:`.Any`.

        Arguments:
            obj (list):
                Schema node.
            path (str):
                Path taken through the schema from the root.
            seen (.Schema list):
                Nodes already visited in the schema.
        """
        if obj is list or not obj:
            return obj
        elif len(obj) == 1:
            return [cls.dispatch(obj[0], "{}[0]".format(path), seen, *args)]
        else:
            # [int, str] == [Any(int, str)]
            return cls.any(Any(*obj), path, seen, *args).choices

    @classmethod
    def dict(cls, obj, path, seen, *args):
        """
        Handle a dictionary.

        Arguments:
            obj (dict):
                Schema node.
            path (str):
                Path taken through the schema from the root.
            seen (.Schema list):
                Nodes already visited in the schema.
        """
        if obj is dict or not obj:
            return dict
        for key in obj:
            if isinstance(key, Optional):
                item, default = Optional.unwrap(key)
                if isinstance(default, (list, dict)):
                    raise SchemaError(cls._at_path("Use constructor instead of {} instance"
                                                   .format(type(obj).__name__), path))
            else:
                item = key
            choices = item.choices if isinstance(item, Any) else [item]
            for choice in choices:
                if not isinstance(choice, str) and choice is not str:
                    raise SchemaError(cls._at_path("Dictionary keys must be str, not {}"
                                                   .format(type(choice).__name__), path))
        return {key: cls.dispatch(value, "{}.{}".format(path, key), seen, *args)
                for key, value in obj.items()}

    @classmethod
    def dispatch(cls, obj, path, seen, *args):
        """
        Defer to other helper methods based on the input type.

        Arguments:
            obj (.Schema):
                Schema node.
            path (str):
                Path taken through the schema from the root.
            seen (.Schema list):
                Nodes already visited in the schema.
        """
        if cls._has(obj, seen):
            recursed = cls.recurse(obj, path, seen, *args)
            if recursed is not cls.RECURSE:
                return recursed
        elif not isinstance(obj, cls.STATIC + (type,)) and obj is not None:
            seen = seen + [obj]
        if cls._has(obj, cls.STATIC) or isinstance(obj, cls.STATIC):
            return cls.static(obj, path, seen, *args)
        elif isinstance(obj, Nullable):
            return cls.nullable(obj, path, seen, *args)
        elif isinstance(obj, Any):
            return cls.any(obj, path, seen, *args)
        elif obj is list or isinstance(obj, list):
            return cls.list(obj, path, seen, *args)
        elif obj is dict or isinstance(obj, dict):
            return cls.dict(obj, path, seen, *args)
        elif isinstance(obj, Schema):
            return cls.dispatch(Schema.unwrap(obj), path, seen, *args)
        else:
            raise SchemaError(cls._at_path("Unknown type {}".format(type(obj).__name__), path))

    @classmethod
    def walk(cls, obj, *args):
        """
        Main entrypoint to the walk.

        Arguments:
            obj (.Schema):
                Schema node.

        Raises:
            SchemaError:
                When the schema is misconfigured.
        """
        return cls.dispatch(obj, "", [], *args)


class Validator(Walker):
    """
    Validation of schemas against input data.
    """

    @classmethod
    def _short(cls, obj):
        if obj is None:
            return "None"
        elif isinstance(obj, type):
            return obj.__name__
        else:
            return "{} {!r}".format(type(obj).__name__, obj)

    @classmethod
    def recurse(cls, obj, path, seen, data):
        return cls.RECURSE

    @classmethod
    def static(cls, obj, path, seen, data):
        # Don't allow the usual subclassing of ints as bools.
        if obj is int and isinstance(data, int) and not isinstance(data, bool):
            return data
        elif obj in (float, bool, str) and isinstance(data, obj):
            return data
        elif (obj == data and isinstance(obj, int) and
              not isinstance(obj, bool) and not isinstance(data, bool)):
            return data
        elif (obj == data and isinstance(obj, (float, bool, str)) and
              isinstance(data, (float, bool, str))):
            return data
        else:
            raise Invalid(cls._at_path("Expecting {} but got {}"
                                       .format(cls._short(obj), cls._short(data)), path))

    @classmethod
    def nullable(cls, obj, path, seen, data):
        if data is None:
            return data
        else:
            return super().nullable(obj, path, seen, data)

    @classmethod
    def any(cls, obj, path, seen, data):
        if not obj.choices:
            return data
        excs = []
        for pos, choice in enumerate(obj.choices):
            try:
                return cls.dispatch(choice, "{}:any({})".format(path, pos), seen, data)
            except Invalid as e:
                excs.append(e)
        else:
            # No schemas matched the data.
            raise Invalid(cls._at_path("No matches for Any()", path), *excs)

    @classmethod
    def list(cls, obj, path, seen, data):
        if not isinstance(data, list):
            raise Invalid(cls._at_path("Expecting list but got {}"
                                       .format(cls._short(data)), path))
        elif obj is list:
            return data
        elif len(obj) == 1:
            return [cls.dispatch(obj[0], "{}[{}]".format(path, pos), seen, item)
                    for pos, item in enumerate(data)]
        else:
            multi = Any(*obj)
            return [cls.any(multi, "{}[{}]".format(path, pos), seen, item)
                    for pos, item in enumerate(data)]

    @classmethod
    def dict(cls, obj, path, seen, data):
        if not isinstance(data, dict):
            raise Invalid(cls._at_path("Expecting dict but got {}"
                                       .format(cls._short(data)), path))
        elif obj is dict or not obj:
            return dict(data)
        parsed = {}
        optional = dict(Optional.unwrap(key) for key in obj if isinstance(key, Optional))
        for item in obj:
            key = Optional.unwrap(item)[0]
            if isinstance(key, Any):
                matches = [choice for choice in key.choices if choice in data]
                if len(matches) > 1:
                    raise Invalid(cls._at_path("Multiple matches for Any()", path))
                elif matches:
                    parsed[matches[0]] = cls.dispatch(obj[item], "{}.{}".format(path, matches[0]),
                                                      seen, data[matches[0]])
                elif not cls._has(key, optional):
                    raise Invalid(cls._at_path("No matches for Any()", path))
        for key in obj:
            if isinstance(key, str) and key not in data:
                if key in optional:
                    parsed[key] = optional[key]
                else:
                    raise Invalid(cls._at_path("Missing key {!r}".format(key), path))
        typed = tuple(key for key in obj if isinstance(key, type))
        fixed = {key for key in obj if not isinstance(key, type)}
        for key, value in data.items():
            here = "{}.{}".format(path, key)
            if key in fixed:
                parsed[key] = cls.dispatch(obj[key], here, seen, value)
                continue
            for match in typed:
                if isinstance(key, match):
                    parsed[key] = cls.dispatch(obj[match], here, seen, value)
                    break
            else:
                if key not in parsed:
                    # Unmatched keys are passed through without further validation.
                    parsed[key] = value
        for key in optional:
            here = "{}.{}".format(path, key)
            if key in data or isinstance(key, Any):
                continue
            # Missing but optional keys are filled in and validated.
            default = optional[key]
            if callable(default):
                default = default()
            parsed[key] = cls.dispatch(obj[key], here, seen, default)
        return parsed

    @classmethod
    def dispatch(cls, obj, path, seen, data):
        if isinstance(data, type):
            raise Invalid(cls._at_path("Expecting instance but got {} type"
                                       .format(data.__name__), path))
        else:
            return super().dispatch(obj, path, seen, data)

    @classmethod
    def walk(cls, obj, data):
        """
        Validate the given data against a schema.

        Args:
            obj (.Schema):
                Description of the data format.
            data:
                Input data to validate.

        Raises:
            Invalid:
                When a key or value doesn't match the accepted type for that field.

        Returns:
            Parsed data with optional values filled in.
        """
        return super().walk(obj, data)


class JSONSchema(Walker):
    """
    Generator of `JSON Schema <https://json-schema.org>`_ objects, suitable for external validation
    of schemas in JSON.
    """

    TYPES = {int: "number", float: "number", bool: "boolean", str: "string"}

    @classmethod
    def _make_anyof(cls, choices):
        types = []
        anys = []
        consts = defaultdict(set)
        for choice in choices:
            if not choice:
                return {}
            elif isinstance(choice, dict) and "type" in choice:
                if len(choice) == 1:
                    types.append(choice["type"])
                elif len(choice) == 2 and "const" in choice:
                    consts[choice["type"]].add(choice["const"])
                elif len(choice) == 2 and "enum" in choice:
                    consts[choice["type"]].update(choice["enum"])
                else:
                    anys.append(choice)
            else:
                anys.append(choice)
        for type_, values in consts.items():
            if type_ in types or not values:
                continue
            elif len(values) == 1:
                anys.append({"type": type_, "const": next(iter(values))})
            else:
                anys.append({"type": type_, "enum": list(values)})
        if len(types) == 1:
            anys.append({"type": types[0]})
        elif types:
            anys.append({"type": types})
        if len(anys) == 1:
            return anys[0]
        elif anys:
            return {"anyOf": anys}
        else:
            return {}

    @classmethod
    def static(cls, obj, path, seen):
        if isinstance(obj, type):
            return {"type": cls.TYPES[obj]}
        else:
            return {"type": cls.TYPES[type(obj)], "const": obj}

    @classmethod
    def nullable(cls, obj, path, seen):
        return cls._make_anyof([{"type": "null"}, super().nullable(obj, path, seen)])

    @classmethod
    def any(cls, obj, path, seen):
        return cls._make_anyof(super().any(obj, path, seen).choices)

    @classmethod
    def list(cls, obj, path, seen):
        root = {"type": "array"}
        if obj is list or not obj:
            return root
        elif len(obj) > 1:
            root["items"] = cls._make_anyof(super().list(obj, path, seen))
        else:
            root["items"] = cls.dispatch(obj[0], path, seen)
        return root

    @classmethod
    def dict(cls, obj, path, seen):
        root = {"type": "object"}
        if obj is dict or not obj:
            return root
        optional = dict(Optional.unwrap(key) for key in obj if isinstance(key, Optional))
        fixed = {key for key in obj if not isinstance(key, type)}
        if fixed:
            root["properties"] = {}
            for key in fixed:
                if isinstance(key, Any):
                    raise SchemaError(cls._at_path("Any() in dictionary keys not supported", path))
                item = Optional.unwrap(key)[0]
                here = "{}.{}".format(path, item)
                prop = cls.dispatch(obj[key], here, seen)
                root["properties"][item] = prop
                if key in optional:
                    default = optional[key]
                    prop["default"] = default() if callable(default) else default
            required = [key for key in fixed if key not in optional]
            if required:
                root["required"] = required
        typed = tuple(key for key in obj if isinstance(key, type))
        if typed:
            root["additonalItems"] = cls.any(Any(*(obj[key] for key in typed)), path, seen)
        return root

    @classmethod
    def walk(cls, obj):
        """
        Convert a schema structure into a `JSON Schema <https://json-schema.org>`_ representation.

        Args:
            schema (.Schema):
                Input schema or structure.

        Returns:
            dict:
                Equivalent JSON Schema data.
        """
        schema = super().walk(obj)
        schema["$schema"] = "http://json-schema.org/schema#"
        return schema


class Schema:
    """
    Validate JSON-like Python structures and provide defaults:

    .. code-block:: python

        config = Schema({
            "flag": bool,
            "numbers": [int],
            "nullable": Nullable(str),
            "nested": {
                Optional("defaulted", 1): int,
                "multiple": Any(int, str)
            }
        })

        validated = config(data)

    Pass a structure representing the expected data format to the constructor, along with an
    optional :data:`base` to extend from, then validate some given data against the schema by
    calling the instance -- see :class:`Validator`.

    Attributes:
        raw:
            Root schema item, including any base items.
        json (dict):
            `JSON Schema <https://json-schema.org>`_ data corresponding to this schema -- see
            :class:`JSONSchema`.
    """

    STATIC = (int, float, bool, str)
    JSON_TYPES = {int: "number", float: "number", bool: "boolean", str: "string"}

    __slots__ = ("raw",)

    @classmethod
    def unwrap(cls, schema):
        return schema.raw if isinstance(schema, cls) else schema

    def __init__(self, raw, base=None):
        raw = Schema.unwrap(raw)
        if base is not None:
            base = Schema.unwrap(base)
            if not isinstance(raw, dict) or not isinstance(base, dict):
                raise SchemaError("Input and base schemas must both be dicts")
            merged = dict(base)
            merged.update(raw)
            raw.update(merged)
        self.raw = raw

    def __call__(self, data):
        return Validator.walk(self, data)

    @property
    def json(self):
        return JSONSchema.walk(self)

    def __repr__(self):
        return "<{}: {!r}>".format(self.__class__.__name__, self.raw)
