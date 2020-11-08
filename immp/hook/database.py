"""
Provider of database access to other hooks.  All first-party hooks with database support will
require the asynchronous database provider to be present.

Synchronous
~~~~~~~~~~~

Config:
    url (str):
        Database connection string, as defined by Peewee's :ref:`db_url`.

This hook provides persistent storage via a database.  Any database types supported by Peewee can
be used, though the usual caveats apply: if a hook requires fields specific to a single database
type, the app is effectively locked-in to that type.

Hooks should subclass :class:`.BaseModel` for their data structures.  At startup, they can register
their models to the database connection by calling :meth:`.DatabaseHook.add_models` (obtained from
``host.resources[DatabaseHook]``), which will create any needed tables on first runs.

.. note::
    This hook requires the `Peewee <http://docs.peewee-orm.com>`_ Python module, along with any
    database-specific libraries (e.g. Psycopg2 for PostgreSQL).

.. warning::
    Database requests will block all other running tasks; notably, all plugs will be unable to make
    any progress whilst long-running queries are executing.

Asynchronous
~~~~~~~~~~~~

Config:
    url (str):
        Tortoise `database connection string
        <https://tortoise-orm.readthedocs.io/en/latest/databases.html>`_.

This hook provides persistent storage via a database.  Any database types supported by Tortoise can
be used.  At startup, they can register their models to the database connection by calling
:meth:`.AsyncDatabaseHook.add_models` (obtained from ``host.resources[AsyncDatabaseHook]``), which
will create any needed tables on first runs.

.. note::
    This hook requires the `Tortoise <https://tortoise-orm.readthedocs.io>`_ Python module, along
    with any database-specific libraries (e.g. asyncpg for PostgreSQL).
"""

import logging

try:
    from peewee import DatabaseProxy, Model
    from playhouse.db_url import connect
except ImportError:
    DatabaseProxy = Model = connect = None

try:
    from tortoise import Tortoise
except ImportError:
    Tortoise = None

import immp


log = logging.getLogger(__name__)


class _ModelsMixin:

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.models = set()

    def add_models(self, *models):
        self.models.update(models)


if Model:

    class BaseModel(Model):
        """
        Template model to be used by other hooks.
        """

        class Meta:
            database = DatabaseProxy()


class DatabaseHook(immp.ResourceHook, _ModelsMixin):
    """
    Hook that provides generic database access to other hooks, backed by :mod:`peewee`.  Because
    models are in the global scope, they can only be attached to a single database, therefore this
    hook acts as the single source of truth for obtaining a "global" database.
    """

    schema = immp.Schema({"url": str})

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        if not Model:
            raise immp.PlugError("'peewee' module not installed")
        self.db = None

    async def start(self):
        log.debug("Opening connection to database")
        self.db = connect(self.config["url"])
        BaseModel._meta.database.initialize(self.db)
        if self.models:
            names = sorted(cls.__name__ for cls in self.models)
            log.debug("Registering models: %s", ", ".join(names))
            self.db.create_tables(self.models, safe=True)

    async def stop(self):
        if self.db:
            log.debug("Closing connection to database")
            self.db.close()
            self.db = None


class AsyncDatabaseHook(immp.ResourceHook, _ModelsMixin):
    """
    Hook that provides generic database access to other hooks, backed by :mod:`tortoise`.  Because
    models are in the global scope, they can only be attached to a single database, therefore this
    hook acts as the single source of truth for obtaining a "global" database.
    """

    schema = immp.Schema({"url": str})

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        if not Tortoise:
            raise immp.PlugError("'tortoise' module not installed")

    async def start(self):
        log.debug("Opening connection to database")
        modules = sorted(set(model.__module__ for model in self.models))
        log.debug("Registering model modules: %s", ", ".join(modules))
        await Tortoise.init(db_url=self.config["url"], modules={"db": modules})
        await Tortoise.generate_schemas(safe=True)

    async def stop(self):
        log.debug("Closing connection to database")
        await Tortoise.close_connections()
