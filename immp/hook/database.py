"""
Provider of database access to other hooks.

Config:
    url (str):
        Database connection URL, passed to :func:`playhouse.db_url.connect`.

This hook provides persistent storage via a database.  Any database types supported by Peewee can
be used, though the usual caveats apply: if a hook requires fields specific to a single database
type, the app is effectively locked-in to that type.

Hooks should subclass :class:`.BaseModel` for their data structures.  At startup, they can obtain
the database connection via :attr:`host.resources[DatabaseHook].db`, and use it to create their
database tables via :meth:`peewee.Database.create_tables`.

.. note::
    This hook requires the `Peewee <http://docs.peewee-orm.com>`_ Python module, along with any
    database-specific libraries (e.g. Psycopg2 for PostgreSQL).
"""

import logging

from peewee import Model, Proxy
from playhouse.db_url import connect

import immp


log = logging.getLogger(__name__)


class BaseModel(Model):
    """
    Template model to be used by other hooks.
    """

    class Meta:
        database = Proxy()


class DatabaseHook(immp.ResourceHook):
    """
    Hook that provides generic database access to other hooks, backed by :mod:`peewee`.  Because
    models are in the global scope, they can only be attached to a single database, therefore this
    hook acts as the single source of truth for obtaining a "global" database.

    Attributes:
        db (peewee.Database):
            Connected database instance.
    """

    schema = immp.Schema({"url": str})

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        self.db = None

    async def start(self):
        log.debug("Opening connection to database")
        self.db = connect(self.config["url"])
        BaseModel._meta.database.initialize(self.db)

    async def stop(self):
        if self.db:
            log.debug("Closing connection to database")
            self.db.close()
            self.db = None
