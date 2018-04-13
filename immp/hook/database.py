import logging

from peewee import Model, Proxy
from playhouse.db_url import connect
from voluptuous import ALLOW_EXTRA, Schema

import immp


log = logging.getLogger(__name__)


class _Schema(object):

    config = Schema({"url": str}, extra=ALLOW_EXTRA, required=True)


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

    Config:
        url (str):
            Database connection url, passed to :func:`playhouse.db_url.connect`.

    Attributes:
        db (peewee.Database):
            Connected database instance.
    """

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        config = _Schema.config(config)

    async def start(self):
        log.debug("Opening connection to database")
        self.db = connect(self.config["url"])
        BaseModel._meta.database.initialize(self.db)
