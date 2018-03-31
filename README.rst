IMMP
====

A modular processing platform for instant messages.

Requirements
------------

This project uses the latest and greatest Python features (that is, native asyncio syntax and
asynchronous generators), and therefore requires at least **Python 3.6**.

The following extra modules are required:

- `aiohttp <https://aiohttp.readthedocs.io>`_
- `voluptuous <https://alecthomas.github.io/voluptuous/docs/_build/html/>`_

Further modules may also be needed for certain features:

- `aioconsole <https://aioconsole.readthedocs.io>`_ (for async shell)
- `anyconfig <https://python-anyconfig.readthedocs.io>`_ (for running from command-line)
- `discord.py <https://discordpy.readthedocs.io/en/rewrite/>`_ **1.0+** (for Discord)
- `hangups <https://hangups.readthedocs.io>`_ (for Hangouts)
- `ptpython <https://github.com/jonathanslenders/ptpython>`_ (for blocking shell)

Terminology
-----------

Plug
    A class that handles all communication with an external network.
Channel
    A single room in an external network, containing messages and users.
Hook
    A class that processes a stream of messages, in whichever way it sees fit.

Basic usage
-----------

Prepare a config file in a format of your choosing, e.g. in YAML:

.. code:: yaml

    plugs:
      demo-team:
        path: immp.plug.slack.SlackPlug
        config:
          token: xoxb-...

    channels:
      foo:
        transport: demo-team
        source: C0...
      bar:
        transport: demo-team
        source: G0...

    hooks:
      demo-sync:
        path: immp.hook.sync.SyncHook
        config:
          channels: [foo, bar]

All labels under the top-level names are effectively free text, and are used to reference from
other sections.

Then run IMMP via Python as a module::

    $ python -m immp config.yaml
