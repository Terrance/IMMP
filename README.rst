IMirror
=======

A modular message processing platform.

Requirements
------------

This project uses the latest and greatest Python features (that is, native asyncio syntax and
asynchronous generators), and therefore requires at least **Python 3.6**.

The following modules are also required:

- `aiohttp <https://aiohttp.readthedocs.io>`_
- `aiostream <https://pythonhosted.org/aiostream/>`_
- `anyconfig <https://python-anyconfig.readthedocs.io>`_ (for running from command-line)
- `hangups <https://hangups.readthedocs.io>`_ (for Hangouts transport)
- `voluptuous <https://alecthomas.github.io/voluptuous/docs/_build/html/>`_

Terminology
-----------

Transport
    A class that handles all communication with an external network.
Channel
    A single room in an external network, containing messages and users.
Receiver
    A class that processes a stream of messages, in whichever way it sees fit.

Basic usage
-----------

Prepare a config file in a format of your choosing, e.g. in YAML:

.. code:: yaml

    transports:
      demo-team:
        path: imirror.transport.slack.SlackTransport
        config:
          token: xoxb-...

    channels:
      foo:
        transport: demo-team
        source: C0...
      bar:
        transport: demo-team
        source: G0...

    receivers:
      demo-sync:
        path: imirror.receiver.sync.SyncReceiver
        config:
          channels: [foo, bar]

All labels under the top-level names are effectively free text, and are used to reference from
other sections.

Then run IMirror via Python as a module::

    $ python -m imirror config.yaml
