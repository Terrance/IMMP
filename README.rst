IMMP
====

A modular processing platform for instant messages.

Requirements
------------

This project uses the latest and greatest Python features (that is, native asyncio syntax and
asynchronous generators), and therefore requires at least **Python 3.6**.

Additional modules are required for most plugs and hooks -- consult the docs for each module you
want to use to check its own requirements, or use the included requirements list to install all
possible dependencies for built-in modules.

Terminology
-----------

Network
    An external service that provides message-based communication.
Message
    A unit of data, which can include text, images, attachments, authorship, and so on.
User
    An individual or service which can author messages on a network.
Plug
    A handler for all communication with an external network, transforming the network’s content
    to message objects and back again.
Channel
    A single room in an external network – a source of messages, and often a container of users.
Group
    A collection of plugs and channels.
Hook
    A worker that processes a stream of incoming messages, in whichever way it sees fit.

Basic usage
-----------

Prepare a config file in a format of your choosing, e.g. in YAML:

.. code:: yaml

    plugs:
      demo:
        path: demo.DemoPlug
        config:
          api-key: xyzzy

    channels:
      foo:
        plug: demo
        source: 12345
      bar:
        plug: demo
        source: 98765

    hooks:
      test:
        path: test.TestHook
        config:
          channels: [foo, bar]
          args: [123, 456]

All labels under the top-level names are effectively free text, and are used to reference from
other sections.

Then start the built-in runner::

    $ immp config.yaml
