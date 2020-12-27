"""
Fallback mention and word highlight support for plugs via private channels.

Mentions
~~~~~~~~

Config:
    plugs (str list):
        List of plug names to enable mention alerts for.
    usernames (bool):
        Whether to match network usernames (``True`` by default).
    real-names (bool):
        Whether to match user's display names (``False`` by default).
    ambiguous (bool):
        Whether to notify multiple potential users of an ambiguous mention (``False`` by default).

For networks that don't provide native user mentions, this plug can send users a private message
when mentioned by their username or real name.

A mention is matched from each ``@`` sign until whitespace is encountered.  For real names, spaces
and special characters are ignored, so that e.g. ``@fredbloggs`` will match *Fred Bloggs*.

Partial mentions are supported, failing any exact matches, by basic prefix search on real names.
For example, ``@fred`` will match *Frederick*, and ``@fredb`` will match *Fred Bloggs*.

Subscriptions
~~~~~~~~~~~~~

Dependencies:
    :class:`.AsyncDatabaseHook`

Config:
    plugs (str list):
        List of plug names to enable subscription alerts for.

Commands:
    sub-add <text>:
        Add a subscription to your trigger list.
    sub-remove <text>:
        Remove a subscription from your trigger list.
    sub-exclude <text>:
        Don't trigger a specific subscription in the current public channel.
    sub-list:
        Show all active subscriptions.

Allows users to opt in to private message notifications when chosen highlight words are used in a
group conversation.
"""

from .mentions import MentionsHook
from .subscriptions import SubscriptionsHook
