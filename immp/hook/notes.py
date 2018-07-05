"""
Recallable per-channel lists of text items.

Commands:
    note-add <text>:
        Add a new note for this channel.
    note-remove <#>:
        Delete an existing note from this channel by its position.
    note-show <#>:
        Recall a single note in this channel.
    note-list:
        Recall all notes for this channel.

.. note::
    This hook requires an active :class:`.DatabaseHook` to store data.
"""

import time

from peewee import CharField, IntegerField

import immp
from immp.hook.command import Command, Commandable, CommandScope
from immp.hook.database import BaseModel, DatabaseHook


CROSS = "\N{CROSS MARK}"
TICK = "\N{WHITE HEAVY CHECK MARK}"


class Note(BaseModel):
    """
    Representation of a single note.

    Attributes:
        timestamp (int):
            Creation time of the note.
        network (str):
            Network identifier for the channel's plug.
        channel (str):
            Channel identifier where the note was created.
        user (str):
            User identifier of the note's author.
        text (str):
            Note content.
    """

    timestamp = IntegerField(default=lambda: int(time.time()))
    network = CharField()
    channel = CharField(null=True)
    user = CharField(null=True)
    text = CharField()

    @classmethod
    def select_channel(cls, channel):
        return (cls.select().where(cls.network == channel.plug.network_id,
                                   cls.channel == channel.source)
                            .order_by(cls.timestamp))

    @classmethod
    def select_position(cls, channel, pos):
        try:
            # ModelSelect.get() ignores the offset clause, use an index instead.
            return cls.select_channel(channel).limit(1).offset(pos - 1)[0]
        except IndexError:
            raise Note.DoesNotExist from None

    @property
    def ago(self):
        diff = int(time.time()) - self.timestamp
        for step, unit in ((60, "s"), (60, "m"), (24, "h")):
            if diff < step:
                return "{}{}".format(diff, unit)
            diff //= step
        return "{}d".format(diff)

    def __repr__(self):
        return "<{}: #{} {} @ {}: {}>".format(self.__class__.__name__, self.id, self.ago,
                                              repr(self.channel), repr(self.text))


class NotesHook(immp.Hook, Commandable):
    """
    Hook for managing and recalling notes in channels.
    """

    def commands(self):
        return [Command("note-add", self.add, CommandScope.any, "<text>",
                        "Add a new note for this channel."),
                Command("note-remove", self.remove, CommandScope.any, "<#>",
                        "Delete an existing note from this channel by its position."),
                Command("note-show", self.show, CommandScope.any, "<#>",
                        "Recall a single note in this channel."),
                Command("note-list", self.list, CommandScope.any, None,
                        "Recall all notes for this channel.")]

    async def start(self):
        self.db = self.host.resources[DatabaseHook].db
        self.db.create_tables([Note], safe=True)

    async def add(self, channel, msg, *text):
        Note.create(network=channel.plug.network_id,
                    channel=channel.source,
                    user=(msg.user.id or msg.user.username) if msg.user else None,
                    text=" ".join(text))
        await channel.send(immp.Message(text="{} Added".format(TICK)))

    async def remove(self, channel, msg, pos):
        try:
            pos = int(pos)
        except ValueError:
            return
        try:
            note = Note.select_position(channel, pos)
        except Note.DoesNotExist:
            text = "{} Does not exist".format(CROSS)
        else:
            note.delete_instance()
            text = "{} Removed".format(TICK)
        await channel.send(immp.Message(text=text))

    async def show(self, channel, msg, pos):
        try:
            pos = int(pos)
        except ValueError:
            return
        try:
            note = Note.select_position(channel, pos)
        except Note.DoesNotExist:
            text = "{} Does not exist".format(CROSS)
        else:
            text = immp.RichText([immp.Segment("{}.".format(pos), bold=True),
                                  immp.Segment("\t{}\t".format(note.text)),
                                  immp.Segment(note.ago, italic=True)])
        await channel.send(immp.Message(text=text))

    async def list(self, channel, msg):
        notes = list(Note.select_channel(channel))
        text = immp.RichText([immp.Segment("{} note{} in this channel{}"
                                           .format(len(notes), "" if len(notes) == 1 else "s",
                                                   ":" if notes else "."), bold=bool(notes))])
        for pos, note in enumerate(notes, 1):
            text.append(immp.Segment("\n"),
                        immp.Segment("{}.".format(pos), bold=True),
                        immp.Segment("\t{}\t".format(note.text)),
                        immp.Segment(note.ago, italic=True))
        await channel.send(immp.Message(text=text))
