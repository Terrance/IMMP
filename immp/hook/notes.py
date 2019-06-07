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
from immp.hook.command import CommandParser, command
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


class NotesHook(immp.Hook):
    """
    Hook for managing and recalling notes in channels.
    """

    def __init__(self, name, config, host):
        super().__init__(name, config, host)
        self.db = None

    async def start(self):
        self.db = self.host.resources[DatabaseHook].db
        self.db.create_tables([Note], safe=True)

    async def channel_migrate(self, old, new):
        count = (Note.update(network=new.plug.network_id, channel=new.source)
                     .where(Note.network == old.plug.network_id,
                            Note.channel == old.source).execute())
        return count > 0

    @command("note-add", parser=CommandParser.none)
    async def add(self, msg, text):
        """
        Add a new note for this channel.
        """
        Note.create(network=msg.channel.plug.network_id,
                    channel=msg.channel.source,
                    user=(msg.user.id or msg.user.username) if msg.user else None,
                    text=text.raw())
        count = Note.select_channel(msg.channel).count()
        await msg.channel.send(immp.Message(text="{} Added #{}".format(TICK, count)))

    @command("note-edit", parser=CommandParser.hybrid)
    async def edit(self, msg, pos, text):
        """
        Update an existing note from this channel with new text.
        """
        try:
            pos = int(pos)
        except ValueError:
            return
        try:
            note = Note.select_position(msg.channel, pos)
        except Note.DoesNotExist:
            text = "{} Does not exist".format(CROSS)
        else:
            note.text = text.raw()
            note.save()
            text = "{} Edited".format(TICK)
        await msg.channel.send(immp.Message(text=text))

    @command("note-remove")
    async def remove(self, msg, pos):
        """
        Delete an existing note from this channel by its position.
        """
        try:
            pos = int(pos)
        except ValueError:
            return
        try:
            note = Note.select_position(msg.channel, pos)
        except Note.DoesNotExist:
            text = "{} Does not exist".format(CROSS)
        else:
            note.delete_instance()
            text = "{} Removed".format(TICK)
        await msg.channel.send(immp.Message(text=text))

    @command("note-show")
    async def show(self, msg, pos):
        """
        Recall a single note in this channel.
        """
        try:
            pos = int(pos)
        except ValueError:
            return
        try:
            note = Note.select_position(msg.channel, pos)
        except Note.DoesNotExist:
            text = "{} Does not exist".format(CROSS)
        else:
            text = immp.RichText([immp.Segment("{}.".format(pos), bold=True),
                                  immp.Segment("\t"),
                                  *immp.RichText.unraw(note.text, self.host),
                                  immp.Segment("\t"),
                                  immp.Segment(note.ago, italic=True)])
        await msg.channel.send(immp.Message(text=text))

    @command("note-list")
    async def list(self, msg, query=None):
        """
        Recall all notes for this channel, or search for text across all notes.
        """
        notes = Note.select_channel(msg.channel)
        if query:
            matches = notes.where(Note.text.contains(query))
            count = len(matches)
        else:
            count = len(notes)
        title = ("{}{} note{} in this channel{}"
                 .format(count, " matching" if query else "",
                         "" if count == 1 else "s", ":" if count else "."))
        text = immp.RichText([immp.Segment(title, bold=bool(notes))])
        for pos, note in enumerate(notes, 1):
            if query and note not in matches:
                continue
            text.append(immp.Segment("\n"),
                        immp.Segment("{}.".format(pos), bold=True),
                        immp.Segment("\t"),
                        *immp.RichText.unraw(note.text, self.host),
                        immp.Segment("\t"),
                        immp.Segment(note.ago, italic=True))
        await msg.channel.send(immp.Message(text=text))
