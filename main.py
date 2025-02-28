import os.path
from datetime import tzinfo
from typing import assert_never

import pytz
from telethon import TelegramClient, types
from telethon.tl.types import (
    TypeInputMedia,
    TypeMessageMedia,
    MessageMediaDocument,
    MessageMediaWebPage,
    MessageMediaVenue,
    MessageMediaGame,
    MessageMediaInvoice,
    MessageMediaGeoLive,
    MessageMediaPoll,
    MessageMediaDice,
    MessageMediaStory,
    MessageMediaGiveaway,
    MessageMediaGiveawayResults,
    MessageMediaEmpty,
    MessageMediaPhoto,
    MessageMediaGeo,
    MessageMediaContact,
    MessageMediaUnsupported,
)

# Remember to use your own values from my.telegram.org!
api_id = 11111
api_hash = "foobarbaz"
client = TelegramClient("anon", api_id, api_hash)


async def main():
    # Getting information about yourself
    me = await client.get_me()
    try:


    # "me" is a user object. You can pretty-print
    # any Telegram object with the "stringify" method:
    conv = client.conversation(me.id)

    ch: types.User = await conv.get_chat()
    messages: list[types.Message] = await client.get_messages(ch, 5)

    result_file = open("foo.md", "w")
    if not os.path.exists("files"):
        os.mkdir("files")
    try:
        for msg in messages:
            if msg.media is not None:
                file_name: str | None = None
                match msg.media:
                    case MessageMediaEmpty():
                        raise NotImplementedError("Empty media")
                    case MessageMediaPhoto():
                        file_name = f"files/{msg.id}.jpg"
                    case MessageMediaGeo():
                        raise NotImplementedError("MessageMediaGeo")
                    case MessageMediaContact():
                        raise NotImplementedError("MessageMediaContact")
                    case MessageMediaUnsupported():
                        raise NotImplementedError("MessageMediaUnsupported")
                    case MessageMediaDocument():
                        print("MessageMediaDocument")
                        continue
                    case MessageMediaWebPage():
                        pass
                    case MessageMediaVenue():
                        raise NotImplementedError("MessageMediaVenue")
                    case MessageMediaGame():
                        raise NotImplementedError("MessageMediaGame")
                    case MessageMediaInvoice():
                        raise NotImplementedError("MessageMediaInvoice")
                    case MessageMediaGeoLive():
                        raise NotImplementedError("MessageMediaGeoLive")
                    case MessageMediaPoll():
                        raise NotImplementedError("MessageMediaPoll")
                    case MessageMediaDice():
                        raise NotImplementedError("MessageMediaDice")
                    case MessageMediaStory():
                        raise NotImplementedError("MessageMediaStory")
                    case MessageMediaGiveaway():
                        raise NotImplementedError("MessageMediaGiveaway")
                    case MessageMediaGiveawayResults():
                        raise NotImplementedError("MessageMediaGiveawayResults")
                    case _:
                        assert_never(msg.media)
                if file_name is not None:
                    await client.download_media(
                        msg, file_name, progress_callback=download_callback, thumb=-1
                    )
            header = await make_post_header(client, msg)

            result_file.write(header)
            result_file.write(msg.text.replace('#', '\\#') + "\n")
            result_file.write("----------------\n\n")
    finally:
        result_file.close()


async def make_post_header(client, msg: types.Message) -> str:
    # header consists of date
    # if message is forwarded: add link to original message with chat name (markdown)
    # madrid timezone
    header = 'date: {date}  \n'.format(date=msg.date.astimezone(pytz.timezone("Europe/Madrid")).strftime("%Y-%m-%d %H:%M:%S"))

    if msg.forwards:
        original_chat = await client.get_entity(msg.fwd_from.from_id)
        original_message_id = msg.forwards
        header += 'forwarded from: [{original_chat_name}](https://t.me/{original_chat_username}/{original_message_id})  \n'.format(
            original_chat_name=original_chat.title,
            original_chat_username=original_chat.username,
            original_message_id=original_message_id,
        )
    return header


def download_callback(current, total):
    print(
        "Downloaded", current, "out of", total, "bytes: {:.2%}".format(current / total)
    )


with client:
    client.loop.run_until_complete(main())
