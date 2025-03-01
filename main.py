import os
import json
import sqlite3
import datetime
import logging
import asyncio
import re
import click
from typing import Optional, Dict, Any, List, Tuple
from pathlib import Path

from openai import OpenAI

import pytz
from pydantic import BaseModel
from telethon import TelegramClient, types
from telethon.tl.functions.messages import GetMessagesRequest
from telethon.tl.types import (
    MessageMediaDocument,
    MessageMediaEmpty,
    MessageMediaPhoto,
)

import dotenv

from ai import (
    get_assistant,
    create_assistant,
    make_thread,
    submit_articles,
)

dotenv.load_dotenv('.env.local')
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


class TagsResponse(BaseModel):
    eng: list[str]
    ru: list[str]


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
DB_PATH = Path("telegram_messages.sqlite3")
OUTPUT_DIR = Path("telegram_export")
FILES_DIR = OUTPUT_DIR / "files"
MARKDOWN_FILES_PATH = "files"  # Relative path for markdown links
OUTPUT_FILE = OUTPUT_DIR / "saved_messages.md"
TIMEZONE = "Europe/Madrid"
DOWNLOAD_CONCURRENCY = 5  # Maximum number of concurrent downloads

def get_db_connection() -> sqlite3.Connection:
    """Obtain a SQLite database connection."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db() -> None:
    """Initialize SQLite database for message storage."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                message_id INTEGER PRIMARY KEY,
                json_data TEXT NOT NULL,
                file_downloaded INTEGER DEFAULT 0,
                date TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id INTEGER,
                file_path TEXT,
                markdown_path TEXT,
                file_type TEXT,
                downloaded INTEGER DEFAULT 0,
                FOREIGN KEY (message_id) REFERENCES messages (message_id)
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS tags (
                message_id INTEGER,
                language TEXT,
                tags TEXT,
                PRIMARY KEY (message_id, language)
            )
        ''')
        conn.execute('''
        create table if not exists meta (
        key string not null primary key,
        value string not null
        )''')
        conn.commit()


def get_meta(key: str) -> str | None:
    with get_db_connection() as conn:
        cursor = conn.execute("SELECT value FROM meta where key = ?", (key,))
        row = cursor.fetchone()
        if not row:
            return
        return row[0]



def save_meta(key: str, value: str) -> None:
    with get_db_connection() as conn:
        conn.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", (key, value))
        conn.commit()

def save_message_to_db(message: types.Message) -> None:
    """Save a Telegram message to the database as JSON."""
    json_data = message.to_json()
    date_str = str(message.date)
    with get_db_connection() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO messages (message_id, json_data, date) VALUES (?, ?, ?)",
            (message.id, json_data, date_str)
        )
        conn.commit()

def get_all_messages_from_db() -> list[tuple[int, str]]:
    """Retrieve all messages from the database."""
    with get_db_connection() as conn:
        cursor = conn.execute("SELECT message_id, json_data FROM messages ORDER BY message_id DESC")
        results = cursor.fetchall()
    return [(row["message_id"], row["json_data"]) for row in results]

def register_file_in_db(message_id: int, file_path: str, markdown_path: str, file_type: str) -> None:
    """Register a file in the database for a given message."""
    with get_db_connection() as conn:
        conn.execute(
            "INSERT INTO files (message_id, file_path, markdown_path, file_type) VALUES (?, ?, ?, ?)",
            (message_id, file_path, markdown_path, file_type)
        )
        conn.commit()

def update_file_downloaded(message_id: int, file_path: str) -> None:
    """Mark a file as downloaded in the database."""
    with get_db_connection() as conn:
        conn.execute(
            "UPDATE files SET downloaded = 1 WHERE message_id = ? AND file_path = ?",
            (message_id, file_path)
        )
        conn.execute(
            """
            UPDATE messages 
            SET file_downloaded = 1 
            WHERE message_id = ? AND 
            (SELECT COUNT(*) FROM files WHERE message_id = ? AND downloaded = 0) = 0
            """,
            (message_id, message_id)
        )
        conn.commit()

def get_files_to_download() -> List[Tuple[int, str, str]]:
    """Get list of files that need to be downloaded."""
    with get_db_connection() as conn:
        cursor = conn.execute("""
            SELECT f.message_id, f.file_path, m.json_data 
            FROM files f
            JOIN messages m ON f.message_id = m.message_id
            WHERE f.downloaded = 0
        """)
        results = cursor.fetchall()
    return [(row[0], row[1], row[2]) for row in results]

async def get_client() -> TelegramClient:
    """Get authenticated Telegram client."""
    client = TelegramClient("anon", API_ID, API_HASH)
    await client.start()
    return client

def download_callback(current: int, total: int, message_id: int, file_path: str) -> None:
    """Progress callback for file downloads."""
    percentage = 100 * (current / total)
    logger.info(f"Message {message_id}: Downloaded {current}/{total} bytes: {percentage:.2f}%")
    if current == total:
        update_file_downloaded(message_id, file_path)

async def identify_media_files(client: TelegramClient, msg: types.Message) -> None:
    """Identify media files in a message and register them in the database."""
    message_id = msg.id
    if not hasattr(msg, 'media') or msg.media is None:
        return
    try:
        match msg.media:
            case MessageMediaEmpty():
                return
            case MessageMediaPhoto():
                file_name = f"{msg.id}_photo.jpg"
                file_path = str(FILES_DIR / file_name)
                markdown_path = f"{MARKDOWN_FILES_PATH}/{file_name}"
                register_file_in_db(message_id, file_path, markdown_path, "photo")
            case MessageMediaDocument():
                document = msg.media.document
                file_name = None
                for attr in document.attributes:
                    if hasattr(attr, 'file_name') and attr.file_name:
                        file_name = attr.file_name
                        break
                if not file_name:
                    mime_type = document.mime_type
                    extension = mime_type.split('/')[-1] if '/' in mime_type else 'file'
                    file_name = f"{msg.id}_document.{extension}"
                file_name = re.sub(r'[\\/*?:"<>|]', '_', file_name)
                file_path = str(FILES_DIR / file_name)
                markdown_path = f"{MARKDOWN_FILES_PATH}/{file_name}"
                file_type = "image" if document.mime_type.startswith('image/') else "document"
                register_file_in_db(message_id, file_path, markdown_path, file_type)
    except Exception as e:
        logger.error(f"Error identifying media in message {message_id}: {e}")

@click.group()
def cli() -> None:
    """Telegram Saved Messages Exporter"""
    init_db()

@cli.command()
@click.option('--limit', default=10, help='Number of messages to fetch (0 for all)')
def get_messages(limit: int) -> None:
    """Fetch messages from Telegram and store them in the SQLite database."""
    async def _get_messages() -> None:
        client = await get_client()
        me = await client.get_me()
        logger.info(f"Logged in as {me.first_name} (ID: {me.id})")
        message_limit = None if limit == 0 else limit
        logger.info(f"Fetching {'all' if limit == 0 else limit} messages from saved messages")
        messages = await client.get_messages("me", limit=message_limit)
        logger.info(f"Found {len(messages)} messages")
        for i, msg in enumerate(messages, 1):
            try:
                logger.info(f"Processing message {i}/{len(messages)} (ID: {msg.id})")
                save_message_to_db(msg)
                await identify_media_files(client, msg)
            except Exception as e:
                logger.error(f"Error processing message {msg.id}: {e}")
        logger.info(f"Successfully stored {len(messages)} messages in the database")
    asyncio.run(_get_messages())
    logger.info("Message fetching complete")

async def download_single_file(client: TelegramClient, message_id: int, file_path: str, json_data: str, semaphore: asyncio.Semaphore) -> None:
    """Download a single file using the Telegram client."""
    async with semaphore:
        try:
            logger.info(f"Downloading file for message {message_id}")
            messages = await client(GetMessagesRequest(id=[message_id]))
            if not messages or not messages.messages:
                logger.error(f"Could not retrieve message {message_id} from Telegram")
                return
            message = messages.messages[0]
            callback = lambda current, total, mid=message_id, fp=file_path: download_callback(current, total, mid, fp)
            await client.download_media(message, file_path, progress_callback=callback)
            logger.info(f"Downloaded file for message {message_id}")
        except Exception as e:
            logger.error(f"Error downloading file for message {message_id}: {e}")

@cli.command()
def download_files() -> None:
    """Download files for messages stored in the database."""
    async def _download_files() -> None:
        OUTPUT_DIR.mkdir(exist_ok=True)
        FILES_DIR.mkdir(exist_ok=True, parents=True)
        files = get_files_to_download()
        if not files:
            logger.info("No files to download")
            return
        logger.info(f"Found {len(files)} files to download")
        client = await get_client()
        semaphore = asyncio.Semaphore(DOWNLOAD_CONCURRENCY)
        tasks = [
            download_single_file(client, message_id, file_path, json_data, semaphore)
            for message_id, file_path, json_data in files
        ]
        await asyncio.gather(*tasks)
        logger.info("File download complete")
    asyncio.run(_download_files())

def get_file_references(message_id: int) -> List[Tuple[str, str]]:
    """Get file references for a message."""
    with get_db_connection() as conn:
        cursor = conn.execute(
            "SELECT markdown_path, file_type FROM files WHERE message_id = ? AND downloaded = 1",
            (message_id,)
        )
        results = cursor.fetchall()
    return [(row[0], row[1]) for row in results]

def extract_message_data(message_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Extract relevant data from a message dictionary."""
    data = {
        'id': message_dict.get('id', 0),
        'message': message_dict.get('message', ''),
        'date': message_dict.get('date', None),
        'to_id': message_dict.get('to_id', {}),
    }
    if 'fwd_from' in message_dict and message_dict['fwd_from']:
        data['fwd_from'] = message_dict['fwd_from']
    # Optionally include full chat details if available
    if 'chat' in message_dict:
        data['chat'] = message_dict['chat']
    return data



# Updated export: include tags in the Markdown export.
def format_message_for_markdown(message_dict: Dict[str, Any], message_id: int) -> str:
    """Format a message dictionary for Markdown output with message links and tags."""
    message_data = extract_message_data(message_dict)
    date_str = message_data.get('date')
    try:
        dt = datetime.datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        dt = dt.astimezone(pytz.timezone(TIMEZONE))
        formatted_date = dt.strftime("%A, %B %d, %Y at %H:%M:%S")
    except Exception:
        formatted_date = "Unknown date"

    markdown = f"## Message {message_id}\n\n"
    markdown += f"**Date:** {formatted_date}\n\n"

    # Generate original message link.
    to_id = message_data.get("to_id", {})
    if (to_id.get("_") == "PeerChannel") or ("channel_id" in to_id):
        channel_id = to_id.get("channel_id")
        chat_info = message_data.get("chat", {})
        username = chat_info.get("username") if chat_info else None
        if username:
            original_link = f"https://t.me/{username}/{message_id}"
        else:
            channel_id_str = str(channel_id)
            if channel_id_str.startswith("-100"):
                channel_id_str = channel_id_str.replace("-100", "")
            original_link = f"https://t.me/c/{channel_id_str}/{message_id}"
    else:
        original_link = f"tg://msg?id={message_id}"
    markdown += f"[View Original Message]({original_link})\n\n"

    # Process forwarded message link if available.
    fwd = message_data.get('fwd_from')
    if fwd and isinstance(fwd, dict):
        channel_post = fwd.get("channel_post")
        from_id = fwd.get("from_id")
        if channel_post and isinstance(from_id, dict) and "channel_id" in from_id:
            channel_id = from_id["channel_id"]
            chat_info = fwd.get("chat", {})  # if available
            username = chat_info.get("username") if chat_info else None
            if username:
                forwarded_link = f"https://t.me/{username}/{channel_post}"
            else:
                channel_id_str = str(channel_id)
                if channel_id_str.startswith("-100"):
                    channel_id_str = channel_id_str.replace("-100", "")
                forwarded_link = f"https://t.me/c/{channel_id_str}/{channel_post}"
            markdown += f"[View Forwarded Message]({forwarded_link})\n\n"
        else:
            forwarded_from = fwd.get("from_name", "Unknown")
            markdown += f"**Forwarded from:** {forwarded_from}\n\n"

    # Attachments section.
    file_refs = get_file_references(message_id)
    if file_refs:
        markdown += "### Attachments\n\n"
        for file_path, file_type in file_refs:
            if file_type in ("image", "photo"):
                markdown += f"![Image]({file_path})\n\n"
            else:
                filename = os.path.basename(file_path)
                markdown += f"- [{filename}]({file_path})\n"
        markdown += "\n"

    # Message text section (raw text, no escaping).
    text = message_data.get('message', '').strip()
    markdown += "### Message Text\n\n"
    if text:
        markdown += f"{text}\n\n"
    else:
        markdown += "_No text provided_\n\n"

    # Tags section.
    all_tags = get_all_tags_for_message(message_id)
    if all_tags:
        # make list of all tags
        total_tags = []
        for lang, tags in all_tags:
            total_tags += [f'#{t.strip()}' for t in json.loads(tags) if t is not None]
        if total_tags:
            markdown += "### Tags\n\n"
            markdown += f"{', '.join(total_tags)}\n\n"

    markdown += "---\n\n"
    return markdown

@cli.command()
def export_markdown() -> None:
    """Export messages and downloaded files to a well-formatted Markdown file."""
    OUTPUT_DIR.mkdir(exist_ok=True)
    messages = get_all_messages_from_db()
    if not messages:
        logger.info("No messages to export")
        return
    logger.info(f"Exporting {len(messages)} messages to Markdown")
    with OUTPUT_FILE.open("w", encoding="utf-8") as result_file:
        # Global header for the export.
        result_file.write(f"# Telegram Saved Messages Export\n\n")
        result_file.write(f"**Export Date:** {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        result_file.write(f"**Total Messages:** {len(messages)}\n\n")
        result_file.write("---\n\n")
        # Process and format each message.
        for i, (message_id, json_data) in enumerate(messages, 1):
            try:
                logger.info(f"Formatting message {i}/{len(messages)} (ID: {message_id})")
                message_dict = json.loads(json_data)
                markdown_content = format_message_for_markdown(message_dict, message_id)
                result_file.write(markdown_content)
            except Exception as e:
                logger.error(f"Error formatting message {message_id}: {e}")
                result_file.write(f"## Message {message_id}\n\n**Error processing this message**\n\n---\n\n")
    logger.info(f"Export completed successfully. Output saved to {OUTPUT_FILE}")


def get_tags_for_message(message_id: int) -> Optional[str]:
    """Retrieve tags for a given message from the database."""
    with get_db_connection() as conn:
        cursor = conn.execute(
            "SELECT tags FROM tags WHERE message_id = ?",
            (message_id,)
        )
        row = cursor.fetchone()
    return row["tags"] if row else None


def get_all_tags_for_message(message_id: int) -> List[Tuple[str, str]]:
    """Retrieve all tags records for a given message from the database."""
    with get_db_connection() as conn:
        cursor = conn.execute("SELECT language, tags FROM tags WHERE message_id = ?", (message_id,))
        results = cursor.fetchall()
    return [(row["language"], row["tags"]) for row in results]



def store_tags_for_message(message_id: int, tags: list[str]) -> None:
    """Store generated tags for a message in the database."""
    with get_db_connection() as conn:

        conn.execute(
            "INSERT OR REPLACE INTO tags (message_id, language, tags) VALUES (?, ?, ?)",
            (message_id, '', json.dumps(tags))
        )
        conn.commit()



@cli.command()
@click.option('-o', '--overwrite', is_flag=True, help='Force overwrite existing tags.')
def add_tags(overwrite: bool) -> None:
    """Generate and add hashtags for each message using the OpenAI API."""
    messages = get_all_messages_from_db()
    if not messages:
        logger.info("No messages found in the database.")
        return

    articles = {message_id: json.loads(data).get("message", "").strip() for message_id, data in messages}
    articles = {message_id: text for message_id, text in articles.items() if text}
    if not overwrite:
        articles = {message_id: text for message_id, text in articles.items() if not get_tags_for_message(message_id)}

    ass_id = get_meta('assistant_id')
    if ass_id is None:
        ass = create_assistant(client)
        save_meta('assistant_id', ass.id)
        ass_id = ass.id

    thread_id = get_meta('thread_id')
    if thread_id is None:
        thread = make_thread(client)
        save_meta('thread_id', thread.id)
        thread_id = thread.id

    # batches by 50 articles
    articles_list = list(articles.items())
    for i in range(0, len(articles_list), 50):
        batch = articles_list[i:i+50]
        result = submit_articles(client, thread_id, ass_id, {k: v for k, v in batch})
        for message_id, tags in result.items():
            if tags is not None:
                store_tags_for_message(message_id, tags)


if __name__ == "__main__":
    cli()
