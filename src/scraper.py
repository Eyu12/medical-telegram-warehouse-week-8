"""
Telegram Scraper for Ethiopian Medical Channels
"""

import os
import json
import asyncio
import logging
import sys
import traceback
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Dict, Any
from dataclasses import dataclass, asdict
from enum import Enum

import aiofiles
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError,
    ChannelPrivateError,
    UsernameNotOccupiedError,
    SessionPasswordNeededError
)
from telethon.tl.types import (
    MessageMediaPhoto,
    MessageMediaDocument,
    DocumentAttributeFilename
)

# --------------------------------------------------------------------------
# PROJECT PATH
# --------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.datalake import write_manifest, write_combined_csv

# --------------------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------------------

load_dotenv()

API_ID = os.getenv("Tg_API_ID")
API_HASH = os.getenv("Tg_API_HASH")

if not API_ID or not API_HASH:
    print("ERROR: Tg_API_ID or Tg_API_HASH missing in .env")
    sys.exit(1)

API_ID = int(API_ID)
TODAY = datetime.today().strftime("%Y-%m-%d")

SCRAPE_LIMIT = 200
MESSAGE_DELAY = 0.5
CHANNEL_DELAY = 3.0

# --------------------------------------------------------------------------
# LOGGING
# --------------------------------------------------------------------------

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(f"logs/scrape_{TODAY}.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("telegram_scraper")

# --------------------------------------------------------------------------
# DATA MODELS
# --------------------------------------------------------------------------

class MediaType(Enum):
    PHOTO = "photo"
    VIDEO = "video"
    AUDIO = "audio"
    DOCUMENT = "document"
    OTHER = "other"

@dataclass
class TelegramMessage:
    message_id: int
    channel_id: int
    channel_username: str
    channel_title: str
    date: str
    text: str
    views: int
    forwards: int
    replies: int
    edit_date: Optional[str]
    media_type: Optional[str]
    file_path: Optional[str]
    url: str
    scraped_at: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

# --------------------------------------------------------------------------
# HELPERS
# --------------------------------------------------------------------------

def sanitize_filename(name: str) -> str:
    invalid = '<>:"/\\|?*'
    for c in invalid:
        name = name.replace(c, "_")
    return name[:200]

def extract_replies(message) -> int:
    try:
        return message.replies.replies if message.replies else 0
    except Exception:
        return 0

def detect_media_type(media) -> Optional[MediaType]:
    if isinstance(media, MessageMediaPhoto):
        return MediaType.PHOTO
    if isinstance(media, MessageMediaDocument):
        for attr in media.document.attributes:
            if isinstance(attr, DocumentAttributeFilename):
                name = attr.file_name.lower()
                if name.endswith((".mp4", ".mkv")):
                    return MediaType.VIDEO
                if name.endswith((".mp3", ".wav")):
                    return MediaType.AUDIO
        return MediaType.DOCUMENT
    return None

async def download_media(client, message, media_type, out_dir) -> Optional[str]:
    if not message.media:
        return None
    try:
        os.makedirs(out_dir, exist_ok=True)
        path = await client.download_media(message.media, out_dir)
        return path
    except Exception as e:
        logger.error(f"Media download failed for message {message.id}: {e}")
        return None

# --------------------------------------------------------------------------
# SCRAPING
# --------------------------------------------------------------------------

async def scrape_channel(client, channel: str, base_path: str) -> List[TelegramMessage]:
    messages: List[TelegramMessage] = []
    try:
        entity = await client.get_entity(channel)
        image_dir = os.path.join(base_path, "raw", "images", sanitize_filename(channel))

        async for msg in client.iter_messages(entity, limit=SCRAPE_LIMIT):
            try:
                media_type = detect_media_type(msg.media)
                file_path = None
                if media_type:
                    file_path = await download_media(client, msg, media_type, image_dir)

                message = TelegramMessage(
                    message_id=msg.id,
                    channel_id=entity.id,
                    channel_username=entity.username or channel,
                    channel_title=entity.title or "",
                    date=msg.date.isoformat() if msg.date else "",
                    text=msg.text or "",
                    views=msg.views or 0,
                    forwards=msg.forwards or 0,
                    replies=extract_replies(msg),
                    edit_date=msg.edit_date.isoformat() if msg.edit_date else None,
                    media_type=media_type.value if media_type else None,
                    file_path=file_path,
                    url=f"https://t.me/{entity.username}/{msg.id}" if entity.username else "",
                    scraped_at=datetime.now().isoformat()
                )

                messages.append(message)
                await asyncio.sleep(MESSAGE_DELAY)

            except Exception as e:
                logger.error(f"Message {msg.id} skipped: {e}")

        logger.info(f"Scraped {len(messages)} messages from {channel}")

    except FloodWaitError as e:
        logger.warning(f"Flood wait: sleeping {e.seconds}s")
        await asyncio.sleep(e.seconds)

    except (ChannelPrivateError, UsernameNotOccupiedError):
        logger.error(f"Cannot access channel: {channel}")

    return messages

# --------------------------------------------------------------------------
# MAIN PIPELINE
# --------------------------------------------------------------------------

async def run_scraper(channels: List[str], base_path: str):
    all_counts: Dict[str, int] = {}
    combined_rows: List[Dict[str, Any]] = []

    for channel in channels:
        logger.info(f"Scraping channel: {channel}")
        msgs = await scrape_channel(client, channel, base_path)
        all_counts[channel] = len(msgs)

        if msgs:
            json_dir = os.path.join(base_path, "raw", "telegram_messages", TODAY)
            os.makedirs(json_dir, exist_ok=True)

            json_path = os.path.join(json_dir, f"{sanitize_filename(channel)}.json")
            async with aiofiles.open(json_path, "w", encoding="utf-8") as f:
                await f.write(json.dumps([m.to_dict() for m in msgs], indent=2, ensure_ascii=False))

            # Append for CSV
            for m in msgs:
                combined_rows.append(m.to_dict())

        await asyncio.sleep(CHANNEL_DELAY)

    # Write combined CSV
    write_combined_csv(
        base_path=base_path,
        date_str=TODAY,
        rows=combined_rows,
    )

    # Write manifest
    write_manifest(
        base_path=base_path,
        date_str=TODAY,
        channel_message_counts=all_counts,
    )

    logger.info("SCRAPING COMPLETE")

# --------------------------------------------------------------------------
# ENTRY POINT
# --------------------------------------------------------------------------

if __name__ == "__main__":

    channels = [
        "@cheMed123",
        "@lobelia4cosmetics",
        "@tikvahpharma",
    ]

    client = TelegramClient(
        "telegram_scraper_session",
        API_ID,
        API_HASH,
        device_model="Medical Scraper",
        system_version="1.0",
        app_version="2.0"
    )

    async def main():
        try:
            async with client:
                me = await client.get_me()
                logger.info(f"Logged in as {me.first_name}")
                await run_scraper(channels, base_path="data")

        except SessionPasswordNeededError:
            logger.error("2FA enabled on Telegram account")
        except Exception:
            logger.error(traceback.format_exc())

    asyncio.run(main())
