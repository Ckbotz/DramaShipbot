import logging
from struct import pack
import re
import base64
from typing import Dict, List
from pyrogram.file_id import FileId
from pymongo.errors import DuplicateKeyError
from umongo import Instance, Document, fields
from motor.motor_asyncio import AsyncIOMotorClient
from marshmallow.exceptions import ValidationError
from info import *
from utils import get_settings, save_group_settings
from collections import defaultdict
from logging_helper import LOGGER
from datetime import datetime, timedelta
# MongoDB setup
mongo_client = AsyncIOMotorClient("mongodb+srv://user:pass@cluster/db")
db = mongo_client["yourdbname"]
files_collection = db["files"]
progress_collection = db["sendall_progress"]

BIN_CHANNEL = -100123456789  # replace with your dump channel ID
ADMIN_ID = 123456789  # replace with your Telegram user ID


# ---------- Save skip file_id ----------
@Client.on_message(filters.command("skipfile") & filters.user(ADMIN_ID))
async def set_skip_file(client: Client, message: Message):
    if not message.reply_to_message or not getattr(message.reply_to_message, "media", None):
        return await message.reply_text("⚠️ Reply to a media message with /skipfile")

    file_id = None
    if message.reply_to_message.video:
        file_id = message.reply_to_message.video.file_id
    elif message.reply_to_message.document:
        file_id = message.reply_to_message.document.file_id
    elif message.reply_to_message.audio:
        file_id = message.reply_to_message.audio.file_id

    if not file_id:
        return await message.reply_text("⚠️ Unsupported media type.")

    await progress_collection.update_one(
        {"_id": "skipfile"},
        {"$set": {"file_id": file_id}},
        upsert=True
    )

    await message.reply_text("✅ Skip file set successfully!")


# ---------- Send All Files ----------
@Client.on_message(filters.command("sendall") & filters.user(ADMIN_ID))
async def send_all_files(client: Client, message: Message):
    # Fetch skip file
    skip_doc = await progress_collection.find_one({"_id": "skipfile"})
    skip_file_id = skip_doc["file_id"] if skip_doc else None

    query = {}
    if skip_file_id:
        # find its position in DB
        skip_doc_in_db = await files_collection.find_one({"file_id": skip_file_id})
        if skip_doc_in_db:
            query = {"_id": {"$gte": skip_doc_in_db["_id"]}}

    cursor = files_collection.find(query).sort("_id", 1)
    total_files = await files_collection.count_documents(query)

    progress_msg = await message.reply_text(
        f"📦 Starting to send {total_files} files...\n\nSent: 0/{total_files}",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("❌ Cancel", callback_data="cancel_sendall")]]
        )
    )

    sent = 0
    batch = 0

    # Save cancel state
    await progress_collection.update_one(
        {"_id": "cancel"},
        {"$set": {"status": False}},
        upsert=True
    )

    async for file_data in cursor:
        # Check cancel state
        cancel_doc = await progress_collection.find_one({"_id": "cancel"})
        if cancel_doc and cancel_doc.get("status"):
            await progress_msg.edit_text("⛔ SendAll cancelled by user.")
            return

        file_id = file_data.get("file_id")
        if not file_id:
            continue

        try:
            await client.send_cached_media(
                chat_id=BIN_CHANNEL,
                file_id=file_id
            )
            sent += 1
            batch += 1
        except Exception as e:
            print(f"❌ Error sending {file_id}: {e}")
            continue

        if sent % 500 == 0:  # update progress every 10 files
            try:
                await progress_msg.edit_text(
                    f"📦 Sending files...\n\nSent: {sent}/{total_files}",
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("❌ Cancel", callback_data="cancel_sendall")]]
                    )
                )
            except:
                pass

        if batch == 30:
            batch = 0
            await asyncio.sleep(30)

    await progress_msg.edit_text(f"✅ Finished sending {sent}/{total_files} files.")


# ---------- Cancel Button ----------
@Client.on_callback_query(filters.regex("cancel_sendall") & filters.user(ADMIN_ID))
async def cancel_sendall(client, callback_query):
    await progress_collection.update_one(
        {"_id": "cancel"},
        {"$set": {"status": True}},
        upsert=True
    )
    await callback_query.message.edit_text("⛔ Cancel request received, stopping...")
