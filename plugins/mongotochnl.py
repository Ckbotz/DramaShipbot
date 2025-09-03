import asyncio
import re
import ast
import math
import random
import pytz
from datetime import datetime, timedelta, date, time
lock = asyncio.Lock()
from database.users_chats_db import db
from database.refer import referdb
from pyrogram.errors.exceptions.bad_request_400 import MediaEmpty, PhotoInvalidDimensions, WebpageMediaEmpty
from Script import script
import pyrogram
from info import *
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, InputMediaPhoto, WebAppInfo
from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait, UserIsBlocked, MessageNotModified, PeerIdInvalid
from utils import *
from fuzzywuzzy import process
from database.users_chats_db import db
from database.ia_filterdb import Media, Media2, get_file_details, get_search_results, get_bad_files
import logging
from urllib.parse import quote_plus
from Lucia.util.file_properties import get_name, get_hash, get_media_file_size
from database.topdb import silentdb
import requests
import string
import tracemalloc
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
from bson.objectid import ObjectId

# MongoDB setup
mongo_client = AsyncIOMotorClient(DATABASE_URI)
db = mongo_client[DATABASE_NAME]
files_collection = db[COLLECTION_NAME]
progress_collection = db["sendall_progress"]

# Use a global variable for cancellation
sendall_cancelled = False

# ---------- Save skip file_id ----------
@Client.on_message(filters.command("skipfile") & filters.user(ADMINS))
async def set_skip_file(client: Client, message: Message):
    if not message.reply_to_message or not getattr(message.reply_to_message, "media", None):
        return await message.reply_text("⚠️ Reply to a media message with /skipfile.")

    file_id = None
    if message.reply_to_message.video:
        file_id = message.reply_to_message.video.file_id
    elif message.reply_to_message.document:
        file_id = message.reply_to_message.document.file_id
    elif message.reply_to_message.audio:
        file_id = message.reply_to_message.audio.file_id

    if not file_id:
        return await message.reply_text("⚠️ Unsupported media type.")

    # Find the ObjectId of the replied-to file
    file_doc = await files_collection.find_one({"file_id": file_id})
    if not file_doc:
        return await message.reply_text("⚠️ This file was not found in the database.")

    await progress_collection.update_one(
        {"_id": "skipfile"},
        {"$set": {"last_sent_id": file_doc["_id"]}},
        upsert=True
    )
    
    await message.reply_text("✅ Skip file set successfully! The next `sendall` command will resume from this file.")

# ---------- Send All Files ----------
@Client.on_message(filters.command("sendall") & filters.user(ADMINS))
async def send_all_files(client: Client, message: Message):
    global sendall_cancelled
    sendall_cancelled = False

    # Resume support (skip file)
    skip_doc = await progress_collection.find_one({"_id": "skipfile"})
    start_id = skip_doc.get("last_sent_id") if skip_doc and "last_sent_id" in skip_doc else None

    query = {"_id": {"$gte": start_id}} if start_id else {}
    cursor = files_collection.find(query).sort("_id", 1)
    total_files = await files_collection.count_documents(query)

    if total_files == 0:
        return await message.reply_text("⚠️ No files found to send.")

    progress_msg = await message.reply_text(
        f"📦 Starting to send **{total_files}** files...\n\nSent: 0/{total_files}",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("❌ Cancel", callback_data="cancel_sendall")]]
        )
    )

    sent = 0
    errors = 0
    batch = 0
    start_time = datetime.now()

    async for file_doc in cursor:
        if sendall_cancelled:
            await progress_msg.edit_text("⛔ SendAll cancelled by user.")
            await progress_collection.delete_one({"_id": "skipfile"})
            return

        file_id = file_doc.get("file_id")
        if not file_id:
            continue

        try:
            # Get latest file details
            files_ = await get_file_details(file_id)
            if not files_:
                continue
            files = files_[0]

            title = ' '.join(
                filter(lambda x: not x.startswith('[') and not x.startswith('@') and not x.startswith('www.'),
                       files.file_name.split())
            )
            size = get_size(files.file_size)
            f_caption = files.caption

            # Load channel settings
            settings = await get_settings(BIN_CHANNEL)
            SILENTX_CAPTION = settings.get('caption', CUSTOM_FILE_CAPTION)

            if SILENTX_CAPTION:
                try:
                    f_caption = SILENTX_CAPTION.format(
                        file_name=title or "",
                        file_size=size or "",
                        file_caption=f_caption or ""
                    )
                except Exception as e:
                    LOGGER.warning(f"Caption format error: {e}")
                    f_caption = f_caption or title

            if not f_caption:
                f_caption = title

            # Send file (permanent, no auto-delete)
            await client.send_cached_media(
                chat_id=BIN_CHANNEL,
                file_id=file_id,
                caption=f_caption,
                protect_content=settings.get('file_secure', PROTECT_CONTENT)
            )
            sent += 1
            batch += 1

        except Exception as e:
            errors += 1
            LOGGER.error(f"❌ Error sending {file_id}: {e}")
            await client.send_message(
                chat_id=ADMINS[0],
                text=f"⚠️ Error sending file:\n\nFile ID: `{file_id}`\nError: `{e}`"
            )

        # Update progress message
        if sent % 50 == 0:
            try:
                await progress_msg.edit_text(
                    f"📦 Sending files...\n\nSent: **{sent}/{total_files}**\nErrors: **{errors}**",
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("❌ Cancel", callback_data="cancel_sendall")]]
                    )
                )
            except MessageNotModified:
                pass

        # Save last sent file for resume
        await progress_collection.update_one(
            {"_id": "skipfile"},
            {"$set": {"last_sent_id": file_doc["_id"]}},
            upsert=True
        )

        # Batch sleep (avoid FloodWait)
        if batch >= 30:
            batch = 0
            await asyncio.sleep(10)

    # Finished
    end_time = datetime.now()
    duration = end_time - start_time
    minutes, seconds = divmod(duration.total_seconds(), 60)

    await progress_msg.edit_text(
        f"✅ Finished sending files!\n\n"
        f"**Summary:**\n"
        f"Total Sent: **{sent}/{total_files}**\n"
        f"Errors: **{errors}**\n"
        f"Duration: **{int(minutes)}m {int(seconds)}s**"
    )
    await progress_collection.delete_one({"_id": "skipfile"})

# ---------- Cancel Button ----------
@Client.on_callback_query(filters.regex("cancel_sendall") & filters.user(ADMINS))
async def cancel_sendall(client, callback_query):
    global sendall_cancelled
    sendall_cancelled = True
    await callback_query.message.edit_text("⛔ Cancel request received, stopping soon...")

