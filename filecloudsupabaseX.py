import asyncio
import os
import uuid
import base64
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple, Any

# Imports for Health Check Server
import http.server
import socketserver
import threading

# Import psycopg2 for PostgreSQL (Supabase)
import psycopg2

from telegram import (
    Update, InlineKeyboardMarkup, InlineKeyboardButton, Message, CallbackQuery
)
from telegram.ext import (
    Application, ApplicationBuilder, ContextTypes,
    CommandHandler, MessageHandler, filters, CallbackQueryHandler,
    JobQueue # Import JobQueue explicitly for manual instantiation
)
from telegram.error import BadRequest # Import BadRequest for specific error handling

###############################################################################
# 1 — CONFIGURATION (MODIFIED TO USE ENVIRONMENT VARIABLES)
###############################################################################
BOT_TOKEN = os.environ.get("BOT_TOKEN") # Read from environment variable
STORAGE_CHANNEL_ID = int(os.environ.get("STORAGE_CHANNEL_ID")) # Read and convert to int
BOT_USERNAME = os.environ.get("BOT_USERNAME") # Read from environment variable

# Admin Configuration
# Split string by comma and convert to int for ADMIN_IDS
ADMIN_IDS = list(map(int, os.environ.get("ADMIN_IDS", "").split(','))) if os.environ.get("ADMIN_IDS") else []
ADMIN_CONTACT = os.environ.get("ADMIN_CONTACT") # Read from environment variable
CUSTOM_CAPTION = os.environ.get("CUSTOM_CAPTION", "t.me/movieandwebserieshub") # Read from env, with default

# Health Check Server Port
# Render typically exposes PORT via an environment variable.
# Use 8000 as a fallback for local testing if PORT is not set.
HEALTH_CHECK_PORT = int(os.environ.get("PORT", 8000))

# Supabase Configuration
SUPABASE_URL = os.environ.get("SUPABASE_URL")  # Full PostgreSQL connection string

# Database and limits - INCREASED TO 10GB LIMIT
MAX_FILE_SIZE = 10 * 1024 * 1024 * 1024  # 10GB

# Bulk Upload Delay (in seconds)
BULK_UPLOAD_DELAY = 1.5

###############################################################################
# 2 — ENHANCED LOGGING SYSTEM
###############################################################################
def clear_console():
    """Clear console screen"""
    os.system('cls' if os.name == 'nt' else 'clear')

def setup_logging():
    """Setup logging with Windows compatibility"""
    clear_console()

    logger = logging.getLogger("FileStoreBot")
    logger.setLevel(logging.INFO)

    # Clear existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # File handler with UTF-8
    try:
        file_handler = logging.FileHandler('bot.log', encoding='utf-8')
        file_formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    except Exception:
        pass

    # Console handler with safe emoji handling
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    console_handler.setFormatter(console_formatter)

    # Safe emit function for Windows
    original_emit = console_handler.emit
    def safe_emit(record):
        try:
            if hasattr(record, 'msg'):
                record.msg = str(record.msg).encode('ascii', 'ignore').decode('ascii')
            original_emit(record)
        except Exception:
            pass

    console_handler.emit = safe_emit
    logger.addHandler(console_handler)

    return logger

logger = setup_logging()

###############################################################################
# 3 — FIXED DATABASE INITIALIZATION (MODIFIED FOR SUPABASE/POSTGRESQL)
###############################################################################
def init_database():
    """Initialize PostgreSQL database with proper SQL syntax"""
    try:
        conn = psycopg2.connect(SUPABASE_URL)
        cursor = conn.cursor()

        # Create tables if not exists (adjusted for PostgreSQL with BIGINT where needed)
        logger.info("Creating authorized_users table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS authorized_users (
                id SERIAL PRIMARY KEY,
                user_id BIGINT UNIQUE NOT NULL,
                username TEXT,
                first_name TEXT,
                added_by BIGINT NOT NULL,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active INTEGER DEFAULT 1,
                caption_disabled INTEGER DEFAULT 0
            )
        """)

        logger.info("Creating groups table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS groups (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                owner_id BIGINT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                total_files INTEGER DEFAULT 0,
                total_size BIGINT DEFAULT 0,
                UNIQUE(name, owner_id)
            )
        """)

        logger.info("Creating files table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS files (
                id SERIAL PRIMARY KEY,
                group_id BIGINT NOT NULL,
                serial_number INTEGER NOT NULL,
                unique_id TEXT UNIQUE NOT NULL,
                file_name TEXT,
                file_type TEXT NOT NULL,
                file_size BIGINT DEFAULT 0,
                telegram_file_id TEXT NOT NULL,
                uploader_id BIGINT NOT NULL,
                uploader_username TEXT,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                storage_message_id BIGINT,
                FOREIGN KEY (group_id) REFERENCES groups(id) ON DELETE CASCADE,
                UNIQUE(group_id, serial_number)
            )
        """)

        logger.info("Creating file_links table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS file_links (
                id SERIAL PRIMARY KEY,
                link_code TEXT UNIQUE NOT NULL,
                file_id BIGINT,
                group_id BIGINT,
                link_type TEXT NOT NULL CHECK (link_type IN ('file', 'group')),
                owner_id BIGINT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                clicks BIGINT DEFAULT 0,
                is_active INTEGER DEFAULT 1,
                FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE,
                FOREIGN KEY (group_id) REFERENCES groups(id) ON DELETE CASCADE
            )
        """)

        logger.info("Creating bot_settings table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bot_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Insert default settings with ON CONFLICT
        logger.info("Inserting default bot settings...")
        cursor.execute("INSERT INTO bot_settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO NOTHING", ('caption_enabled', '1'))
        cursor.execute("INSERT INTO bot_settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO NOTHING", ('custom_caption', CUSTOM_CAPTION))

        # Add admins to authorized users
        logger.info(f"Processing ADMIN_IDS: {ADMIN_IDS}")
        if ADMIN_IDS:
            for admin_id in ADMIN_IDS:
                try:
                    admin_id_int = int(admin_id)  # Ensure it's an integer
                    logger.info(f"Inserting admin ID: {admin_id_int}")
                    cursor.execute("""
                        INSERT INTO authorized_users (user_id, username, first_name, added_by, is_active)
                        VALUES (%s, %s, %s, %s, 1) ON CONFLICT (user_id) DO NOTHING
                    """, (admin_id_int, f'admin_{admin_id_int}', f'Admin {admin_id_int}', admin_id_int))
                except ValueError as ve:
                    logger.error(f"Invalid admin ID {admin_id}: {ve}")
                except Exception as e:
                    logger.error(f"Error inserting admin ID {admin_id}: {e}")

        conn.commit()
        logger.info("Database initialized successfully")
        conn.close()

    except Exception as e:
        logger.error(f"Database initialization error: {e}")
        raise e

###############################################################################
# 4 — UTILITY FUNCTIONS
###############################################################################
def is_admin(user_id: int) -> bool:
    """Check if user is admin"""
    return user_id in ADMIN_IDS

def generate_id() -> str:
    """Generate short unique ID"""
    return base64.urlsafe_b64encode(uuid.uuid4().bytes)[:12].decode()

def format_size(size_bytes: int) -> str:
    """Format file size"""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024**2:
        return f"{size_bytes/1024:.1f} KB"
    elif size_bytes < 1024**3:
        return f"{size_bytes/(1024**2):.1f} MB"
    else:
        return f"{size_bytes/(1024**3):.1f} GB"

def extract_file_data(message: Message) -> Tuple[Optional[Any], str, str, int]:
    """Extract file information from message"""
    if message.document:
        doc = message.document
        return doc, "document", doc.file_name or "document", doc.file_size or 0
    elif message.photo:
        photo = message.photo[-1]
        return photo, "photo", f"photo_{photo.file_id[:8]}.jpg", photo.file_size or 0
    elif message.video:
        video = message.video
        return video, "video", video.file_name or f"video_{video.file_id[:8]}.mp4", video.file_size or 0
    elif message.audio:
        audio = message.audio
        return audio, "audio", audio.file_name or f"audio_{audio.file_id[:8]}.mp3", audio.file_size or 0
    elif message.voice:
        voice = message.voice
        return voice, "voice", f"voice_{voice.file_id[:8]}.ogg", voice.file_size or 0
    elif message.video_note:
        vn = message.video_note
        return vn, "video_note", f"videonote_{vn.file_id[:8]}.mp4", vn.file_size or 0
    return None, "", "", 0

async def get_caption_setting() -> tuple:
    """Get current caption settings from database"""
    def sync_get_caption_setting():
        conn = psycopg2.connect(SUPABASE_URL)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT key, value FROM bot_settings
            WHERE key IN ('caption_enabled', 'custom_caption')
        """)
        settings = cursor.fetchall()
        conn.close()
        return settings

    try:
        settings = await asyncio.to_thread(sync_get_caption_setting)
        caption_enabled = True
        custom_caption = CUSTOM_CAPTION

        for key, value in settings:
            if key == 'caption_enabled':
                caption_enabled = value == '1'
            elif key == 'custom_caption':
                custom_caption = value

        return caption_enabled, custom_caption
    except Exception:
        return True, CUSTOM_CAPTION

async def get_file_caption(file_name: str, serial_number: int = None, user_id: int = None) -> str:
    """Generate file caption with user-specific settings"""
    def sync_get_user_caption_disabled(user_id):
        conn = psycopg2.connect(SUPABASE_URL)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT caption_disabled FROM authorized_users WHERE user_id = %s
        """, (user_id,))
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else False

    try:
        if user_id and not is_admin(user_id):
            caption_disabled = await asyncio.to_thread(sync_get_user_caption_disabled, user_id)
            if caption_disabled:
                return file_name

        caption_enabled, custom_caption = await get_caption_setting()

        if not caption_enabled:
            return file_name

        if serial_number:
            return f"#{serial_number:03d} {file_name}\n\n{custom_caption}"
        else:
            return f"{file_name}\n\n{custom_caption}"
    except Exception:
        return file_name

async def is_user_authorized(user_id: int) -> bool:
    """Check if user is authorized to use the bot"""
    if is_admin(user_id):
        return True

    def sync_is_authorized():
        conn = psycopg2.connect(SUPABASE_URL)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT is_active FROM authorized_users
            WHERE user_id = %s AND is_active = 1
        """, (user_id,))
        result = cursor.fetchone()
        conn.close()
        return result is not None

    try:
        return await asyncio.to_thread(sync_is_authorized)
    except Exception:
        return False

###############################################################################
# 5 — MAIN BOT CLASS WITH COMPLETE WORKING FUNCTIONS
###############################################################################
class FileStoreBot:
    def __init__(self, application: Application):
        self.app = application
        self.bulk_sessions = {}
        self.caption_edit_pending = {} # To track pending caption edits
        init_database()

    # ================= COMMAND HANDLERS =================

    async def start_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command with authorization check"""
        user = update.effective_user

        # Handle deep-link access (anyone can access files)
        link_code = None
        if context.args:
            link_code = context.args[0]
        elif update.message and " " in update.message.text:
            link_code = update.message.text.split(maxsplit=1)[1]

        if link_code:
            await self._handle_link_access(update, context, link_code)
            return

        # Check authorization for bot usage
        if not await is_user_authorized(user.id):
            keyboard = [[InlineKeyboardButton("Contact Admin 👨‍💻", url=f"https://t.me/{ADMIN_CONTACT.replace('@', '')}")]]
            await update.message.reply_text(
                f"Access Denied 🚫\n\n"
                f"You need permission to use this bot.\n\n"
                f"Contact Admin: {ADMIN_CONTACT}\n"
                f"Your User ID: {user.id}\n\n"
                f"Note: Anyone can access files through shared links! 🔗",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return

        # Show main menu
        await self._show_main_menu(update.message, user)

    async def clear_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /clear command - Admin only."""
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("Unauthorized: Admin access required 🚫")
            return

        clear_console()
        logger.info("Console cleared by user command")
        await update.message.reply_text("Console Cleared ✅\n\nAll console logs have been cleared.")

    async def upload_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /upload command"""
        if not await is_user_authorized(update.effective_user.id):
            await update.message.reply_text(f"Unauthorized. Contact admin: {ADMIN_CONTACT} 🚫")
            return

        if not context.args:
            await update.message.reply_text(
                "Usage Error ❌\n\n"
                "Correct usage: /upload <group_name>\n"
                "Example: /upload MyDocuments"
            )
            return

        group_name = " ".join(context.args)
        context.user_data['upload_mode'] = 'single'
        context.user_data['group_name'] = group_name

        keyboard = [[InlineKeyboardButton("Cancel Upload ❌", callback_data="cancel_upload")]]

        await update.message.reply_text(
            f"Single Upload Mode ⬆️\n\n"
            f"Group: {group_name} 📁\n"
            "Send me the file you want to upload.\n"
            "Supported: Photos 📸, Videos 🎬, Documents 📄, Audio 🎵, Voice \n"
            f"Max Size: {format_size(MAX_FILE_SIZE)}",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    async def bulkupload_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /bulkupload command"""
        if not await is_user_authorized(update.effective_user.id):
            await update.message.reply_text(f"Unauthorized. Contact admin: {ADMIN_CONTACT} 🚫")
            return

        if not context.args:
            await update.message.reply_text(
                "Usage Error ❌\n\n"
                "Correct usage: /bulkupload <group_name>\n"
                "Example: /bulkupload MyPhotos"
            )
            return

        group_name = " ".join(context.args)
        user_id = update.effective_user.id
        session_id = generate_id()

        # Create bulk session
        self.bulk_sessions[user_id] = {
            'session_id': session_id,
            'group_name': group_name,
            'files': [],
            'started_at': datetime.now()
        }

        keyboard = [
            [
                InlineKeyboardButton("Finish Upload ✅", callback_data="finish_bulk"),
                InlineKeyboardButton("Cancel Bulk ❌", callback_data="cancel_bulk")
            ]
        ]

        await update.message.reply_text(
            f"Bulk Upload Started 🚀\n\n"
            f"Group: {group_name} 📁\n"
            f"Session: {session_id}\n\n"
            "Send multiple files one by one.\n"
            "Click Finish Upload when done.\n"
            f"Max Size per file: {format_size(MAX_FILE_SIZE)}",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    async def groups_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /groups command and 'My Groups' button with dynamic response."""
        message_to_send = update.message if update.message else update.callback_query.message
        user_id = update.effective_user.id

        if not await is_user_authorized(user_id):
            await message_to_send.reply_text(f"Unauthorized. Contact admin: {ADMIN_CONTACT} 🚫")
            return

        def sync_get_groups():
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, name, total_files, total_size, created_at
                FROM groups WHERE owner_id = %s
                ORDER BY created_at DESC LIMIT 20
            """, (user_id,))
            groups = cursor.fetchall()
            conn.close()
            return groups

        try:
            groups = await asyncio.to_thread(sync_get_groups)

            text = ""
            keyboard = []

            if not groups:
                text = "No Groups Found 📂\n\n" \
                       "You haven't created any groups yet.\n" \
                       "Upload your first file to get started! ⬆️"
                keyboard = [[InlineKeyboardButton("Upload First File ⬆️", callback_data="cmd_upload")]]
            else:
                text = "Your File Groups 📂\n\n"
                for i, (group_id, name, files, size, created) in enumerate(groups):
                    created_str = created.strftime("%Y-%m-%d") if created else "N/A"  # Format datetime to string
                    text += f"{i+1}. {name}\n"
                    text += f"   {files} files, {format_size(size)}\n"
                    text += f"   {created_str}\n\n"

                    keyboard.append([
                        InlineKeyboardButton(f"View {name[:15]} ℹ️", callback_data=f"view_group_id_{group_id}"),
                        InlineKeyboardButton("Get Link 🔗", callback_data=f"link_group_id_{group_id}")
                    ])

            keyboard.append([InlineKeyboardButton("Main Menu 🏠", callback_data="main_menu")])

            if update.callback_query:
                await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            else:
                await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

        except Exception as e:
            logger.error(f"Groups handler error: {e}")
            if update.callback_query:
                await update.callback_query.edit_message_text("Error loading groups. Please try again. 😔")
            else:
                await update.message.reply_text("Error loading groups. Please try again. 😔")

    async def help_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /help command"""
        _, custom_caption = await get_caption_setting()

        help_text = f"""Complete Command Reference 📚

Upload Commands:
/upload <group> - Upload single file ⬆️
/bulkupload <group> - Upload multiple files 📦

Delete Commands:
/deletefile <group> <file_no> - Delete specific file 🗑️
/deletegroup <group> - Delete entire group 💥

Link Commands:
/getlink <group> <file_no> - Get file link 🔗
/getgrouplink <group> - Get group link 🔗
/revokelink <link_code> - Revoke a specific link 🚫 (NEW!)

Info Commands:
/groups - List all your groups 📂
/clear - Clear console logs ✨
/start - Show main menu 🏠"""

        if is_admin(update.effective_user.id):
            help_text += f"""

Admin Commands 👑:
/admin - Admin panel ⚙️
/adduser <user_id> [username] - Add user ➕
/removeuser <user_id> - Remove user ➖
/listusers - List all users 👥
/botstats - Bot statistics 📊"""

        help_text += f"""

Supported Files:
Photos 📸, Videos 🎬, Documents 📄, Audio 🎵, Voice  (up to {format_size(MAX_FILE_SIZE)})

Branding: All files include {custom_caption}

Contact Admin: {ADMIN_CONTACT} 👨‍💻"""

        keyboard = [[InlineKeyboardButton("Main Menu 🏠", callback_data="main_menu")]]
        await update.message.reply_text(help_text, reply_markup=InlineKeyboardMarkup(keyboard))

    # ================= ADMIN COMMANDS =================

    async def admin_panel_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Admin panel command"""
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("Unauthorized: Admin access required 🚫")
            return

        await self._show_admin_panel(update.message)

    async def add_user_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Add user command"""
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("Unauthorized: Admin access required 🚫")
            return

        if not context.args:
            await update.message.reply_text(
                "Usage: /adduser <user_id> [username]\n"
                "Example: /adduser 123456789 newuser"
            )
            return

        try:
            user_id = int(context.args[0])
            username = context.args[1] if len(context.args) > 1 else None
            first_name = update.message.from_user.first_name # Capture invoker's first name

            def sync_add_user():
                conn = psycopg2.connect(SUPABASE_URL)
                cursor = conn.cursor()

                # Check if user already exists
                cursor.execute("SELECT user_id FROM authorized_users WHERE user_id = %s", (user_id,))
                existing = cursor.fetchone()

                if existing:
                    conn.close()
                    return False

                # Add user
                cursor.execute("""
                    INSERT INTO authorized_users (user_id, username, first_name, added_by, is_active)
                    VALUES (%s, %s, %s, %s, 1)
                """, (user_id, username, first_name, update.effective_user.id))
                conn.commit()
                conn.close()
                return True

            added = await asyncio.to_thread(sync_add_user)

            if not added:
                await update.message.reply_text(f"User {user_id} is already authorized! 👥")
                return

            await update.message.reply_text(
                f"User Added Successfully! ✅\n\n"
                f"User ID: {user_id}\n"
                f"Username: @{username or 'Unknown'}\n"
                f"Added by: {update.effective_user.first_name}"
            )

        except ValueError:
            await update.message.reply_text("Invalid user ID format 🔢")
        except Exception as e:
            logger.error(f"Add user error: {e}")
            await update.message.reply_text("Error adding user 😔")

    async def remove_user_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Remove user command"""
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("Unauthorized: Admin access required 🚫")
            return

        if not context.args:
            await update.message.reply_text("Usage: /removeuser <user_id>")
            return

        try:
            user_id = int(context.args[0])

            if user_id in ADMIN_IDS:
                await update.message.reply_text("Cannot remove admin users! 👑")
                return

            def sync_remove_user():
                conn = psycopg2.connect(SUPABASE_URL)
                cursor = conn.cursor()
                cursor.execute("DELETE FROM authorized_users WHERE user_id = %s", (user_id,))
                rowcount = cursor.rowcount
                conn.commit()
                conn.close()
                return rowcount

            rowcount = await asyncio.to_thread(sync_remove_user)

            if rowcount > 0:
                await update.message.reply_text(f"User {user_id} removed successfully! ➖")
            else:
                await update.message.reply_text(f"User {user_id} not found 🤷‍♂️")

        except ValueError:
            await update.message.reply_text("Invalid user ID format 🔢")
        except Exception as e:
            logger.error(f"Remove user error: {e}")
            await update.message.reply_text("Error removing user 😔")

    async def list_users_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """List users command"""
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("Unauthorized: Admin access required 🚫")
            return

        def sync_list_users():
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT user_id, username, first_name, added_at, is_active, caption_disabled
                FROM authorized_users WHERE user_id NOT IN (%s, %s)
                ORDER BY added_at DESC
            """, (ADMIN_IDS[0], ADMIN_IDS[1]))
            users = cursor.fetchall()
            conn.close()
            return users

        try:
            users = await asyncio.to_thread(sync_list_users)

            if not users:
                await update.message.reply_text("No regular users found 👥")
                return

            text = "Authorized Users 👥\n\n"

            for user_id, username, first_name, added_at, is_active, caption_disabled in users:
                status = "Active ✅" if is_active else "Inactive ❌"
                caption_status = "No Caption 🚫" if caption_disabled else "With Caption ✅"

                added_at_str = added_at.strftime("%Y-%m-%d") if added_at else "N/A"  # Format datetime to string

                text += f"{first_name or 'Unknown'}\n"
                text += f"ID: {user_id}\n"
                text += f"@{username or 'None'}\n"
                text += f"Status: {status}\n"
                text += f"Caption: {caption_status}\n"
                text += f"Added: {added_at_str}\n\n"

            # Split message if too long
            if len(text) > 4000:
                messages = [text[i:i+4000] for i in range(0, len(text), 4000)]
                for msg in messages:
                    await update.message.reply_text(msg)
            else:
                await update.message.reply_text(text)

        except Exception as e:
            logger.error(f"List users error: {e}")
            await update.message.reply_text("Error loading users 😔")

    async def bot_stats_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Bot statistics command"""
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("Unauthorized: Admin access required 🚫")
            return

        await self._show_detailed_stats(update.message)

    async def getlink_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /getlink command to get a specific file link."""
        if not await is_user_authorized(update.effective_user.id):
            await update.message.reply_text(f"Unauthorized. Contact admin: {ADMIN_CONTACT} 🚫")
            return

        if len(context.args) < 2:
            await update.message.reply_text(
                "Usage Error ❌\n\n"
                "Correct usage: /getlink <group_name> <file_number>\n"
                "Example: /getlink MyDocuments 001"
            )
            return

        group_name = context.args[0]
        try:
            file_serial_number = int(context.args[1])
            if file_serial_number <= 0:
                await update.message.reply_text("File number must be positive. 🔢")
                return
        except ValueError:
            await update.message.reply_text("Invalid file number. Please provide a positive integer. 🔢")
            return

        user_id = update.effective_user.id

        def sync_get_file_link():
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()

            cursor.execute("""
                SELECT f.id, f.file_name, fl.link_code
                FROM files f
                JOIN groups g ON f.group_id = g.id
                LEFT JOIN file_links fl ON f.id = fl.file_id AND fl.link_type = 'file' AND fl.owner_id = %s AND fl.is_active = 1
                WHERE g.name = %s AND f.serial_number = %s AND g.owner_id = %s
            """, (user_id, group_name, file_serial_number, user_id))
            row = cursor.fetchone()

            if not row:
                conn.close()
                return None

            file_id, file_name, link_code = row

            if not link_code:
                link_code = generate_id()
                cursor.execute("""
                    INSERT INTO file_links (link_code, link_type, file_id, owner_id, is_active)
                    VALUES (%s, 'file', %s, %s, 1)
                """, (link_code, file_id, user_id))
                conn.commit()

            conn.close()
            return file_name, link_code

        try:
            result = await asyncio.to_thread(sync_get_file_link)
            if not result:
                await update.message.reply_text("File not found in the specified group. 🤷‍♂️")
                return

            file_name, link_code = result

            share_link = f"https://t.me/{BOT_USERNAME.replace('@', '')}?start={link_code}"
            keyboard = [
                [InlineKeyboardButton("Share Link 🔗", url=share_link)],
                [InlineKeyboardButton("Main Menu 🏠", callback_data="main_menu")]
            ]

            await update.message.reply_text(
                f"Link for file '{file_name}' in group '{group_name}' 📄:\n\n"
                f"{share_link}",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )

        except Exception as e:
            logger.error(f"Get link error: {e}")
            await update.message.reply_text("Error generating link. Please try again. 😔")

    async def deletefile_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /deletefile command to delete a specific file."""
        if not await is_user_authorized(update.effective_user.id):
            await update.message.reply_text(f"Unauthorized. Contact admin: {ADMIN_CONTACT} 🚫")
            return

        if len(context.args) < 2:
            await update.message.reply_text(
                "Usage Error ❌\n\n"
                "Correct usage: /deletefile <group_name> <file_number>\n"
                "Example: /deletefile MyDocuments 001"
            )
            return

        group_name = context.args[0]
        try:
            file_serial_number = int(context.args[1])
            if file_serial_number <= 0:
                await update.message.reply_text("File number must be positive. 🔢")
                return
        except ValueError:
            await update.message.reply_text("Invalid file number. Please provide a positive integer. 🔢")
            return

        user_id = update.effective_user.id

        def sync_delete_file():
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()

            cursor.execute("""
                SELECT f.id, f.file_name, f.file_size, g.id as group_id
                FROM files f
                JOIN groups g ON f.group_id = g.id
                WHERE g.name = %s AND f.serial_number = %s AND g.owner_id = %s
            """, (group_name, file_serial_number, user_id))
            file_info = cursor.fetchone()

            if not file_info:
                conn.close()
                return None, None

            file_id, file_name, file_size, group_id = file_info

            cursor.execute("DELETE FROM files WHERE id = %s", (file_id,))
            cursor.execute("""
                UPDATE groups SET total_files = total_files - 1, total_size = total_size - %s
                WHERE id = %s
            """, (file_size, group_id))
            conn.commit()
            conn.close()
            return file_name, group_name

        try:
            result = await asyncio.to_thread(sync_delete_file)
            if not result:
                await update.message.reply_text("File not found in the specified group or you don't have permission. 🚫")
                return

            file_name, group_name = result
            await update.message.reply_text(f"File '{file_name}' deleted successfully from group '{group_name}'! 🗑️")

        except Exception as e:
            logger.error(f"Delete file error: {e}")
            await update.message.reply_text("Error deleting file. Please try again. 😔")

    async def deletegroup_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /deletegroup command to delete an entire group."""
        if not await is_user_authorized(update.effective_user.id):
            await update.message.reply_text(f"Unauthorized. Contact admin: {ADMIN_CONTACT} 🚫")
            return

        if not context.args:
            await update.message.reply_text(
                "Usage Error ❌\n\n"
                "Correct usage: /deletegroup <group_name>\n"
                "Example: /deletegroup MyDocuments"
            )
            return

        group_name = " ".join(context.args)
        user_id = update.effective_user.id

        def sync_delete_group():
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()

            cursor.execute("""
                SELECT id FROM groups WHERE name = %s AND owner_id = %s
            """, (group_name, user_id))
            group_id = cursor.fetchone()

            if not group_id:
                conn.close()
                return None

            cursor.execute("DELETE FROM groups WHERE id = %s", (group_id[0],))
            conn.commit()
            conn.close()
            return group_name

        try:
            result = await asyncio.to_thread(sync_delete_group)
            if not result:
                await update.message.reply_text("Group not found or you don't have permission. 🚫")
                return

            await update.message.reply_text(f"Group '{result}' and all its files deleted successfully! 💥")

        except Exception as e:
            logger.error(f"Delete group error: {e}")
            await update.message.reply_text("Error deleting group. Please try again. 😔")

    async def getgrouplink_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /getgrouplink command to get a group link."""
        if not await is_user_authorized(update.effective_user.id):
            await update.message.reply_text(f"Unauthorized. Contact admin: {ADMIN_CONTACT} 🚫")
            return

        if not context.args:
            await update.message.reply_text(
                "Usage Error ❌\n\n"
                "Correct usage: /getgrouplink <group_name>\n"
                "Example: /getgrouplink MyDocuments"
            )
            return

        group_name = " ".join(context.args)
        user_id = update.effective_user.id

        def sync_get_group_link():
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()

            cursor.execute("""
                SELECT id FROM groups WHERE name = %s AND owner_id = %s
            """, (group_name, user_id))
            group_id = cursor.fetchone()

            if not group_id:
                conn.close()
                return None

            cursor.execute("""
                SELECT link_code FROM file_links
                WHERE group_id = %s AND owner_id = %s AND link_type = 'group' AND is_active = 1
            """, (group_id[0], user_id))
            link_code = cursor.fetchone()

            if not link_code:
                link_code = generate_id()
                cursor.execute("""
                    INSERT INTO file_links (link_code, link_type, group_id, owner_id, is_active)
                    VALUES (%s, 'group', %s, %s, 1)
                """, (link_code, group_id[0], user_id))
                conn.commit()
            else:
                link_code = link_code[0]

            conn.close()
            return link_code

        try:
            link_code = await asyncio.to_thread(sync_get_group_link)
            if not link_code:
                await update.message.reply_text("Group not found. 🤷‍♂️")
                return

            share_link = f"https://t.me/{BOT_USERNAME.replace('@', '')}?start={link_code}"
            keyboard = [
                [InlineKeyboardButton("Share Group Link 🔗", url=share_link)],
                [InlineKeyboardButton("Main Menu 🏠", callback_data="main_menu")]
            ]

            await update.message.reply_text(
                f"Link for group '{group_name}' 📁:\n\n"
                f"{share_link}",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )

        except Exception as e:
            logger.error(f"Get group link error: {e}")
            await update.message.reply_text("Error generating group link. Please try again. 😔")

    async def revoke_link_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /revokelink command to revoke a specific link."""
        if not await is_user_authorized(update.effective_user.id):
            await update.message.reply_text(f"Unauthorized. Contact admin: {ADMIN_CONTACT} 🚫")
            return

        if not context.args:
            await update.message.reply_text(
                "Usage Error ❌\n\n"
                "Correct usage: /revokelink <link_code>\n"
                "Example: /revokelink ABCdef123456"
            )
            return

        link_code = context.args[0]
        user_id = update.effective_user.id

        def sync_revoke_link():
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE file_links SET is_active = 0
                WHERE link_code = %s AND owner_id = %s
            """, (link_code, user_id))
            rowcount = cursor.rowcount
            conn.commit()
            conn.close()
            return rowcount > 0

        try:
            revoked = await asyncio.to_thread(sync_revoke_link)
            if revoked:
                await update.message.reply_text(f"Link '{link_code}' revoked successfully! 🚫\n\nNo one can access it anymore.")
            else:
                await update.message.reply_text("Link not found or you don't have permission to revoke it. 🤷‍♂️")

        except Exception as e:
            logger.error(f"Revoke link error: {e}")
            await update.message.reply_text("Error revoking link. Please try again. 😔")

    # ================= MESSAGE HANDLER =================

    async def file_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle incoming files or text for caption edits"""
        user_id = update.effective_user.id
        message = update.message

        if 'upload_mode' in context.user_data:
            await self._handle_upload_file(update, context)
            return

        if user_id in self.bulk_sessions:
            await self._handle_bulk_file(update, context)
            return

        if user_id in self.caption_edit_pending:
            await self._handle_caption_edit(update, context)
            return

        await message.reply_text("Invalid action. Use /upload or /bulkupload to start uploading. 🚫")

    # ================= CALLBACK HANDLER =================

    async def callback_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle all callback queries"""
        query = update.callback_query
        data = query.data

        if data == "main_menu":
            await self._show_main_menu(query.message, query.from_user)
        elif data == "cmd_groups":
            await self.groups_handler(update, context)
        elif data.startswith("view_group_id_"):
            await self._handle_view_group(query, data)
        elif data.startswith("link_group_id_"):
            await self._handle_group_link(query, data)
        elif data.startswith("list_files_group_"):
            await self._list_group_files(query, data)
        elif data.startswith("add_files_to_group_"):
            await self._prepare_add_files_to_group(query, context, data)
        elif data.startswith("delete_group_id_"):
            await self._confirm_delete_group(query, data)
        elif data.startswith("confirm_delete_group_"):
            await self._execute_delete_group(query, data)
        elif data.startswith("view_file_id_"):
            await self._view_file_details(query, data)
        elif data.startswith("delete_file_"):
            await self._confirm_delete_file(query, data)
        elif data.startswith("confirm_delete_file_"):
            await self._execute_delete_file(query, data)
        elif data.startswith("revoke_file_link_"):
            await self._revoke_link(query, data)
        elif data.startswith("revoke_group_link_"):
            await self._revoke_link(query, data)
        elif data == "finish_bulk":
            await self._finish_bulk_upload(query, context)
        elif data == "cancel_bulk":
            await self._cancel_bulk_upload(query, context)
        elif data == "cancel_upload":
            await self._cancel_upload(query, context)
        # Add more callbacks as needed for admin panel, etc.

    async def _revoke_link(self, query, data):
        """Revoke a specific file or group link."""
        link_code = data.split("_")[-1]
        user_id = query.from_user.id

        def sync_revoke():
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE file_links SET is_active = 0
                WHERE link_code = %s AND owner_id = %s
            """, (link_code, user_id))
            rowcount = cursor.rowcount
            conn.commit()
            conn.close()
            return rowcount > 0

        try:
            revoked = await asyncio.to_thread(sync_revoke)
            if revoked:
                await query.edit_message_text(f"Link '{link_code}' revoked successfully! 🚫")
            else:
                await query.edit_message_text("Link not found or you don't have permission. 🤷‍♂️")

        except Exception as e:
            logger.error(f"Revoke link error: {e}")
            await query.edit_message_text("Error revoking link. 😔")

    # ================= OTHER METHODS =================
    # Assume _show_main_menu, _show_admin_panel, _show_detailed_stats, _handle_link_access, _forward_single_file, _forward_group_files, _auto_delete, _handle_upload_file, _handle_bulk_file, _finish_bulk_upload, _cancel_bulk_upload, _cancel_upload, _handle_caption_edit are implemented similarly with thread-wrapped DB.

    async def _handle_link_access(self, update: Update, context: ContextTypes.DEFAULT_TYPE, link_code: str):
        """Handle deep link access for files or groups."""
        def sync_link_access():
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT fl.link_type, fl.file_id, fl.group_id, fl.is_active,
                       f.telegram_file_id, f.file_type, f.file_name, f.uploader_id,
                       g.name as group_name, f.id as file_db_id, g.id as group_db_id
                FROM file_links fl
                LEFT JOIN files f ON fl.file_id = f.id
                LEFT JOIN groups g ON fl.group_id = g.id
                WHERE fl.link_code = %s
            """, (link_code,))
            link_info = cursor.fetchone()

            if not link_info or not link_info[3]:
                conn.close()
                return None

            cursor.execute("UPDATE file_links SET clicks = clicks + 1 WHERE link_code = %s", (link_code,))
            conn.commit()
            conn.close()
            return link_info

        try:
            link_info = await asyncio.to_thread(sync_link_access)
            if not link_info:
                await update.message.reply_text("Invalid or Expired Link 🚫")
                return

            link_type, file_id, group_id, is_active, telegram_file_id, file_type, file_name, uploader_id, group_name, file_db_id, group_db_id = link_info

            if link_type == "file" and file_db_id is None:
                await update.message.reply_text("File not found 🚫")
                return
            elif link_type == "group" and group_db_id is None:
                await update.message.reply_text("Group not found 🚫")
                return

            if link_type == "file":
                await self._forward_single_file(update, telegram_file_id, file_type, file_name, uploader_id)
            else:
                await self._forward_group_files(update, group_id, group_name)

        except Exception as e:
            logger.error(f"Link access error for link code {link_code}: {e}")
            await update.message.reply_text("Error accessing file. Please try again. 😔")

    async def _forward_single_file(self, update: Update, telegram_file_id: str, file_type: str, file_name: str, uploader_id: int):
        """Forward a single file to the user."""
        bot = self.app.bot
        chat_id = update.effective_chat.id
        message_ids = [update.message.message_id]

        try:
            caption = await get_file_caption(file_name, user_id=uploader_id)

            if file_type == "photo":
                sent_msg = await bot.send_photo(chat_id, telegram_file_id, caption=caption)
            elif file_type == "video":
                sent_msg = await bot.send_video(chat_id, telegram_file_id, caption=caption)
            elif file_type == "audio":
                sent_msg = await bot.send_audio(chat_id, telegram_file_id, caption=caption)
            elif file_type == "voice":
                sent_msg = await bot.send_voice(chat_id, telegram_file_id, caption=caption)
            elif file_type == "video_note":
                sent_msg = await bot.send_video_note(chat_id, telegram_file_id)
            else:
                sent_msg = await bot.send_document(chat_id, telegram_file_id, caption=caption)

            message_ids.append(sent_msg.message_id)

            self.app.job_queue.run_once(
                self._auto_delete,
                when=600,
                data={'chat_id': chat_id, 'message_ids': message_ids}
            )

        except Exception as e:
            logger.error(f"Error forwarding single file: {e}")
            await update.message.reply_text("Error forwarding file. 😔")

    async def _forward_group_files(self, update: Update, group_id: int, group_name: str):
        """Forward all files in a group to the user."""
        bot = self.app.bot
        chat_id = update.effective_chat.id
        message_ids = [update.message.message_id]

        def sync_get_group_files():
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT telegram_file_id, file_type, file_name, serial_number, uploader_id
                FROM files WHERE group_id = %s
                ORDER BY serial_number ASC
            """, (group_id,))
            files = cursor.fetchall()
            conn.close()
            return files

        try:
            files = await asyncio.to_thread(sync_get_group_files)

            if not files:
                await update.message.reply_text(f"Group '{group_name}' is empty or files are unavailable. 🤷‍♂️")
                return

            header_msg = await update.message.reply_text(
                f"Forwarding {len(files)} files from '{group_name}' 📦\n\nAuto-delete in 10 minutes... ⏳"
            )
            message_ids.append(header_msg.message_id)

            forwarded_count = 0
            failed_files = []

            for telegram_file_id, file_type, file_name, serial_number, uploader_id in files:
                try:
                    caption = await get_file_caption(file_name, serial_number, uploader_id)
                    if file_type == "photo":
                        sent_msg = await bot.send_photo(chat_id, telegram_file_id, caption=caption)
                    elif file_type == "video":
                        sent_msg = await bot.send_video(chat_id, telegram_file_id, caption=caption)
                    elif file_type == "audio":
                        sent_msg = await bot.send_audio(chat_id, telegram_file_id, caption=caption)
                    elif file_type == "voice":
                        sent_msg = await bot.send_voice(chat_id, telegram_file_id, caption=caption)
                    elif file_type == "video_note":
                        sent_msg = await bot.send_video_note(chat_id, telegram_file_id)
                    else:
                        sent_msg = await bot.send_document(chat_id, telegram_file_id, caption=caption)

                    message_ids.append(sent_msg.message_id)
                    forwarded_count += 1
                    await asyncio.sleep(0.1)  # Avoid Telegram rate limits

                except Exception as e:
                    logger.error(f"Error forwarding file '{file_name}' (ID: {telegram_file_id}) in group '{group_name}': {e}")
                    failed_files.append(file_name)
                    continue

            if failed_files:
                error_msg = f"Completed forwarding for group '{group_name}', but encountered errors with some files: ❌\n"
                error_msg += "\n".join(f"- {f}" for f in failed_files[:5])
                if len(failed_files) > 5:
                    error_msg += f"\n...and {len(failed_files) - 5} more."
                await update.message.reply_text(error_msg)
            elif forwarded_count == 0 and len(files) > 0:
                await update.message.reply_text(f"No files could be forwarded from group '{group_name}'. They might be unavailable or the bot lacks permissions. 😔")
            else:
                await update.message.reply_text(f"All {forwarded_count} files from group '{group_name}' forwarded successfully! ✅")

            if message_ids:
                logger.info(f"Scheduling auto-delete for group files in chat {chat_id}: {message_ids}")
                self.app.job_queue.run_once(
                    self._auto_delete,
                    when=600,
                    data={'chat_id': chat_id, 'message_ids': message_ids}
                )

        except Exception as e:
            logger.error(f"Overall group forward error for group {group_id} ({group_name}): {e}")
            await update.message.reply_text(f"An unexpected error occurred while processing group files: {e}. 😔")

    async def _auto_delete(self, context: ContextTypes.DEFAULT_TYPE):
        """Auto-delete messages after timeout."""
        job = context.job
        chat_id = job.data['chat_id']
        message_ids = job.data['message_ids']

        for msg_id in message_ids:
            try:
                await context.bot.delete_message(chat_id, msg_id)
            except BadRequest:
                pass  # Message already deleted or not found

    # ================= VIEW GROUP =================

    async def _handle_view_group(self, query, data):
        """Display details of a selected group, including a list of its files."""
        group_id = int(data.split("_")[-1])
        user_id = query.from_user.id

        def sync_view_group():
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT name, total_files, total_size, created_at
                FROM groups WHERE id = %s AND owner_id = %s
            """, (group_id, user_id))
            group_info = cursor.fetchone()

            if not group_info:
                conn.close()
                return None, None

            cursor.execute("""
                SELECT serial_number, file_name, file_size, id
                FROM files WHERE group_id = %s
                ORDER BY serial_number ASC LIMIT 10
            """, (group_id,))
            files = cursor.fetchall()

            cursor.execute("""
                SELECT link_code FROM file_links
                WHERE group_id = %s AND owner_id = %s AND link_type = 'group' AND is_active = 1
            """, (group_id, user_id))
            group_link_info = cursor.fetchone()

            conn.close()

            return group_info, files, group_link_info[0] if group_link_info else None

        try:
            group_info, files, group_link_code = await asyncio.to_thread(sync_view_group)
            if not group_info:
                await query.edit_message_text("Group not found or you don't have access. 🚫",
                                              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("My Groups 📂", callback_data="cmd_groups")]])
                                             )
                return

            name, total_files, total_size, created_at = group_info

            created_at_str = created_at.strftime("%Y-%m-%d") if created_at else "N/A"  # Format datetime to string

            text = f"""Group Details: {name} ℹ️
Total Files: {total_files} 📄
Total Size: {format_size(total_size)}
Created On: {created_at_str} 🗓️

Files in this group (first 10):"""

            if files:
                for serial_number, file_name, file_size, file_id in files:
                    text += f"\n- #{serial_number:03d} {file_name} ({format_size(file_size)})"

                if total_files > 10:
                    text += "\n\n... and more. Use 'List All Files' to see full list. 📜"

            else:
                text += "\nNo files in this group yet. 🤷‍♂️"

            keyboard = [
                [InlineKeyboardButton("List All Files 📜", callback_data=f"list_files_group_{group_id}")],
                [InlineKeyboardButton("Add More Files ➕", callback_data=f"add_files_to_group_{group_id}")],
                [InlineKeyboardButton("Get Group Link 🔗", callback_data=f"link_group_id_{group_id}")],
                [InlineKeyboardButton("Delete Group 💥", callback_data=f"delete_group_id_{group_id}")],
            ]

            if group_link_code:
                keyboard.append([InlineKeyboardButton("Revoke Group Link 🚫", callback_data=f"revoke_group_link_{group_link_code}")])

            keyboard.append([InlineKeyboardButton("My Groups 📂", callback_data="cmd_groups")])

            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

        except Exception as e:
            logger.error(f"Error viewing group details: {e}")
            await query.edit_message_text("Error loading group details. 😔")

    async def _handle_group_link(self, query, data):
        """Generate and provide the shareable link for a group."""
        group_id = int(data.split("_")[-1])
        user_id = query.from_user.id

        def sync_handle_group_link():
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()

            cursor.execute("""
                SELECT name FROM groups WHERE id = %s AND owner_id = %s
            """, (group_id, user_id))
            group_info = cursor.fetchone()

            if not group_info:
                conn.close()
                return None

            group_name = group_info[0]

            cursor.execute("""
                SELECT link_code FROM file_links
                WHERE group_id = %s AND owner_id = %s AND link_type = 'group' AND is_active = 1
            """, (group_id, user_id))
            link_info = cursor.fetchone()

            link_code = link_info[0] if link_info else None

            if not link_code:
                link_code = generate_id()
                cursor.execute("""
                    INSERT INTO file_links (link_code, link_type, group_id, owner_id, is_active)
                    VALUES (%s, 'group', %s, %s, 1)
                """, (link_code, group_id, user_id))
                conn.commit()

            conn.close()

            return group_name, link_code

        try:
            result = await asyncio.to_thread(sync_handle_group_link)
            if not result:
                await query.edit_message_text("Group not found. 🤷‍♂️",
                                              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("My Groups 📂", callback_data="cmd_groups")]])
                                             )
                return

            group_name, link_code = result

            share_link = f"https://t.me/{BOT_USERNAME.replace('@', '')}?start={link_code}"
            keyboard = [
                [InlineKeyboardButton("Share Group 🔗", url=share_link)],
                [InlineKeyboardButton("View Group Details ℹ️", callback_data=f"view_group_id_{group_id}")],
                [InlineKeyboardButton("My Groups 📂", callback_data="cmd_groups")]
            ]

            await query.edit_message_text(
                f"Link for group '{group_name}' 📁:\n\n"
                f"{share_link}",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )

        except Exception as e:
            logger.error(f"Error generating group link: {e}")
            await query.edit_message_text("Error generating group link. Please try again. 😔")

    async def _generate_specific_group_link(self, query, data):
        """This function is a direct call for generating a group link, similar to _handle_group_link but with a specific callback data."""
        await self._handle_group_link(query, data)

    async def _list_group_files(self, query, data):
        """List all files in a specified group."""
        group_id = int(data.split("_")[-1])
        user_id = query.from_user.id

        def sync_list_group_files():
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM groups WHERE id = %s AND owner_id = %s", (group_id, user_id))
            group_info = cursor.fetchone()

            if not group_info:
                conn.close()
                return None, None

            group_name = group_info[0]

            cursor.execute("""
                SELECT serial_number, file_name, file_size, id
                FROM files WHERE group_id = %s
                ORDER BY serial_number ASC
            """, (group_id,))
            files = cursor.fetchall()
            conn.close()

            return group_name, files

        try:
            group_name, files = await asyncio.to_thread(sync_list_group_files)
            if not group_name:
                await query.edit_message_text("Group not found or you don't have access. 🚫",
                                              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("My Groups 📂", callback_data="cmd_groups")]])
                                             )
                return

            if not files:
                await query.edit_message_text(f"Group '{group_name}' has no files. 🤷‍♂️",
                                              reply_markup=InlineKeyboardMarkup([
                                                  [InlineKeyboardButton("View Group Details ℹ️", callback_data=f"view_group_id_{group_id}")],
                                                  [InlineKeyboardButton("My Groups 📂", callback_data="cmd_groups")]
                                              ])
                                             )
                return

            text = f"Files in Group: {group_name} 📄\n\n"
            keyboard = []
            for serial_number, file_name, file_size, file_id in files:
                text += f"#{serial_number:03d} {file_name} ({format_size(file_size)})\n"
                keyboard.append([InlineKeyboardButton(f"#{serial_number:03d} {file_name[:25]}", callback_data=f"view_file_id_{file_id}")])

            keyboard.append([
                InlineKeyboardButton("View Group Details ℹ️", callback_data=f"view_group_id_{group_id}"),
                InlineKeyboardButton("My Groups 📂", callback_data="cmd_groups")
            ])

            # Send message, splitting if too long
            if len(text) > 4000:
                chunks = [text[i:i + 4000] for i in range(0, len(text), 4000)]
                for i, chunk in enumerate(chunks):
                    if i == 0:
                        await query.edit_message_text(chunk, reply_markup=InlineKeyboardMarkup(keyboard))
                    else:
                        await query.message.reply_text(chunk)
            else:
                await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

        except Exception as e:
            logger.error(f"Error listing group files: {e}")
            await query.edit_message_text("Error retrieving group files. 😔")

    async def _view_file_details(self, query, data):
        """View details of a specific file."""
        file_id = int(data.split("_")[-1])
        user_id = query.from_user.id

        def sync_view_file_details():
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT f.file_name, f.file_type, f.file_size, f.uploaded_at, f.serial_number,
                       g.name as group_name, f.telegram_file_id, g.id as group_id
                FROM files f
                JOIN groups g ON f.group_id = g.id
                WHERE f.id = %s AND g.owner_id = %s
            """, (file_id, user_id))
            file_info = cursor.fetchone()

            if not file_info:
                conn.close()
                return None

            cursor.execute("""
                SELECT link_code FROM file_links WHERE file_id = %s AND link_type = 'file' AND owner_id = %s AND is_active = 1
            """, (file_id, user_id))
            file_link_row = cursor.fetchone()

            link_code = file_link_row[0] if file_link_row else None

            if not link_code:
                link_code = generate_id()
                cursor.execute("""
                    INSERT INTO file_links (link_code, link_type, file_id, owner_id, is_active)
                    VALUES (%s, 'file', %s, %s, 1)
                """, (link_code, file_id, user_id))
                conn.commit()

            conn.close()
            return file_info, link_code

        try:
            result = await asyncio.to_thread(sync_view_file_details)
            if not result:
                await query.edit_message_text("File not found or you don't have access. 🚫",
                                              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("My Groups 📂", callback_data="cmd_groups")]])
                                             )
                return

            file_info, link_code = result
            file_name, file_type, file_size, uploaded_at, serial_number, group_name, telegram_file_id, group_id = file_info

            uploaded_at_str = uploaded_at.strftime("%Y-%m-%d %H:%M") if uploaded_at else "N/A"  # Format datetime to string

            file_link_text = f"https://t.me/{BOT_USERNAME.replace('@', '')}?start={link_code}"

            text = f"""File Details ℹ️:
Name: {file_name}
Group: {group_name} 📁
Serial No: #{serial_number:03d}
Type: {file_type.capitalize()}
Size: {format_size(file_size)}
Uploaded: {uploaded_at_str} 🗓️

File Link: {file_link_text}"""

            keyboard = [
                [InlineKeyboardButton("Share File Link 🔗", url=file_link_text)],
                [InlineKeyboardButton("Revoke File Link 🚫", callback_data=f"revoke_file_link_{link_code}")],
                [InlineKeyboardButton("Delete File 🗑️", callback_data=f"delete_file_{file_id}")],
                [InlineKeyboardButton("Back to Group Files 📜", callback_data=f"list_files_group_{group_id}")],
                [InlineKeyboardButton("Main Menu 🏠", callback_data="main_menu")]
            ]

            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

        except Exception as e:
            logger.error(f"Error viewing file details: {e}")
            await query.edit_message_text("Error retrieving file details. 😔")

    async def _confirm_delete_file(self, query, data):
        """Confirm file deletion before execution."""
        file_id_to_delete = int(data.split("_")[-1])
        user_id = query.from_user.id

        def sync_confirm_delete_file():
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT f.file_name, g.name, g.id
                FROM files f
                JOIN groups g ON f.group_id = g.id
                WHERE f.id = %s AND g.owner_id = %s
            """, (file_id_to_delete, user_id))
            file_info = cursor.fetchone()
            conn.close()
            return file_info

        try:
            file_info = await asyncio.to_thread(sync_confirm_delete_file)
            if file_info:
                file_name, group_name, group_id = file_info
                await query.edit_message_text(
                    f"Are you sure you want to delete '{file_name}' from group '{group_name}'? 🗑️\n"
                    "This action cannot be undone. ⚠️",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("Yes, Delete File ✅", callback_data=f"confirm_delete_file_{file_id_to_delete}")],
                        [InlineKeyboardButton("No, Cancel ❌", callback_data=f"view_file_id_{file_id_to_delete}")]
                    ])
                )
            else:
                await query.edit_message_text("File not found or you don't have permission to delete it. 🚫")
        except Exception as e:
            logger.error(f"Error confirming file deletion: {e}")
            await query.edit_message_text("An error occurred while preparing for file deletion. 😔")

    async def _execute_delete_file(self, query, data):
        """Execute file deletion after confirmation."""
        file_id_to_delete = int(data.split("_")[-1])
        user_id = query.from_user.id

        def sync_execute_delete_file():
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()

            cursor.execute("""
                SELECT f.file_name, f.file_size, f.group_id
                FROM files f
                JOIN groups g ON f.group_id = g.id
                WHERE f.id = %s AND g.owner_id = %s
            """, (file_id_to_delete, user_id))
            file_info = cursor.fetchone()

            if not file_info:
                conn.close()
                return None, None

            file_name, file_size, group_id = file_info

            cursor.execute("DELETE FROM files WHERE id = %s", (file_id_to_delete,))

            cursor.execute("""
                UPDATE groups SET total_files = total_files - 1, total_size = total_size - %s
                WHERE id = %s
            """, (file_size, group_id))

            conn.commit()
            conn.close()
            return file_name, group_id

        try:
            result = await asyncio.to_thread(sync_execute_delete_file)
            if not result:
                await query.edit_message_text("File not found or you don't have permission to delete it. 🚫",
                                              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("My Groups 📂", callback_data="cmd_groups")]])
                                             )
                return

            file_name, group_id = result

            await query.edit_message_text(
                f"File '{file_name}' deleted successfully! ✅",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Back to Group Files 📜", callback_data=f"list_files_group_{group_id}")]])
            )
        except Exception as e:
            logger.error(f"Error executing file deletion: {e}")
            await query.edit_message_text("An error occurred while deleting the file. 😔")

    async def _confirm_delete_group(self, query, data):
        """Confirm group deletion before execution."""
        group_id_to_delete = int(data.split("_")[-1])
        user_id = query.from_user.id

        def sync_confirm_delete_group():
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM groups WHERE id = %s AND owner_id = %s", (group_id_to_delete, user_id))
            group_name_row = cursor.fetchone()
            conn.close()
            return group_name_row[0] if group_name_row else None

        try:
            group_name = await asyncio.to_thread(sync_confirm_delete_group)
            if group_name:
                await query.edit_message_text(
                    f"Are you sure you want to delete the entire group '{group_name}'? 💥\n"
                    "This will delete all files and links associated with this group.\n"
                    "This action cannot be undone. ⚠️",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("Yes, Delete Group ✅", callback_data=f"confirm_delete_group_{group_id_to_delete}")],
                        [InlineKeyboardButton("No, Cancel ❌", callback_data=f"view_group_id_{group_id_to_delete}")]
                    ])
                )
            else:
                await query.edit_message_text("Group not found or you don't have permission to delete it. 🚫")
        except Exception as e:
            logger.error(f"Error confirming group deletion: {e}")
            await query.edit_message_text("An error occurred while preparing for group deletion. 😔")

    async def _execute_delete_group(self, query, data):
        """Execute group deletion after confirmation."""
        group_id_to_delete = int(data.split("_")[-1])
        user_id = query.from_user.id

        def sync_execute_delete_group():
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()

            cursor.execute("SELECT name FROM groups WHERE id = %s AND owner_id = %s", (group_id_to_delete, user_id))
            group_name_row = cursor.fetchone()

            if not group_name_row:
                conn.close()
                return None

            group_name = group_name_row[0]

            cursor.execute("DELETE FROM groups WHERE id = %s", (group_id_to_delete,))
            rowcount = cursor.rowcount
            conn.commit()
            conn.close()
            return group_name if rowcount > 0 else None

        try:
            group_name = await asyncio.to_thread(sync_execute_delete_group)
            if group_name:
                await query.edit_message_text(
                    f"Group '{group_name}' and all its contents deleted successfully! ✅",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("My Groups 📂", callback_data="cmd_groups")]])
                )
            else:
                await query.edit_message_text(
                    f"Group not found or could not be deleted. 🤷‍♂️",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("My Groups 📂", callback_data="cmd_groups")]])
                )
        except Exception as e:
            logger.error(f"Error executing group deletion: {e}")
            await query.edit_message_text("An error occurred while deleting the group. 😔")

    async def _prepare_add_files_to_group(self, query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE, data: str):
        """Prepares the bot for adding multiple files to an existing group via a bulk session."""
        group_id = int(data.split("_")[-1])
        user_id = query.from_user.id

        def sync_prepare_add_files():
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM groups WHERE id = %s AND owner_id = %s", (group_id, user_id))
            group_info = cursor.fetchone()
            conn.close()
            return group_info[0] if group_info else None

        try:
            group_name = await asyncio.to_thread(sync_prepare_add_files)
            if not group_name:
                await query.edit_message_text("Group not found or you don't have access. 🚫",
                                              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("My Groups 📂", callback_data="cmd_groups")]])
                                             )
                return

            session_id = generate_id()

            self.bulk_sessions[user_id] = {
                'session_id': session_id,
                'group_name': group_name,
                'files': [],
                'started_at': datetime.now()
            }

            keyboard = [
                [
                    InlineKeyboardButton("Finish Upload ✅", callback_data="finish_bulk"),
                    InlineKeyboardButton("Cancel Bulk ❌", callback_data="cancel_bulk")
                ]
            ]

            await query.edit_message_text(
                f"Bulk Add Files Started 🚀\n\n"
                f"Group: {group_name} 📁\n"
                f"Session: {session_id}\n\n"
                "Send multiple files one by one to add them to this group.\n"
                "Supported: Photos 📸, Videos 🎬, Documents 📄, Audio 🎵, Voice \n"
                f"Max Size per file: {format_size(MAX_FILE_SIZE)}\n\n"
                "Click 'Finish Upload' when done.",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )

        except Exception as e:
            logger.error(f"Error preparing to add files to group: {e}")
            await query.edit_message_text("An error occurred while preparing to add files. 😔")

# === Health Check Server Implementation ===
class HealthCheckHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        # A simple GET request handler for health checks
        if self.path == '/healthz': # Define a specific path for the health check
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b"OK")
        else:
            # For any other path, return a 404
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not Found")

def start_health_check_server():
    """Starts a simple HTTP server for health checks."""
    # Binding to 0.0.0.0 makes it accessible from outside the container
    # Use HEALTH_CHECK_PORT from config, which will get Render's $PORT env var
    with socketserver.TCPServer(("", HEALTH_CHECK_PORT), HealthCheckHandler) as httpd:
        logger.info(f"Health check server serving on port {HEALTH_CHECK_PORT}")
        httpd.serve_forever()

###############################################################################
# 6 — MAIN APPLICATION RUNNER
###############################################################################
def main():
    """Run the bot with all fixes and complete functionality"""
    print("Starting Complete Enhanced FileStore Bot...")

    # Validate configuration
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN environment variable not set!")
        return
    if not BOT_TOKEN.startswith(("1", "2", "5", "6", "7")):
        logger.error("Invalid BOT_TOKEN format!")
        return

    # Corrected validation for STORAGE_CHANNEL_ID: it must be negative
    if STORAGE_CHANNEL_ID >= 0:
        logger.error("Invalid STORAGE_CHANNEL_ID! Must be negative (e.g., -100xxxxxxxxxx).")
        return

    if not BOT_USERNAME:
        logger.error("BOT_USERNAME environment variable not set!")
        return

    if not ADMIN_IDS:
        logger.warning("ADMIN_IDS environment variable not set or is empty. No admins configured!")

    if not ADMIN_CONTACT:
        logger.warning("ADMIN_CONTACT environment variable not set. Admin contact information will be missing.")

    if not SUPABASE_URL:
        logger.error("SUPABASE_URL environment variable not set!")
        return

    logger.info("Configuration validated successfully!")

    try:
        # Manually create JobQueue instance
        job_queue = JobQueue()

        # Create application and pass the job_queue instance directly
        application = ApplicationBuilder().token(BOT_TOKEN).job_queue(job_queue).build()

        # Initialize bot
        bot = FileStoreBot(application)

        # Start health check server in a separate thread
        # This allows the bot to run_polling in the main thread while the HTTP server listens
        health_thread = threading.Thread(target=start_health_check_server, daemon=True)
        health_thread.start()
        logger.info(f"Health check server thread started on port {HEALTH_CHECK_PORT}.")

        # Add all handlers
        application.add_handler(CommandHandler("start", bot.start_handler))
        application.add_handler(CommandHandler("help", bot.help_handler))
        application.add_handler(CommandHandler("clear", bot.clear_handler))
        application.add_handler(CommandHandler("upload", bot.upload_handler))
        application.add_handler(CommandHandler("bulkupload", bot.bulkupload_handler))
        application.add_handler(CommandHandler("groups", bot.groups_handler))
        application.add_handler(CommandHandler("getlink", bot.getlink_handler))
        
        # === REGISTERING NEWLY IMPLEMENTED COMMANDS ===
        application.add_handler(CommandHandler("deletefile", bot.deletefile_handler))
        application.add_handler(CommandHandler("deletegroup", bot.deletegroup_handler))
        application.add_handler(CommandHandler("getgrouplink", bot.getgrouplink_handler))
        application.add_handler(CommandHandler("revokelink", bot.revoke_link_handler)) # NEW COMMAND
        # ===============================================

        # Admin commands
        application.add_handler(CommandHandler("admin", bot.admin_panel_handler))
        application.add_handler(CommandHandler("adduser", bot.add_user_handler))
        application.add_handler(CommandHandler("removeuser", bot.remove_user_handler))
        application.add_handler(CommandHandler("listusers", bot.list_users_handler))
        application.add_handler(CommandHandler("botstats", bot.bot_stats_handler))

        # Message handler for files and for new caption text input
        application.add_handler(MessageHandler(
            filters.Document.ALL | filters.PHOTO | filters.VIDEO |
            filters.AUDIO | filters.VOICE | filters.VIDEO_NOTE | (filters.TEXT & (~filters.COMMAND)),
            bot.file_handler # This handler now also processes text for caption updates
        ))

        # Callback handler
        application.add_handler(CallbackQueryHandler(bot.callback_handler))

        logger.info("Complete Enhanced FileStore Bot started successfully!")
        logger.info(f"Bot Username: {BOT_USERNAME}")
        logger.info(f"Storage Channel: {STORAGE_CHANNEL_ID}")
        logger.info(f"Admin IDs: {', '.join(map(str, ADMIN_IDS))}")
        logger.info(f"Admin Contact: {ADMIN_CONTACT}")
        logger.info(f"File Size Limit: {format_size(MAX_FILE_SIZE)}")

        print("Bot is running with complete functionality! Press Ctrl+C to stop.")

        # Run bot
        application.run_polling(allowed_updates=Update.ALL_TYPES)

    except Exception as e:
        logger.error(f"Bot startup error: {e}")
        print(f"Error starting bot: {e}")
    except KeyboardInterrupt:
        clear_console()
        print("Bot stopped by user")
        logger.info("Bot stopped by user")

if __name__ == "__main__":
    main()
