# Telegram FileStore Bot (Supabase-Ready)

This is a Telegram bot that allows you to upload, organize, and share files with permanent links.  
It uses **Supabase (PostgreSQL)** as its database backend and stores files in a private Telegram channel.

---

## Features
- Upload single or multiple files to organized groups.
- Auto-generated serial numbers and captions.
- Generate permanent shareable links for files or groups.
- Admin panel with user management.
- Supports up to 2GB files.
- Works with Supabase/Postgres for persistence.

---

## Deployment

### 1. Clone the repository
```bash
git clone https://github.com/yourrepo/filestore-bot.git
cd filestore-bot
```

### 2. Set up environment
Copy `.env.example` → `.env` and fill in your credentials.

```bash
cp .env.example .env
```

### 3. Run locally
```bash
pip install -r requirements.txt
python filecloudsupabaseX.py
```

### 4. Run with Docker
```bash
docker build -t filestore-bot .
docker run --env-file .env -p 8000:8000 filestore-bot
```

---

## Environment Variables

- `BOT_TOKEN` — Telegram bot token from [BotFather](https://t.me/BotFather).
- `BOT_USERNAME` — Your bot’s username.
- `STORAGE_CHANNEL_ID` — Private Telegram channel ID for file storage.
- `ADMIN_IDS` — Comma-separated list of Telegram admin user IDs.
- `ADMIN_CONTACT` — Admin username (`@username`) shown to unauthorized users.
- `CUSTOM_CAPTION` — Optional caption appended to uploads.
- `SUPABASE_URL` — PostgreSQL connection string from Supabase.
- `PORT` — Port for health check server (default `8000`).

---

## Database
On first run, the bot automatically creates required tables in Supabase/Postgres:
- `authorized_users`
- `groups`
- `files`
- `file_links`
- `bot_settings`

---

## License
MIT
