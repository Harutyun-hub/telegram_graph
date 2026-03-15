# Telegram Session Setup for Railway Deployment

## Overview

This document explains how to set up persistent Telegram authentication for Railway.com deployment. The main challenge is that Railway containers are ephemeral - any files created at runtime (like session files) are lost when the container restarts.

## The Problem

- **Local Development**: Telethon creates a `telegram_scraper.session` file that persists on disk
- **Railway Deployment**: Session files are lost on every deployment/restart
- **Result**: The error you saw: `"Scraper error: Telegram session is not authorized"`

## The Solution

We now support **StringSession** - storing the session as an environment variable that persists across deployments.

## Setup Instructions

### Step 1: Generate Session String Locally

1. **Ensure you have your Telegram API credentials in `.env`**:
   ```bash
   TELEGRAM_API_ID=your_api_id
   TELEGRAM_API_HASH=your_api_hash
   TELEGRAM_PHONE=+1234567890  # Your phone with country code
   ```

2. **Run the session export script**:
   ```bash
   python scripts/export_telegram_session.py
   ```

3. **Authenticate**:
   - A QR code will appear in your terminal
   - Open Telegram app → Settings → Devices → Link Desktop Device
   - Scan the QR code
   - Tap "Confirm" in the app

4. **Copy the session string**:
   - The script will display a long base64 string
   - This is your `TELEGRAM_SESSION_STRING` value

### Step 2: Configure Railway Environment

1. **Go to Railway Dashboard**:
   - Navigate to your project
   - Click on your backend service
   - Go to the "Variables" tab

2. **Add the session string**:
   - Click "New Variable"
   - Name: `TELEGRAM_SESSION_STRING`
   - Value: [Paste the base64 string from Step 1]
   - Click "Add"

3. **Add other required variables** (if not already set):
   ```
   TELEGRAM_API_ID=your_api_id
   TELEGRAM_API_HASH=your_api_hash
   TELEGRAM_PHONE=+1234567890

   # Plus all other required vars:
   SUPABASE_URL=...
   SUPABASE_SERVICE_ROLE_KEY=...
   NEO4J_URI=...
   NEO4J_PASSWORD=...
   OPENAI_API_KEY=...
   ```

4. **Deploy**: Railway will automatically redeploy with the new variables

## How It Works

### Local Development (No Changes)
```python
# When TELEGRAM_SESSION_STRING is not set:
# Uses file-based session (telegram_scraper.session)
client = TelegramClient('telegram_scraper', api_id, api_hash)
```

### Railway Deployment (New)
```python
# When TELEGRAM_SESSION_STRING is set:
# Uses string session from environment variable
from telethon.sessions import StringSession
session_string = os.getenv('TELEGRAM_SESSION_STRING')
client = TelegramClient(StringSession(session_string), api_id, api_hash)
```

## File Structure

```
scripts/
└── export_telegram_session.py   # Generate session string

scraper/
└── session_manager.py           # Modified to support StringSession

config.py                        # Added TELEGRAM_SESSION_STRING variable
.gitignore                       # Ignores .env.telegram_session
```

## Security Considerations

⚠️ **CRITICAL SECURITY NOTES**:

1. **Session String = Full Account Access**
   - Treat it like a password
   - Anyone with this string can access your Telegram account
   - NEVER commit it to Git or share publicly

2. **Revocation**
   - To revoke access: Telegram App → Settings → Devices → Terminate session
   - Each new export invalidates previous session strings

3. **Best Practices**
   - Use a dedicated Telegram account for production
   - Regularly rotate session strings
   - Monitor active sessions in Telegram settings

## Troubleshooting

### Error: "Invalid TELEGRAM_SESSION_STRING"
- The session string may be expired or invalid
- Re-run `python scripts/export_telegram_session.py` locally
- Update the Railway environment variable with the new string

### Error: "Cannot authenticate interactively in cloud deployment"
- This means Railway is trying to show a QR code (impossible in cloud)
- You must set `TELEGRAM_SESSION_STRING` in Railway environment variables

### Session Works Locally but Not on Railway
- Ensure you copied the ENTIRE session string (it's long!)
- Check for any spaces or line breaks in the Railway variable
- Verify all Telegram-related env vars are set correctly

## Alternative: Disable Telegram Scraping

If you don't need Telegram data immediately, you can disable the scraper:

1. Add to Railway environment variables:
   ```
   SKIP_TELEGRAM_SCRAPER=true
   ```

2. The API and frontend will work without Telegram data

## Testing the Setup

After deployment, check Railway logs:

**Success**:
```
INFO: Using session from TELEGRAM_SESSION_STRING environment variable
SUCCESS: Session active — logged in as: YourName (@yourusername)
```

**Failure**:
```
ERROR: TELEGRAM_SESSION_STRING is invalid or expired!
ERROR: Please run locally: python scripts/export_telegram_session.py
```

## Questions?

If you encounter issues:
1. Check Railway deployment logs
2. Verify all environment variables are set
3. Ensure the session string was copied completely
4. Try generating a fresh session string

Remember: The session string approach is the **only** way to maintain Telegram authentication in ephemeral container environments like Railway.