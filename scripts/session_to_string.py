#!/usr/bin/env python3
"""
Proper conversion from file session to string session.
"""

import asyncio
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from telethon import TelegramClient
from telethon.sessions import StringSession
import config


async def file_to_string_session():
    """Convert file-based session to string session."""

    print("🔍 Checking existing session...")

    # First, connect with file session to get the auth data
    file_client = TelegramClient(
        "telegram_scraper_backup",
        config.TELEGRAM_API_ID,
        config.TELEGRAM_API_HASH
    )

    await file_client.connect()

    if not await file_client.is_user_authorized():
        print("❌ Existing session is not authorized")
        await file_client.disconnect()
        return None

    # Get user info
    me = await file_client.get_me()
    print(f"✅ Found valid session for: {me.first_name} (@{me.username})")

    # Now create a StringSession client with the same connection
    # This is the key: we need to export from a StringSession, not SQLiteSession
    string_client = TelegramClient(
        StringSession(),  # Empty string session
        config.TELEGRAM_API_ID,
        config.TELEGRAM_API_HASH
    )

    # Copy the session data
    string_client.session.set_dc(
        file_client.session.dc_id,
        file_client.session.server_address,
        file_client.session.port
    )
    string_client.session.auth_key = file_client.session.auth_key

    # Save the string session
    session_string = string_client.session.save()

    # Clean up
    await file_client.disconnect()

    print(f"✅ Converted to string session")
    print(f"📏 Session string: {len(session_string)} characters")

    return session_string


def save_and_display(session_string):
    """Save session string and show instructions."""

    # Save to file
    env_file = Path(".env.telegram_session")
    with open(env_file, "w") as f:
        f.write(f"# Telegram Session String for Railway Deployment\n")
        f.write(f"# Generated from existing session file\n")
        f.write(f"TELEGRAM_SESSION_STRING={session_string}\n")

    print("\n" + "=" * 80)
    print("🎉 SUCCESS! Session converted to string format")
    print("=" * 80)

    print("\n📋 FOR RAILWAY DEPLOYMENT:")
    print("-" * 80)

    print("\n1️⃣  Copy this to Railway environment variables:\n")
    print("   Name:  TELEGRAM_SESSION_STRING")
    print(f"   Value: {session_string}\n")

    print("2️⃣  Steps in Railway:")
    print("   • Go to your Railway project")
    print("   • Click on your service")
    print("   • Go to 'Variables' tab")
    print("   • Click 'New Variable'")
    print("   • Paste the name and value from above")
    print("   • Click 'Add'")
    print("   • Railway will auto-deploy")

    print(f"\n3️⃣  Session saved to: {env_file.absolute()}")
    print("   (This file is gitignored)")

    print("\n⚠️  IMPORTANT:")
    print("   • This session gives full access to your Telegram")
    print("   • Keep it secret like a password")
    print("   • Never commit to Git")
    print("   • To revoke: Telegram → Settings → Devices")

    print("\n✅ Your Railway deployment will now have persistent Telegram auth!")
    print("=" * 80)


async def main():
    print("\n" + "=" * 80)
    print("🔄 FILE TO STRING SESSION CONVERTER")
    print("=" * 80)
    print("\nConverting existing telegram_scraper.session to string format...")
    print("-" * 80)

    try:
        session_string = await file_to_string_session()

        if session_string:
            save_and_display(session_string)
        else:
            print("\n❌ Conversion failed")
            print("\nPossible issues:")
            print("  • Session file doesn't exist")
            print("  • Session is not authorized")
            print("  • File is corrupted")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nTry:")
        print("  1. Close any apps using the session")
        print("  2. Check if telegram_scraper.session exists")
        print("  3. Verify your .env has TELEGRAM_API_ID and TELEGRAM_API_HASH")


if __name__ == "__main__":
    asyncio.run(main())