#!/usr/bin/env python3
"""
Convert existing file session to string session.
"""

import asyncio
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from telethon import TelegramClient
from telethon.sessions import StringSession
import config


async def convert_session():
    """Convert file session to string session."""

    # Use the backup to avoid lock issues
    backup_file = "telegram_scraper_backup"

    # Load from file session
    print("Loading from file session...")
    file_client = TelegramClient(
        backup_file,
        config.TELEGRAM_API_ID,
        config.TELEGRAM_API_HASH
    )

    try:
        await file_client.connect()

        if not await file_client.is_user_authorized():
            print("❌ Session is not authorized")
            await file_client.disconnect()
            return None

        # Get user info to verify session
        me = await file_client.get_me()
        print(f"✅ Session valid for: {me.first_name} (@{me.username})")

        # Export session
        session_string = file_client.session.save()

        await file_client.disconnect()

        print(f"✅ Session exported successfully")
        print(f"📏 String length: {len(session_string)} characters")

        return session_string

    except Exception as e:
        print(f"❌ Error: {e}")
        if file_client:
            await file_client.disconnect()
        return None


def save_session(session_string):
    """Save and display session string."""

    env_file = Path(".env.telegram_session")
    with open(env_file, "w") as f:
        f.write(f"# Telegram Session String for Railway\n")
        f.write(f"TELEGRAM_SESSION_STRING={session_string}\n")

    print("\n" + "=" * 80)
    print("🎉 SESSION CONVERTED SUCCESSFULLY!")
    print("=" * 80)

    print("\n📋 COPY THIS TO RAILWAY:")
    print("-" * 80)
    print(f"\nVariable Name:  TELEGRAM_SESSION_STRING")
    print(f"Variable Value: {session_string}")

    print("\n📍 STEPS:")
    print("1. Go to Railway Dashboard → Your Service → Variables")
    print("2. Add the variable above")
    print("3. Deploy")

    print(f"\n💾 Also saved to: {env_file.absolute()}")
    print("=" * 80)


async def main():
    print("\n" + "=" * 80)
    print("🔄 SESSION CONVERTER")
    print("=" * 80)

    session_string = await convert_session()

    if session_string:
        save_session(session_string)
    else:
        print("\n❌ Conversion failed")
        print("\nTry closing any apps using the session file")


if __name__ == "__main__":
    asyncio.run(main())