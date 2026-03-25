#!/usr/bin/env python3
"""
Generate a new Telegram session for production use on Railway.
This script creates both a session file and a string session.
"""

import asyncio
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent))

try:
    from telethon import TelegramClient
    from telethon.sessions import StringSession
except ImportError:
    print("❌ Error: telethon not installed. Run: pip install telethon")
    sys.exit(1)

# Load environment variables
load_dotenv()

# Get credentials from .env
api_id = os.getenv('TELEGRAM_API_ID')
api_hash = os.getenv('TELEGRAM_API_HASH')
phone = os.getenv('TELEGRAM_PHONE')

# Validate credentials
if not api_id or not api_hash:
    print("❌ Error: TELEGRAM_API_ID and TELEGRAM_API_HASH must be set in .env")
    sys.exit(1)

if not phone:
    print("❌ Error: TELEGRAM_PHONE must be set in .env")
    sys.exit(1)

print("="*50)
print("🚀 TELEGRAM SESSION GENERATOR FOR RAILWAY")
print("="*50)
print(f"📱 Phone number: {phone}")
print(f"🔑 API ID: {api_id}")
print("="*50)

async def generate_session():
    """Generate both file and string sessions."""

    # Session file name for production
    session_name = 'telegram_session_prod_new'

    print(f"\n📂 Creating new session file: {session_name}.session")
    print("\n⚠️  IMPORTANT: You will receive a code in your Telegram app")
    print("    (Not SMS! Check your Telegram messages from Telegram itself)")
    print("\n")

    # Create client with file session first
    client = TelegramClient(session_name, api_id, api_hash)

    try:
        # Start client and authenticate
        await client.start(phone=phone)

        # Test the connection
        print("\n✅ Session created successfully!")
        me = await client.get_me()
        print(f"👤 Logged in as: {me.first_name} {me.last_name or ''}")
        print(f"🆔 User ID: {me.id}")
        print(f"📱 Username: @{me.username}" if me.username else "📱 No username set")

        # Get the session string
        string_session = StringSession.save(client.session)

        print("\n" + "="*50)
        print("📋 SESSION STRING FOR RAILWAY (copy everything between the lines):")
        print("="*50)
        print(string_session)
        print("="*50)

        # Save the string session to a file for backup
        string_file = f"{session_name}_string.txt"
        with open(string_file, 'w') as f:
            f.write(string_session)
        print(f"\n💾 Session string also saved to: {string_file}")

        # Instructions for Railway
        print("\n" + "="*50)
        print("🚀 NEXT STEPS FOR RAILWAY:")
        print("="*50)
        print("1. Copy the session string above")
        print("2. Go to your Railway project dashboard")
        print("3. Navigate to Variables tab")
        print("4. Add new variable:")
        print("   Name:  TELEGRAM_SESSION_STRING")
        print("   Value: [Paste the session string]")
        print("5. Redeploy your Railway service")
        print("\n⚠️  IMPORTANT REMINDERS:")
        print("   - Use this session ONLY on Railway")
        print("   - Don't run local scraper with this session")
        print("   - Keep the string secure (it's like a password)")
        print("="*50)

        await client.disconnect()

    except Exception as e:
        print(f"\n❌ Error during session creation: {e}")
        print("\nPossible issues:")
        print("1. Wrong phone number format (should include country code)")
        print("2. Invalid API credentials")
        print("3. Network connection issues")
        print("4. Two-factor authentication is enabled (need to handle password)")
        await client.disconnect()
        sys.exit(1)

def main():
    """Main entry point."""
    try:
        asyncio.run(generate_session())
        print("\n✅ Session generation completed successfully!")
    except KeyboardInterrupt:
        print("\n\n❌ Session generation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()