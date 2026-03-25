#!/usr/bin/env python3
"""
Convert existing session file to string session for Railway.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Add parent directory to path
sys.path.append(str(Path(__file__).parent))

try:
    from telethon import TelegramClient
    from telethon.sessions import StringSession
except ImportError:
    print("❌ Error: telethon not installed. Run: pip install telethon")
    sys.exit(1)

# Load environment variables
load_dotenv()

# Get credentials
api_id = os.getenv('TELEGRAM_API_ID')
api_hash = os.getenv('TELEGRAM_API_HASH')

if not api_id or not api_hash:
    print("❌ Error: TELEGRAM_API_ID and TELEGRAM_API_HASH must be set in .env")
    sys.exit(1)

print("="*70)
print("🔄 CONVERTING SESSION FILE TO STRING SESSION")
print("="*70)

# Session file to convert
session_file = 'telegram_session_prod_new'

if not os.path.exists(f'{session_file}.session'):
    print(f"❌ Error: {session_file}.session not found!")
    print("\nAvailable session files:")
    for f in Path('.').glob('*.session'):
        print(f"  - {f}")
    sys.exit(1)

print(f"📂 Reading session file: {session_file}.session")

# Create client with existing session
client = TelegramClient(session_file, api_id, api_hash)

async def convert_session():
    """Convert file session to string session."""

    try:
        # Connect to verify session works
        await client.connect()

        if not await client.is_user_authorized():
            print("❌ Session is not authorized! You may need to regenerate it.")
            await client.disconnect()
            return None

        # Get user info to confirm it works
        me = await client.get_me()
        print(f"✅ Session is valid!")
        print(f"👤 Account: {me.first_name} {me.last_name or ''}")
        if me.username:
            print(f"📱 Username: @{me.username}")

        # Get the string session
        print("\n🔄 Converting to string session...")
        string_session = StringSession.save(client.session)

        # Save to file
        output_file = f'{session_file}_string.txt'
        with open(output_file, 'w') as f:
            f.write(string_session)

        print(f"\n💾 String session saved to: {output_file}")

        await client.disconnect()

        return string_session

    except Exception as e:
        print(f"❌ Error: {e}")
        await client.disconnect()
        return None

# Run the conversion
import asyncio

string_session = asyncio.run(convert_session())

if string_session:
    print("\n" + "="*70)
    print("✅ SESSION STRING FOR RAILWAY (copy everything between the lines):")
    print("="*70)
    print(string_session)
    print("="*70)

    print("\n🚀 NEXT STEPS FOR RAILWAY:")
    print("1. Copy the session string above")
    print("2. Go to Railway → Your Project → Variables")
    print("3. Add or update variable:")
    print("   • Name:  TELEGRAM_SESSION_STRING")
    print("   • Value: [paste the session string]")
    print("4. Redeploy your Railway service")

    print("\n⚠️  IMPORTANT:")
    print("• Use this session ONLY on Railway")
    print("• Don't use it locally while Railway is running")
    print("• Keep the string secure (it's like a password)")
else:
    print("\n❌ Failed to convert session")
    print("You may need to generate a new session")