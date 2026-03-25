#!/usr/bin/env python3
"""
Interactive Telegram session generator for Railway production.
Run this script directly in your terminal to enter the verification code.
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

print("="*60)
print("🚀 TELEGRAM SESSION GENERATOR FOR RAILWAY PRODUCTION")
print("="*60)
print(f"📱 Phone number: {phone}")
print(f"🔑 API ID: {api_id}")
print("="*60)

async def generate_session():
    """Generate both file and string sessions."""

    session_name = 'telegram_session_prod_new'

    print(f"\n📂 Creating new session file: {session_name}.session")
    print("\n" + "⚠️ " * 10)
    print("IMPORTANT: A verification code will be sent to your Telegram app")
    print("Check your Telegram messages from 'Telegram' (official account)")
    print("⚠️ " * 10)
    print("\nConnecting to Telegram...")

    # Create client with file session
    client = TelegramClient(session_name, api_id, api_hash)

    try:
        # Start client - this will prompt for code interactively
        await client.start(phone=phone)

        # If we get here, authentication was successful
        print("\n✅ SUCCESS! Session created!")

        # Get user info
        me = await client.get_me()
        print(f"\n👤 Logged in as: {me.first_name} {me.last_name or ''}")
        print(f"🆔 User ID: {me.id}")
        if me.username:
            print(f"📱 Username: @{me.username}")

        # Generate string session
        print("\n⏳ Generating string session for Railway...")
        string_session = StringSession.save(client.session)

        # Save to file
        string_file = f"{session_name}_string.txt"
        with open(string_file, 'w') as f:
            f.write(string_session)

        print("\n" + "="*60)
        print("✅ SESSION GENERATION COMPLETE!")
        print("="*60)

        print(f"\n📁 Files created:")
        print(f"   - {session_name}.session (backup file)")
        print(f"   - {string_file} (contains string for Railway)")

        print("\n📋 SESSION STRING FOR RAILWAY:")
        print("-"*60)
        print(string_session)
        print("-"*60)

        print("\n🚀 NEXT STEPS:")
        print("1. Copy the session string above (or from the .txt file)")
        print("2. Go to Railway dashboard → your project → Variables")
        print("3. Add new variable:")
        print("   • Name:  TELEGRAM_SESSION_STRING")
        print("   • Value: [paste the session string]")
        print("4. Redeploy your Railway service")

        print("\n⚠️  IMPORTANT:")
        print("• Use this session ONLY on Railway (not locally)")
        print("• Keep the session string secure")
        print("• If you run locally, create a different session")

        await client.disconnect()
        return True

    except Exception as e:
        print(f"\n❌ Error: {e}")

        if "The phone number is invalid" in str(e):
            print("\n💡 Fix: Check your phone number format in .env")
            print("   Should be: +37477914915 (with country code)")
        elif "timeout" in str(e).lower():
            print("\n💡 Fix: Check your internet connection")
        elif "two-factor" in str(e).lower() or "Two-step" in str(e):
            print("\n💡 Fix: Your account has 2FA enabled")
            print("   The script should prompt for your password")
        else:
            print("\n💡 Common fixes:")
            print("   1. Make sure Telegram app is open")
            print("   2. Check for the code in Telegram messages")
            print("   3. Try running the script again")

        await client.disconnect()
        return False

def main():
    """Main entry point."""
    print("\n🔔 This script needs your interaction!")
    print("   You'll need to enter the verification code when prompted.\n")

    try:
        success = asyncio.run(generate_session())
        if success:
            print("\n✅ All done! Your Railway session is ready.")
        else:
            print("\n❌ Session generation failed. Please try again.")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n\n❌ Cancelled by user")
        sys.exit(1)

if __name__ == "__main__":
    main()