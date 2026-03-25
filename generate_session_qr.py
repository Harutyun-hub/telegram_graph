#!/usr/bin/env python3
"""
Generate Telegram session using QR code authentication.
This is more reliable than SMS codes and works better with modern Telegram.
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
    import qrcode
except ImportError as e:
    print("❌ Missing dependencies. Install them with:")
    print("   pip install telethon qrcode[pil]")
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

print("="*60)
print("🚀 TELEGRAM SESSION GENERATOR - QR CODE METHOD")
print("="*60)
print(f"🔑 API ID: {api_id}")
print("="*60)

async def display_qr_code(url):
    """Generate and display QR code in terminal."""
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_L,
        box_size=1,
        border=1,
    )
    qr.add_data(url)
    qr.make(fit=True)

    # Print QR code in terminal using ASCII
    print("\n" + "="*60)
    print("📱 SCAN THIS QR CODE WITH YOUR TELEGRAM APP:")
    print("="*60)
    qr.print_ascii(invert=True)

    # Also save as image file
    img = qr.make_image(fill_color="black", back_color="white")
    qr_file = "telegram_login_qr.png"
    img.save(qr_file)
    print(f"\n💾 QR code also saved as: {qr_file}")
    print("   (You can open this image file if the terminal QR doesn't work)")

    return qr_file

async def generate_session_with_qr():
    """Generate session using QR code authentication."""

    session_name = 'telegram_session_prod_new'

    print(f"\n📂 Creating new session file: {session_name}.session")
    print("\n🔄 Connecting to Telegram...")

    # Create client
    client = TelegramClient(session_name, api_id, api_hash)

    try:
        await client.connect()

        if not await client.is_user_authorized():
            print("\n📲 Generating QR code for authentication...")

            # Request QR code login
            qr_login = await client.qr_login()

            # Display QR code
            qr_file = await display_qr_code(qr_login.url)

            print("\n" + "⚠️ " * 10)
            print("HOW TO AUTHENTICATE:")
            print("1. Open Telegram app on your phone")
            print("2. Go to Settings → Devices → Link Desktop Device")
            print("3. Scan the QR code above")
            print("4. Confirm login on your phone")
            print("⚠️ " * 10)

            print("\n⏳ Waiting for QR code scan...")
            print("   (This will timeout in 60 seconds)")

            # Wait for login
            try:
                await asyncio.wait_for(qr_login.wait(), timeout=60)
                print("\n✅ QR code scanned successfully!")
            except asyncio.TimeoutError:
                print("\n❌ QR code expired. Please run the script again.")
                await client.disconnect()
                return False
            except Exception as e:
                print(f"\n❌ Authentication failed: {e}")
                await client.disconnect()
                return False

        # Verify we're logged in
        if not await client.is_user_authorized():
            print("\n❌ Authentication failed. Please try again.")
            await client.disconnect()
            return False

        print("\n✅ SUCCESS! Logged in!")

        # Get user info
        me = await client.get_me()
        print(f"\n👤 Account: {me.first_name} {me.last_name or ''}")
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

        print("\n🚀 RAILWAY DEPLOYMENT STEPS:")
        print("1. Copy the session string above")
        print("2. Go to Railway dashboard → Your Project → Variables")
        print("3. Add new variable:")
        print("   • Name:  TELEGRAM_SESSION_STRING")
        print("   • Value: [paste the session string]")
        print("4. Redeploy your Railway service")

        print("\n⚠️  IMPORTANT REMINDERS:")
        print("• This session is for Railway ONLY")
        print("• Don't use it for local development")
        print("• Keep the session string secure")
        print("• Delete the QR code image after use")

        # Clean up QR code image
        if os.path.exists(qr_file):
            os.remove(qr_file)
            print(f"\n🗑️  Cleaned up QR code image")

        await client.disconnect()
        return True

    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\n💡 Troubleshooting:")
        print("1. Make sure you have the latest Telegram app")
        print("2. Try Settings → Devices → Link Desktop Device")
        print("3. Make sure you're connected to the internet")
        await client.disconnect()
        return False

async def generate_session_with_phone():
    """Fallback: Generate session using phone number."""

    session_name = 'telegram_session_prod_new'

    print("\n📱 Using phone number authentication as fallback...")
    print(f"📞 Phone: {phone}")

    client = TelegramClient(session_name, api_id, api_hash)

    try:
        # This will handle code input automatically
        await client.start(phone=phone)

        if await client.is_user_authorized():
            print("\n✅ Authenticated successfully!")

            me = await client.get_me()
            print(f"👤 Logged in as: {me.first_name}")

            # Generate string session
            string_session = StringSession.save(client.session)

            # Save to file
            string_file = f"{session_name}_string.txt"
            with open(string_file, 'w') as f:
                f.write(string_session)

            print(f"\n📋 Session string saved to: {string_file}")
            print("\nSession string for Railway:")
            print("-"*60)
            print(string_session)
            print("-"*60)

            await client.disconnect()
            return True
    except Exception as e:
        print(f"❌ Phone auth failed: {e}")
        await client.disconnect()
        return False

async def main():
    """Main function."""
    print("\n🔐 Starting authentication process...")
    print("   We'll try QR code first, then phone if needed.\n")

    # Try QR code first
    success = await generate_session_with_qr()

    # If QR fails and we have phone number, try phone auth
    if not success and phone:
        print("\n" + "="*60)
        print("🔄 QR code didn't work. Trying phone number method...")
        print("="*60)
        success = await generate_session_with_phone()

    if success:
        print("\n" + "🎉"*10)
        print(" SESSION READY FOR RAILWAY!")
        print("🎉"*10)
    else:
        print("\n😞 Session generation failed.")
        print("Please try running the script again.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n❌ Cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)