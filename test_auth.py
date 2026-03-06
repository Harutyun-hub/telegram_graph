"""
test_auth.py — Diagnostic: tests Telegram auth and shows exact response.
Run with: python3 test_auth.py
"""
import asyncio
from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError,
    PhoneNumberBannedError,
    PhoneNumberFloodError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
)
from dotenv import load_dotenv
import os

load_dotenv()

API_ID   = int(os.getenv("TELEGRAM_API_ID", 0))
API_HASH = os.getenv("TELEGRAM_API_HASH", "")
PHONE    = os.getenv("TELEGRAM_PHONE", "")


async def test():
    print(f"\n{'='*50}")
    print("TELEGRAM AUTH DIAGNOSTIC")
    print(f"{'='*50}")
    print(f"API_ID  : {API_ID}")
    print(f"PHONE   : {PHONE}")
    print(f"{'='*50}\n")

    client = TelegramClient("test_session", API_ID, API_HASH)

    try:
        print("Step 1: Connecting to Telegram servers...")
        await client.connect()
        print("✅ Connected to Telegram MTProto servers successfully\n")

        print("Step 2: Checking if already authorized...")
        authorized = await client.is_user_authorized()
        print(f"   Authorized: {authorized}\n")

        if authorized:
            me = await client.get_me()
            print(f"✅ Already logged in as: {me.first_name} (@{me.username})")
            print("   No auth needed — session is valid!")
        else:
            print("Step 3: Requesting a login code from Telegram...")
            try:
                result = await client.send_code_request(PHONE)
                print(f"✅ CODE REQUEST SUCCESSFUL")
                print(f"   Phone code hash: {result.phone_code_hash[:10]}...")
                print(f"   Code type:       {type(result.type).__name__}")
                print(f"\n✅ Telegram is NOT blocking us.")
                print(f"   The code was sent. Check your Telegram app now.")

            except FloodWaitError as e:
                print(f"❌ FLOOD WAIT ERROR — Telegram is rate limiting us")
                print(f"   Must wait: {e.seconds} seconds ({e.seconds//60} minutes)")
                print(f"   This is the proof of a rate limit.")

            except PhoneNumberBannedError:
                print(f"❌ PHONE BANNED — This phone number is banned from Telegram")

            except PhoneNumberFloodError:
                print(f"❌ PHONE FLOOD — Too many code requests for this number")
                print(f"   Telegram rate limit confirmed. Wait before retrying.")

            except Exception as e:
                print(f"❌ ERROR from Telegram: {type(e).__name__}: {e}")

    except Exception as e:
        print(f"❌ CONNECTION ERROR: {type(e).__name__}: {e}")

    finally:
        await client.disconnect()
        # Clean up test session file
        import os as _os
        for f in ["test_session.session", "test_session.session-journal"]:
            if _os.path.exists(f):
                _os.remove(f)
        print("\nTest session cleaned up.")


if __name__ == "__main__":
    asyncio.run(test())
