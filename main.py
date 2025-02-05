import os
from dotenv import load_dotenv
from fuse_impl import runFs
from TelegramFUSE import TelegramFileClient

def init():
    load_dotenv()

    api_id = os.getenv("APP_ID")
    api_hash = os.getenv("APP_HASH")
    channel_id = os.getenv("CHANNEL_ID")
    session_name = os.getenv("SESSION_NAME")

    client = TelegramFileClient(session_name, api_id, api_hash, channel_id)
    runFs(client)

if __name__ == "__main__":
    init()
