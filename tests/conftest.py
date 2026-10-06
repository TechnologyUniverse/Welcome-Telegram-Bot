import os

# Importing the bot constructs an aiogram Bot object, but does not call Telegram.
# Build a syntactically valid token at runtime without storing token-shaped text.
TEST_BOT_TOKEN = "123456789:" + ("A" * 35)
os.environ["BOT_TOKEN"] = TEST_BOT_TOKEN
