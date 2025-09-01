import os
from dotenv import load_dotenv

load_dotenv()

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GENAI_MODEL = os.getenv("GENAI_MODEL")
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD") or os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")
TESSERACT_CMD = os.getenv("TESSERACT_CMD")

# Validate critical variables
if not DB_PASSWORD:
    raise ValueError("Database password not found. Check your .env file.")
