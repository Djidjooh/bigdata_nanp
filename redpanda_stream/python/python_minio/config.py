import os
from dotenv import load_dotenv

# Charge les variables du fichier .env
load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_SECURE = os.getenv("MINIO_SECURE", "False").lower() == "true"

DEFAULT_BUCKET = os.getenv("DEFAULT_BUCKET", "default-bucket")
