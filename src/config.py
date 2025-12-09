# src/config.py
from dotenv import load_dotenv
import os

load_dotenv()  # carga .env en variables de entorno

DB = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'dbname': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASS', ''),
    'port': int(os.getenv('DB_PORT', 5432))
}
