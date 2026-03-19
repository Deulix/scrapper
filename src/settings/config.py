from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

SITE_URL = "https://inote.by/index.php?route=product/category&path="

CATEGORIES = [
    "Оперативная память",
    "Процессор",
    "SSD",
    "Материнская плата",
    "Видеокарта",
]

BASE_DIR = Path(__file__).parents[2]

RAW_DATA_DIR = BASE_DIR / "data" / "raw"

CLEAN_DATA_DIR = BASE_DIR / "data" / "clean"

CATEGORY_REGEX = "^(" + "|".join(CATEGORIES) + ")"
CAPACITY_REGEX = r"((?:\d+\.)?\d+)\s?(ГБ|ТБ)"
SOCKET_REGEX = r"сокет(?:\s(?:Intel|AMD))?\s([\d\w]+)"
MEMORY_TYPE_REGEX = r"G?DDR\dX?|HBM\d?\w?"
