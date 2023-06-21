import os

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())


# ==================== PROJECT CONFIG =========================================
PROJECT_FILE = os.getenv("PROJECT_FILE")
PROJECT_ROOT = os.getenv("PROJECT_ROOT")

# ==================== POSTGRES CONFIG ========================================
PG11_DATABASE = os.getenv("PG11_DATABASE")
PG11_USER = os.getenv("PG11_USER")
PG11_PASSWORD = os.getenv("PG11_PASSWORD")
PG11_HOST = os.getenv("PG11_HOST")
PG11_PORT = os.getenv("PG11_PORT")

# ================ MCARD CONFIG ===============================================
YOY_FILE = os.getenv("YOY_FILE")

# ================ BT CONFIG ==================================================
BT_DIR = os.getenv("BT_DIR")
CONSUMER_KEY = os.getenv("CONSUMER_KEY")
CONSUMER_SECRET = os.getenv("CONSUMER_SECRET")
BT_LSOA_DAILY_PREFIX = "lsoa_daily_agg"
BT_MSOA_DAILY_PREFIX = "msoa_daily_agg"
BT_LSOA_MONTHLY_PREFIX = "lsoa_monthly_agg"
BT_MSOA_MONTHLY_PREFIX = "msoa_monthly_agg"
BT_TFL_HEX_DAILY_PREFIX = "tfl_hex_daily_agg"
BT_TFL_HEX_MONTHLY_PREFIX = "tfl_hex_monthly_agg"
BT_MSOA_FOOTFALL_HIST_PREFIX = "msoa_footfall_hist"
