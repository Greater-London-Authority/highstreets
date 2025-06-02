import os
import pandas as pd
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

# ==================== BASE DIRECTORY and AWS CONFIG =================================
S3_BUCKET = "hsds-data"
# BASE_DIR = "Z:/HSDS/data/"
# BASE_DIR = "/mnt/q"
BASE_DIR = f"s3://{S3_BUCKET}/"


# ================ MCARD CONFIG ===============================================
YOY_FILE = os.getenv("YOY_FILE")
CPI_API_ENDPOINT = "https://api.beta.ons.gov.uk/v1/datasets/cpih01"
# ADJUSTMENT_FACTOR_DIR = ("//onelondon.tfl.local/gla/INTELLIGENCE/Projects/2019-20/"
#                          "Covid-19 Busyness/data/mastercard/"
#                          "SpendingPulse/mcard_adjustment_factor.csv")
# INNER_OUTER_QUAD_DIR = ("//onelondon.tfl.local/gla/INTELLIGENCE/Projects/2019-20/"
#                         "Covid-19 Busyness/data/mastercard/
#                        "Inner_outer_quad_lookup.csv")
# SPENDING PULSE
SP_DIR = f"{BASE_DIR}mastercard/spendingpulse/received/"
SP_FILEPATH_PROCESSED = (
    f"{BASE_DIR}mastercard/spendingpulse/" f"SpendingPulse - London - 2018-2024.csv"
)
MCARD_ADJ_PATH = f"{BASE_DIR}mastercard/spendingpulse/mcard_adjustment_factor.csv"
MCARD_ADJ_PATH1 = f"{BASE_DIR}reference_data/mcard_adjustment_factor.csv"
MCARD_ADJ_PATH2 = "Z:/HSDS/data/mastercard/spendingpulse/mcard_adjustment_factor.csv"
MCARD_ADJ_PATH3 = "Z:/HSDS/data/reference_data/mcard_adjustment_factor.csv"
#  These were for testing
# MCARD_ADJ_PATH = "Z:/HSDS/data/mastercard/spendingpulse/test/mcard_adjustment_factor.csv"
# MCARD_ADJ_PATH1 = "Z:/HSDS/data/mastercard/spendingpulse/test/mcard_adjustment_factor.csv"

ADJUSTMENT_FACTOR_DIR = (
    f"{BASE_DIR}mastercard/spendingpulse/" "mcard_adjustment_factor.csv"
)
INNER_OUTER_QUAD_DIR = (
    f"{BASE_DIR}mastercard/spendingpulse/" "Inner_outer_quad_lookup.csv"
)
SECTORS_DF = pd.DataFrame(
    {
        "spending_pulse": ["Total Retail (excl. Auto)", "Apparel", "Restaurants"],
        "geo_insights": ["retail", "apparel", "eating"],
        "geo_insights_raw": ["Total Retail", "Total Apparel", "Eating Places"],
        "cpi": [
            "Overall Index",
            "03 Clothing and footwear",
            "11 Restaurants and hotels",
        ],
        "adjustment_factor": ["retail", "apparel", "eating"],
    }
)
CPI_CATEGORIES = [
    "Overall Index",
    "01 Food and non-alcoholic beverages",
    "02 Alcoholic beverages and tobacco",
    "03 Clothing and footwear",
    "04 Housing, water, electricity, gas and other fuels",
    "05 Furniture, household equipment and maintenance",
    "05.1 Furniture, furnishings and carpets",
    "06 Health",
    "07 Transport",
    "07.2.2 Fuels and lubricants",
    "08 Communication",
    "09 Recreation and culture",
    "09.1 Audio-visual equipment and related products",
    "10 Education",
    "11 Restaurants and hotels",
    "12 Miscellaneous goods and services",
    "12.3.1 Jewellery, clocks and watches",
]

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

BT_HEX_API_ENDPOINT = (
    "https://api.business.bt.com/v1/footfall/reports/hex-grid/tfl?agg=time_indicator"
)
BT_MSOA_API_ENDPOINT = (
    "https://api.business.bt.com/v1/footfall/reports/hourly-aggregate/msoa?agg=hour"
)
BT_LSOA_API_ENDPOINT = (
    "https://api.business.bt.com/v1/footfall/reports/hourly-aggregate/lsoa?agg=hour"
)
BT_HOURLY_OUTAGE_API_ENDPOINT = (
    "https://api.business.bt.com/v1/active-intelligence-health-checks/"
    "data-pipelines/hourly-outage-history"
)
BT_OUTAGE_HISTORY_API_ENDPOINT = (
    "https://api.business.bt.com/v1/active-intelligence-health-checks/"
    "data-pipelines/outage-history"
)
BT_DAILY_AGGREGATED_SHAPES = (
    "https://api.business.bt.com/v1/footfall/reports/daily-aggregate/customer-shapes"
)
BT_CATCHMENT_VISITOR_API_ENDPOINT = (
    "https://api.business.bt.com/v1/footfall/reports/monthly-aggregate/"
    "home-catchment-visitor/lsoa?agg=time_indicator"
)
BT_CATCHMENT_WORKER_API_ENDPOINT = (
    "https://api.business.bt.com/v1/footfall/reports/monthly-aggregate/"
    "home-catchment-worker/lsoa?agg=time_indicator"
)
