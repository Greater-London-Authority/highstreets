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
MCARD_ADJ_PATH3 = ("Z:/HSDS/data/reference_data/"
                   "mcard_adjustment_factor.csv")

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
    "06 Health",
    "07 Transport",
    "08 Communication",
    "09 Recreation and culture",
    "10 Education",
    "11 Restaurants and hotels",
    "12 Miscellaneous goods and services",
]

# mm23 time series IDs for each CPIH category (fallback when cpih01 lags)
CPI_MM23_SERIES = {
    "Overall Index": "L522",
    "01 Food and non-alcoholic beverages": "L523",
    "02 Alcoholic beverages and tobacco": "L524",
    "03 Clothing and footwear": "L525",
    "04 Housing, water, electricity, gas and other fuels": "L5PG",
    "05 Furniture, household equipment and maintenance": "L527",
    "06 Health": "L528",
    "07 Transport": "L529",
    "08 Communication": "L52A",
    "09 Recreation and culture": "L52B",
    "10 Education": "L52C",
    "11 Restaurants and hotels": "L52D",
    "12 Miscellaneous goods and services": "L52E",
}

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

# ================ LDC CONFIG ==================================================
LDC_OUTPUT_DIR = f"{BASE_DIR}ldc/"
LDC_BACKUP_DIR = f"{BASE_DIR}ldc/ldc_backups/"

# LDC Snowflake connection config (credentials from env vars)
LDC_SNOWFLAKE_SCHEMA = "SCH_GREEN_STREET"
LDC_SNOWFLAKE_TABLE = "VW_GS_RETAIL_UK_TENANT_V2"

# LDC S3 archive paths
LDC_S3_BASE = f"{BASE_DIR}ldc/"
LDC_S3_INITIAL_LOAD = f"{LDC_S3_BASE}initial_load/"
LDC_S3_SNOWFLAKE_SNAPSHOTS = f"{LDC_S3_BASE}snowflake_snapshots/"
LDC_S3_CLEAN_ARCHIVES = f"{LDC_S3_BASE}clean_archives/"

# LDC PostgreSQL table names
LDC_RAW_TABLE = "ldc_premises_raw"
LDC_CLEAN_TABLE = "ldc_premises_clean"
LDC_STAGING_TABLE = "ldc_premises_staging"

# LDC Z: drive paths (transition period only)
LDC_Z_DRIVE_RAW = "Z:/HSDS/data/ldc/all_biz_raw.csv"
LDC_Z_DRIVE_CLEAN = "Z:/HSDS/data/ldc/all_biz_clean.csv"
LDC_Z_DRIVE_HISTORIC = ("Z:/HSDS/data/ldc/"
                        "Greater London Authority 10-Year Time Series Nov24.xlsx")

# Great Expectations suite names for LDC
LDC_GE_SUITE_SOURCE_SCHEMA = "ldc_source_schema"
LDC_GE_SUITE_RAW_QUALITY = "ldc_raw_quality"
LDC_GE_SUITE_BUSINESS_LOGIC = "ldc_business_logic"
LDC_GE_SUITE_CLEAN_OUTPUT = "ldc_clean_output"
