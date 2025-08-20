import pandas as pd
import warnings
from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.data_source_sink.datawriter import DataWriter
from highstreets.api.clientbase import APIClient
from highstreets.core.sql_manager import SQLManager
from highstreets.data_transformation.mcard_transform import McardTransform
from highstreets import config
from sqlalchemy import create_engine
import psycopg2
import os
from dotenv import find_dotenv, load_dotenv
from sqlalchemy import exc as sa_exc
# Suppress GeoPandas GEOS version warnings
warnings.filterwarnings('ignore', message='.*Shapely GEOS version.*incompatible.*')
# Suppress SQLAlchemy XML column warnings
warnings.filterwarnings(
    'ignore', category=sa_exc.SAWarning, message='.*Did not recognize type.*xml.*')
# Optional: Suppress all SQLAlchemy warnings if needed
# warnings.filterwarnings('ignore', category=sa_exc.SAWarning)
print("Warning filters applied for cleaner output")

load_dotenv(find_dotenv())
base_dir = config.BASE_DIR
# initialize the database connection
database = os.getenv("PG_DATABASE")
username = os.getenv("PG_USER")
password = os.getenv("PG_PASSWORD")
host = os.getenv("PG_HOST")
port = os.getenv("PG_PORT")
engine = create_engine(
    f"postgresql+psycopg2://{username}:{password}@" f"{host}:{port}/{database}"
)

# initialize the data loader, writer, api client, mcard transform and sql manager
data_loader = DataLoader()
data_writer = DataWriter()
api_client = APIClient()
mcard_transform = McardTransform()
sql_manager = SQLManager()

# Connect to PostgreSQL database
conn = psycopg2.connect(
    dbname=os.getenv('PG_DATABASE'),
    user=os.getenv('PG_USER'),
    password=os.getenv('PG_PASSWORD'),
    host=os.getenv('PG_HOST'),
    port=os.getenv('PG_PORT')
)

# Load the bid query and save to csv
query = sql_manager.get_query('bid_intl_query.sql')
df_bid = pd.read_sql_query(query, conn)
df_bid.to_csv(f"{base_dir}mastercard/weekly/processed/international/"
              f"mcard_weekly_bid_international_txn.csv", index=False)

# Load the highstreet query and save to csv
query = sql_manager.get_query('highstreet_intl_query.sql')
hs_weekly = pd.read_sql_query(query, conn)
hs_weekly.to_csv(f"{base_dir}mastercard/weekly/processed/international/"
                 f"mcard_weekly_highstreet_international_txn.csv", index=False)

# Load the towncentre query and save to csv
query = sql_manager.get_query('towncentre_intl_query.sql')
tc_weekly = pd.read_sql_query(query, conn)
tc_weekly.to_csv(f"{base_dir}mastercard/weekly/processed/international/"
                 f"mcard_weekly_towncentre_international_txn.csv", index=False)

# Load the msoa query and save to csv
query = sql_manager.get_query('msoa_intl_query.sql')
msoa_weekly = pd.read_sql_query(query, conn)
msoa_weekly.to_csv(f"{base_dir}mastercard/weekly/processed/international/"
                   f"mcard_weekly_msoa_international_txn.csv", index=False)

# Load the bespoke query and save to csv
query = sql_manager.get_query('bespoke_intl_query.sql')
bespoke_weekly = pd.read_sql_query(query, conn)
bespoke_weekly['week_start'] = pd.to_datetime(bespoke_weekly['week_start'],
                                              errors='coerce')
bespoke_weekly.to_csv(f"{base_dir}mastercard/weekly/processed/international/"
                      f"mcard_weekly_bespoke_international_txn.csv", index=False)

# Load the caz query and save to csv
query = sql_manager.get_query('caz_intl_query.sql')
caz_weekly = pd.read_sql_query(query, conn)
caz_weekly['week_start'] = pd.to_datetime(caz_weekly['week_start'], errors='coerce')
caz_weekly.to_csv(f"{base_dir}mastercard/weekly/processed/international/"
                  f"mcard_weekly_caz_international_txn.csv", index=False)


# Load aggregated data from base directory(default: AWS S3)
# adjust the data using the cpi table
folder = f"{base_dir}mastercard/weekly/processed/international/"
cpi_table = api_client.fetch_cpi()
cpi_table = cpi_table[cpi_table['Aggregate'] == 'Overall Index']
cpi_table.rename(columns={'Aggregate': 'aggregate'}, inplace=True)


def adjust(
    df, cpi_table, reindexing_year=2018,
    cols_to_adjust=['txn_amt_wd_retail', 'txn_amt_we_retail']
):
    df_adj = df.copy()
    # need to change the yr column to not the isocalendar version for the
    # inflation adjustment.
    df_adj['yr'] = df_adj['week_start'].dt.year
    df_adj['month'] = df_adj['week_start'].dt.month
    df_adj = df_adj.sort_values(by='week_start')
    df_adj = mcard_transform.inflation_adjust(
        df_adj, cpi_table, reindexing_year=reindexing_year, col_to_adjust=cols_to_adjust,
        date_col='week_start')
    # Change the year column back to isocalenday year so it aligns with the weeks
    df_adj['yr'] = df_adj['week_start'].dt.isocalendar().year
    return df_adj


for area in ['bespoke', 'bid', 'caz', 'highstreet', 'msoa', 'towncentre']:
    df = pd.read_csv(f"{folder}mcard_weekly_{area}_international_txn.csv")
    df['week_start'] = pd.to_datetime(df['week_start'])
    df_adj = adjust(df, cpi_table)
    df_adj.to_csv(f"{folder}mcard_weekly_{area}_international_txn_adj.csv", index=False)
    data_writer.truncate_and_load_to_postgres(
        df_adj,
        table_name=f'econ_busyness_mcard_{area}_intl_txn',
        schema='gisapdata')


# Sublicense data

bespoke_adj = pd.read_csv(f"{folder}mcard_weekly_bespoke_international_txn.csv")

# sublicense - colliers
holba_ids = [112, 113, 114, 115, 116, 117, 118, 197]
bespoke_adj[bespoke_adj['bespoke_area_id'].isin(holba_ids)].to_csv(
    f"{folder}bespoke/Colliers agreement - Holba sites/"
    "colliers_mcard_weekly_intl_txn.csv", index=False)
data_writer.upload_data_to_lds(
    slug="colliers---hsds",
    custom_date_column="week_start",
    resource_title="colliers_mcard_weekly_intl_txn.csv",
    file_path=(
        f"{folder}bespoke/Colliers agreement - Holba sites/"
        "colliers_mcard_weekly_intl_txn.csv"
    ),
)

# upload the data to the LDS
data_writer.upload_data_to_lds(
    slug="mastercard-retail-location-insights-international-spend",
    custom_date_column="week_start",
    resource_title="mcard_weekly_msoa_international_txn_adj.csv",
    file_path=(
        f"{base_dir}mastercard/weekly/processed/international/"
        "mcard_weekly_msoa_international_txn_adj.csv"
    ),
)
data_writer.upload_data_to_lds(
    slug="mastercard-retail-location-insights-international-spend",
    custom_date_column="week_start",
    resource_title="mcard_weekly_bid_international_txn_adj.csv",
    file_path=(
        f"{base_dir}mastercard/weekly/processed/international/"
        "mcard_weekly_bid_international_txn_adj.csv"
    ),
)

data_writer.upload_data_to_lds(
    slug="mastercard-retail-location-insights-international-spend",
    custom_date_column="week_start",
    resource_title="mcard_weekly_caz_international_txn_adj.csv",
    file_path=(
        f"{base_dir}mastercard/weekly/processed/international/"
        "mcard_weekly_caz_international_txn_adj.csv"
    ),
)

data_writer.upload_data_to_lds(
    slug="mastercard-retail-location-insights-international-spend",
    custom_date_column="week_start",
    resource_title="mcard_weekly_bespoke_international_txn_adj.csv",
    file_path=(
        f"{base_dir}mastercard/weekly/processed/international/"
        "mcard_weekly_bespoke_international_txn_adj.csv"
    ),
)

data_writer.upload_data_to_lds(
    slug="mastercard-retail-location-insights-international-spend",
    custom_date_column="week_start",
    resource_title="mcard_weekly_towncentre_international_txn_adj.csv",
    file_path=(
        f"{base_dir}mastercard/weekly/processed/international/"
        "mcard_weekly_towncentre_international_txn_adj.csv"
    ),
)
data_writer.upload_data_to_lds(
    slug="mastercard-retail-location-insights-international-spend",
    custom_date_column="week_start",
    resource_title="mcard_weekly_highstreet_international_txn_adj.csv",
    file_path=(
        f"{base_dir}mastercard/weekly/processed/international/"
        "mcard_weekly_highstreet_international_txn_adj.csv"
    ),
)
