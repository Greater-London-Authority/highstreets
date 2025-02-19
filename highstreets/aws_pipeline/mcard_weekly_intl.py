import pandas as pd
from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.data_source_sink.datawriter import DataWriter
from highstreets import config
import io
import requests
base_dir = config.BASE_DIR

from IPython import get_ipython
from IPython.terminal.ipapp import load_default_config

# Enable autoreload
get_ipython().run_line_magic('load_ext', 'autoreload')
get_ipython().run_line_magic('autoreload', '2')

from sqlalchemy import create_engine, text
import pandas as pd
import os
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())
database = os.getenv("PG_DATABASE")
username = os.getenv("PG_USER")
password = os.getenv("PG_PASSWORD")
host = os.getenv("PG_HOST")
port = os.getenv("PG_PORT")
engine = create_engine(
    f"postgresql+psycopg2://{username}:{password}@" f"{host}:{port}/{database}"
)
data_loader = DataLoader()
data_writer = DataWriter()

import psycopg2
import pandas as pd
import os
 
# Connect to PostgreSQL database
conn = psycopg2.connect(
    dbname=os.getenv('PG_DATABASE'),
    user=os.getenv('PG_USER'),
    password=os.getenv('PG_PASSWORD'),
    host=os.getenv('PG_HOST'),
    port=os.getenv('PG_PORT')
)

query = """ WITH week_data AS (
    SELECT 
        main.yr,
        main.wk,
        main.industry,
        main.segment,
        main.geo_name,
        main.quad_id,
        main.txn_amt,
        main.txn_cnt,
        main.weekday_weekend,
        lookup.bid_id,
        lookup.bid_name,
        -- Calculate week_start based on yr and wk
        TO_CHAR(
            (DATE_TRUNC('week', 
               TO_DATE(CONCAT(main.yr, '0104'), 'YYYYMMDD') 
               + INTERVAL '1 day' * (7 * (main.wk - 1))
            )), 'YYYY-MM-DD') AS week_start
    FROM econ_busyness_mcard_raw_18_zoom AS main
    JOIN econ_busyness_mcard_bids_quad_lookup AS lookup 
      ON main.quad_id = lookup.quad_id
    WHERE main.industry = 'Total Retail'
      AND main.segment = 'International'
      AND main.geo_name = 'London'
)
SELECT
    bid_id,
    bid_name,
    yr,
    wk,
    week_start,
    segment,
    -- Pivot the txn_amt values based on weekday_weekend and industry
    SUM(CASE WHEN weekday_weekend = 'weekdays' THEN txn_amt END) AS txn_amt_wd_retail,
    SUM(CASE WHEN weekday_weekend = 'weekends' THEN txn_amt END) AS txn_amt_we_retail
FROM week_data
GROUP BY bid_id, bid_name, yr, wk, week_start, segment
ORDER BY yr, wk; """
df_bid = pd.read_sql_query(query, conn)
df_bid.to_csv(f"{base_dir}mastercard/weekly/processed/mcard_weekly_bid_international_txn.csv", index=False)

query = """ WITH week_data AS (
    SELECT 
        main.yr,
        main.wk,
        main.industry,
        main.segment,
        main.geo_name,
        main.quad_id,
        main.txn_amt,
        main.txn_cnt,
        main.weekday_weekend,
        lookup.highstreet_id,
        lookup.highstreet_name,
        -- Calculate week_start based on yr and wk
        TO_CHAR(
            (DATE_TRUNC('week', 
               TO_DATE(CONCAT(main.yr, '0104'), 'YYYYMMDD') 
               + INTERVAL '1 day' * (7 * (main.wk - 1))
            )), 'YYYY-MM-DD') AS week_start
    FROM econ_busyness_mcard_raw_18_zoom AS main
    JOIN econ_busyness_mcard_highstreets_quad_lookup AS lookup 
      ON main.quad_id = lookup.quad_id
    WHERE main.industry = 'Total Retail'
      AND main.segment = 'International'
      AND main.geo_name = 'London'
)
SELECT
    highstreet_id,
    highstreet_name,
    yr,
    wk,
    week_start,
    segment,
    -- Pivot the txn_amt values based on weekday_weekend and industry
    SUM(CASE WHEN weekday_weekend = 'weekdays' THEN txn_amt END) AS txn_amt_wd_retail,
    SUM(CASE WHEN weekday_weekend = 'weekends' THEN txn_amt END) AS txn_amt_we_retail
FROM week_data
GROUP BY highstreet_id, highstreet_name, yr, wk, week_start, segment
ORDER BY yr, wk; """
 
# Load the query result into a DataFrame
hs_weekly = pd.read_sql_query(query, conn)
hs_weekly.to_csv(f"{base_dir}mastercard/weekly/processed/mcard_weekly_highstreet_international_txn.csv", index=False)

# SQL Query for fetching required data
query = """ WITH week_data AS (
    SELECT 
        main.yr,
        main.wk,
        main.industry,
        main.segment,
        main.geo_name,
        main.quad_id,
        main.txn_amt,
        main.txn_cnt,
        main.weekday_weekend,
        lookup.tc_id,
        lookup.tc_name,
        -- Calculate week_start based on yr and wk
        TO_CHAR(
            (DATE_TRUNC('week', 
               TO_DATE(CONCAT(main.yr, '0104'), 'YYYYMMDD') 
               + INTERVAL '1 day' * (7 * (main.wk - 1))
            )), 'YYYY-MM-DD') AS week_start
    FROM econ_busyness_mcard_raw_18_zoom AS main
    JOIN econ_busyness_mcard_towncentres_quad_lookup AS lookup 
      ON main.quad_id = lookup.quad_id
    WHERE main.industry = 'Total Retail'
      AND main.segment = 'International'
      AND main.geo_name = 'London'
)
SELECT
    tc_id,
    tc_name,
    yr,
    wk,
    week_start,
    segment,
    -- Pivot the txn_amt values based on weekday_weekend and industry
    SUM(CASE WHEN weekday_weekend = 'weekdays' THEN txn_amt END) AS txn_amt_wd_retail,
    SUM(CASE WHEN weekday_weekend = 'weekends' THEN txn_amt END) AS txn_amt_we_retail
FROM week_data
GROUP BY tc_id, tc_name, yr, wk, week_start, segment
ORDER BY yr, wk; """

# Load the query result into a DataFrame
tc_weekly = pd.read_sql_query(query, conn)
tc_weekly.to_csv(f"{base_dir}mastercard/weekly/processed/"
                 f"mcard_weekly_towncentre_international_txn.csv", index=False)

# SQL Query for fetching required data
query = """ WITH week_data AS (
    SELECT 
        main.yr,
        main.wk,
        main.industry,
        main.segment,
        main.geo_name,
        main.quad_id,
        main.txn_amt,
        main.txn_cnt,
        main.weekday_weekend,
        lookup.msoa11cd,
        lookup.msoa11nm,
        -- Calculate week_start based on yr and wk
        TO_CHAR(
            (DATE_TRUNC('week', 
               TO_DATE(CONCAT(main.yr, '0104'), 'YYYYMMDD') 
               + INTERVAL '1 day' * (7 * (main.wk - 1))
            )), 'YYYY-MM-DD') AS week_start
    FROM econ_busyness_mcard_raw_18_zoom AS main
    JOIN econ_busyness_mcard_msoas_quad_lookup AS lookup 
      ON main.quad_id = lookup.quad_id
    WHERE main.industry = 'Total Retail'
      AND main.segment = 'International'
      AND main.geo_name = 'London'
)
SELECT
    msoa11cd,
    msoa11nm,
    yr,
    wk,
    week_start,
    segment,
    -- Pivot the txn_amt values based on weekday_weekend and industry
    SUM(CASE WHEN weekday_weekend = 'weekdays' THEN txn_amt END) AS txn_amt_wd_retail,
    SUM(CASE WHEN weekday_weekend = 'weekends' THEN txn_amt END) AS txn_amt_we_retail
FROM week_data
GROUP BY msoa11cd, msoa11nm, yr, wk, week_start, segment
ORDER BY yr, wk; """

# query = """
#     SELECT main.yr, main.wk, main.industry, main.segment, main.geo_name, main.quad_id, main.txn_amt, main.txn_cnt, main.weekday_weekend, lookup.msoa11cd, lookup.msoa11nm
#     FROM econ_busyness_mcard_raw_18_zoom AS main
#     JOIN econ_busyness_mcard_msoas_quad_lookup AS lookup ON main.quad_id = lookup.quad_id
#     WHERE main.industry = 'Total Retail'
#       AND main.segment = 'International'
#       AND main.geo_name = 'London';
# """
 
# Load the query result into a DataFrame
msoa_weekly = pd.read_sql_query(query, conn)
msoa_weekly.to_csv(f"{base_dir}mastercard/weekly/processed/mcard_weekly_msoa_international_txn.csv", index=False)

# SQL Query for fetching required data
query = """ WITH week_data AS (
    SELECT 
        main.yr,
        main.wk,
        main.industry,
        main.segment,
        main.geo_name,
        main.quad_id,
        main.txn_amt,
        main.txn_cnt,
        main.weekday_weekend,
        lookup.bespoke_area_id,
        lookup.name,
        -- Calculate week_start based on yr and wk
        TO_CHAR(
            (DATE_TRUNC('week', 
               TO_DATE(CONCAT(main.yr, '0104'), 'YYYYMMDD') 
               + INTERVAL '1 day' * (7 * (main.wk - 1))
            )), 'YYYY-MM-DD') AS week_start
    FROM econ_busyness_mcard_raw_18_zoom AS main
    JOIN econ_busyness_mcard_bespoke_quad_lookup AS lookup 
      ON main.quad_id = lookup.quad_id
    WHERE main.industry = 'Total Retail'
      AND main.segment = 'International'
      AND main.geo_name = 'London'
)
SELECT
    bespoke_area_id,
    name,
    yr,
    wk,
    week_start,
    segment,
    -- Pivot the txn_amt values based on weekday_weekend and industry
    SUM(CASE WHEN weekday_weekend = 'weekdays' THEN txn_amt END) AS txn_amt_wd_retail,
    SUM(CASE WHEN weekday_weekend = 'weekends' THEN txn_amt END) AS txn_amt_we_retail
FROM week_data
GROUP BY bespoke_area_id, name, yr, wk, week_start, segment
ORDER BY yr, wk; """

# query = """
#     SELECT main.yr, main.wk, main.industry, main.segment, main.geo_name, main.quad_id, main.txn_amt, main.txn_cnt, main.weekday_weekend, lookup.bespoke_area_id, lookup.name
#     FROM econ_busyness_mcard_raw_18_zoom AS main
#     JOIN econ_busyness_mcard_bespoke_quad_lookup AS lookup ON main.quad_id = lookup.quad_id
#     WHERE main.industry = 'Total Retail'
#       AND main.segment = 'International'
#       AND main.geo_name = 'London';
# """
 
# Load the query result into a DataFrame
bespoke_weekly = pd.read_sql_query(query, conn)
bespoke_weekly['week_start'] = pd.to_datetime(bespoke_weekly['week_start'], errors='coerce')
bespoke_weekly.to_csv(f"{base_dir}mastercard/weekly/processed/mcard_weekly_bespoke_international_txn.csv", index=False)

# SQL Query for fetching required data
query = """ WITH week_data AS (
    SELECT 
        main.yr,
        main.wk,
        main.industry,
        main.segment,
        main.geo_name,
        main.quad_id,
        main.txn_amt,
        main.txn_cnt,
        main.weekday_weekend,
        lookup.name,
        -- Calculate week_start based on yr and wk
        TO_CHAR(
            (DATE_TRUNC('week', 
               TO_DATE(CONCAT(main.yr, '0104'), 'YYYYMMDD') 
               + INTERVAL '1 day' * (7 * (main.wk - 1))
            )), 'YYYY-MM-DD') AS week_start
    FROM econ_busyness_mcard_raw_18_zoom AS main
    JOIN econ_busyness_mcard_caz_quad_lookup AS lookup 
      ON main.quad_id = lookup.quad_id
    WHERE main.industry = 'Total Retail'
      AND main.segment = 'International'
      AND main.geo_name = 'London'
)
SELECT
    name,
    yr,
    wk,
    week_start,
    segment,
    -- Pivot the txn_amt values based on weekday_weekend and industry
    SUM(CASE WHEN weekday_weekend = 'weekdays' THEN txn_amt END) AS txn_amt_wd_retail,
    SUM(CASE WHEN weekday_weekend = 'weekends' THEN txn_amt END) AS txn_amt_we_retail
FROM week_data
GROUP BY name, yr, wk, week_start, segment
ORDER BY yr, wk; """

# query = """
#     SELECT main.yr, main.wk, main.industry, main.segment, main.geo_name, main.quad_id, main.txn_amt, main.txn_cnt, main.weekday_weekend, lookup.name
#     FROM econ_busyness_mcard_raw_18_zoom AS main
#     JOIN econ_busyness_mcard_caz_quad_lookup AS lookup ON main.quad_id = lookup.quad_id
#     WHERE main.industry = 'Total Retail'
#       AND main.segment = 'International'
#       AND main.geo_name = 'London';
# """
 
# Load the query result into a DataFrame
caz_weekly = pd.read_sql_query(query, conn)
caz_weekly['week_start'] = pd.to_datetime(caz_weekly['week_start'], errors='coerce')
caz_weekly.to_csv(f"{base_dir}mastercard/weekly/processed/mcard_weekly_caz_international_txn.csv", index=False)

"""Inflation adjustment"""
# FETCH CPI
def fetch_cpi():
    response = requests.get("https://api.beta.ons.gov.uk/v1/datasets/cpih01")
    latest_version = requests.get(response.json()['links']['latest_version']['href'])
    url = latest_version.json()['downloads']['csv']['href']
    s = requests.get(url).content
    cpi_table = pd.read_csv(io.StringIO(s.decode('utf-8')))
    cpi_table['date'] = pd.to_datetime(cpi_table["mmm-yy"], format="%b-%y")
    cpi_table['yr'] = cpi_table['date'].dt.year
    cpi_table['month'] = cpi_table['date'].dt.month
                                      
    cpi_table = cpi_table[cpi_table['Aggregate']=='Overall Index'].sort_values('date')[['yr','month','Aggregate','v4_0']].rename(columns={'v4_0':'cpi_index'}).reset_index().drop(columns='index')
    #cpi_table = cpi_table.sort_values('date')[['yr','month','Aggregate','v4_0']].rename(columns={'v4_0':'cpi_index'}).reset_index().drop(columns='index')

    return cpi_table


# INFLATION ADJUSTMENT
def inflation_adjust(spend, cpi_table, reindexing_year=None,col_to_adjust=['txn_amt']):
    '''
    Adjusts spend columns 'txn_amt' and 'avg_spend_amt by monthly ONS inflation rates.
    spend: spend table of MC 3-hourly data
    cpi_table: imported and cleaned CPIH table from ONS
    reindexing year (optional): the year that you want to use as cpi_index = 100. If none, does not reindex beyond ONS's existing 2015=100 reindex
    '''
    
    # Reindex to a chosen baseline year, otherwise skip
    if reindexing_year is not None:
        reindex = cpi_table[cpi_table['yr']==reindexing_year]['cpi_index'].mean()
        cpi_table['cpi_index'] = cpi_table['cpi_index']/reindex * 100
    else:
        pass
    
    # Join spend data with cpi data
    spend = pd.merge(spend,cpi_table,how='left',on=['yr','month'])
    
    # If spend data is more recent than cpi data, there will be NaNs. Fill them with the latest available cpi index
    max_year = cpi_table['yr'].max()
    max_month = cpi_table[(cpi_table['yr'] == max_year)]['month'].max()
    spend['cpi_index'].fillna(cpi_table[(cpi_table['month']==max_month) & (cpi_table['yr']==max_year)]['cpi_index'])
    
    # Adjust
    for col in col_to_adjust:
        spend[f"{col}"] = spend[col]/spend['cpi_index'] * 100
    #spend['avg_spend_amt'] = spend['avg_spend_amt']/spend['cpi_index'] * 100
    spend.drop(columns=['Aggregate','cpi_index'],inplace=True)
    
    return spend


# Load aggregated data from Q drive
folder = f"{base_dir}mastercard/weekly/processed/"
cpi_table = fetch_cpi()
def adjust(df, cpi_table, reindexing_year=2018, cols_to_adjust=['txn_amt_wd_retail','txn_amt_we_retail']):
    df_adj = df.copy()
    df_adj['yr'] = df_adj['week_start'].dt.year # need to change the yr column to not the isocalendar version for the inflation adjustment. 
    df_adj['month'] = df_adj['week_start'].dt.month
    df_adj = df_adj.sort_values(by='week_start')
    df_adj = inflation_adjust(df_adj, cpi_table, reindexing_year=reindexing_year,col_to_adjust=cols_to_adjust)
    df_adj['yr'] = df_adj['week_start'].dt.isocalendar().year # Change the year column back to isocalenday year so it aligns with the weeks
    
    return df_adj

for area in ['bespoke','bid','caz','highstreet','msoa','towncentre']:
    df = pd.read_csv(f"{folder}mcard_weekly_{area}_international_txn.csv")
    df['week_start'] = pd.to_datetime(df['week_start'])
    df_adj = adjust(df, cpi_table)
    df_adj.to_csv(f"{folder}mcard_weekly_{area}_international_txn_adj.csv", index=False)

bespoke_adj = pd.read_csv(f"{folder}mcard_weekly_bespoke_international_txn.csv")

# sublicense - colliers
holba_ids = [112, 113, 114, 115, 116, 117, 118, 197]
bespoke_adj[bespoke_adj['bespoke_area_id'].isin(holba_ids)].to_csv(f"{folder}"+"bespoke/Colliers agreement - Holba sites/colliers_mcard_weekly_intl_txn.csv", index=False)
data_writer.upload_data_to_lds(
    slug="colliers---hsds",
    custom_date_column="week_start",
    resource_title="colliers_mcard_weekly_intl_txn.csv",
    file_path=(
        f"{folder}"+"bespoke/Colliers agreement - Holba sites/"
        "colliers_mcard_weekly_intl_txn.csv"
    ),
)

data_writer.upload_data_to_lds(
    slug="mastercard-retail-location-insights-international-spend",
    custom_date_column="week_start",
    resource_title="mcard_weekly_msoa_international_txn_adj.csv",
    file_path=(
        f"{base_dir}mastercard/weekly/processed/"
        "mcard_weekly_msoa_international_txn_adj.csv"
    ),
)
data_writer.upload_data_to_lds(
    slug="mastercard-retail-location-insights-international-spend",
    custom_date_column="week_start",
    resource_title="mcard_weekly_bid_international_txn_adj.csv",
    file_path=(
        f"{base_dir}mastercard/weekly/processed/"
        "mcard_weekly_bid_international_txn_adj.csv"
    ),
)

data_writer.upload_data_to_lds(
    slug="mastercard-retail-location-insights-international-spend",
    custom_date_column="week_start",
    resource_title="mcard_weekly_caz_international_txn_adj.csv",
    file_path=(
        f"{base_dir}mastercard/weekly/processed/"
        "mcard_weekly_caz_international_txn_adj.csv"
    ),
)

data_writer.upload_data_to_lds(
    slug="mastercard-retail-location-insights-international-spend",
    custom_date_column="week_start",
    resource_title="mcard_weekly_bespoke_international_txn_adj.csv",
    file_path=(
        f"{base_dir}mastercard/weekly/processed/"
        "mcard_weekly_bespoke_international_txn_adj.csv"
    ),
)

data_writer.upload_data_to_lds(
    slug="mastercard-retail-location-insights-international-spend",
    custom_date_column="week_start",
    resource_title="mcard_weekly_towncentre_international_txn_adj.csv",
    file_path=(
        f"{base_dir}mastercard/weekly/processed/"
        "mcard_weekly_towncentre_international_txn_adj.csv"
    ),
)
data_writer.upload_data_to_lds(
    slug="mastercard-retail-location-insights-international-spend",
    custom_date_column="week_start",
    resource_title="mcard_weekly_highstreet_international_txn_adj.csv",
    file_path=(
        f"{base_dir}mastercard/weekly/processed/"
        "mcard_weekly_highstreet_international_txn_adj.csv"
    ),
)