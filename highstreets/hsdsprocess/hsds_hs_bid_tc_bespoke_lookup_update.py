import os

import geopandas as gpd
from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine, text

load_dotenv(find_dotenv())

database = os.getenv("PG_DATABASE")
username = os.getenv("PG_USER")
password = os.getenv("PG_PASSWORD")
host = os.getenv("PG_HOST")
port = os.getenv("PG_PORT")

# Create a database connection
engine = create_engine(f"postgresql://{username}:{password}@{host}:{port}/{database}")

# bid
query = (
    "select bid_id, bid_name, geom from "
    "regen_business_improvement_districts_27700_live"
)
bid = gpd.GeoDataFrame.from_postgis(text(query), engine.connect(), geom_col="geom")

# highstreet
query = (
    "select highstreet_id, highstreet_name, geom " "from regen_high_streets_proposed_2"
)
highstreet = gpd.GeoDataFrame.from_postgis(
    text(query), engine.connect(), geom_col="geom"
)

# towncentre
query = "select tc_id, tc_name, geom from planning_town_centre_all_2020"
tc = gpd.GeoDataFrame.from_postgis(text(query), engine.connect(), geom_col="geom")

# bespoke
query = (
    "select bespoke_area_id, name, geometry from "
    "econ_busyness_bespoke_focus_areas_live"
)
bespoke = gpd.GeoDataFrame.from_postgis(
    text(query), engine.connect(), geom_col="geometry"
)

# Load the .shp file using GeoPandas
hex350_grid_GLA = gpd.read_file(
    "//DC1-FILE01/Intelligence$/Projects/2019-20/Covid-19 Busyness/data/"
    "reference_data/shapefiles/hex350_grid_GLA.shp"
)
hex_400m_buffer1 = gpd.read_file(
    "//DC1-FILE01/Intelligence$/Projects/2019-20/Covid-19 Busyness/data/"
    "reference_data/shapefiles/hex_400m_buffer1.shp"
)
hex_400m_buffer1 = hex_400m_buffer1.rename(columns={"Hex_ID": "hex_id"})

# Spatially join the two GeoDataFrames based on the geometry intersection
join_result = gpd.sjoin(hex_400m_buffer1, highstreet, how="left", op="intersects")

# Select the 'Hex_ID', 'bespoke_ar_id', and 'geometry' columns from the join result
lookup_table = join_result[["hex_id", "highstreet_id", "highstreet_name"]]

lookup_table["highstreet_id"] = lookup_table["highstreet_id"].astype("Int64")

# Save the lookup table as a CSV file
lookup_table.to_csv(
    "//DC1-FILE01/Intelligence$/Projects/2019-20/Covid-19 Busyness/data/reference_data/"
    "hex_highstreet_lookup.csv",
    index=False,
)

# Spatially join the two GeoDataFrames based on the geometry intersection
join_result = gpd.sjoin(hex_400m_buffer1, tc, how="left", op="intersects")

# Select the 'Hex_ID', 'bespoke_ar_id', and 'geometry' columns from the join result
lookup_table = join_result[["hex_id", "tc_id", "tc_name"]]

lookup_table["tc_id"] = lookup_table["tc_id"].astype("Int64")

# Save the lookup table as a CSV file
lookup_table.to_csv(
    "//DC1-FILE01/Intelligence$/Projects/2019-20/Covid-19 Busyness/data/reference_data/"
    "hex_towncentre_lookup.csv",
    index=False,
)

# Spatially join the two GeoDataFrames based on the geometry intersection
join_result = gpd.sjoin(hex_400m_buffer1, bid, how="left", op="intersects")

# Select the 'Hex_ID', 'bespoke_ar_id', and 'geometry' columns from the join result
lookup_table = join_result[["hex_id", "bid_id", "bid_name"]]

lookup_table["bid_id"] = lookup_table["bid_id"].astype("Int64")

# Save the lookup table as a CSV file
lookup_table.to_csv(
    "//DC1-FILE01/Intelligence$/Projects/2019-20/Covid-19 Busyness/data/reference_data/"
    "hex_bid_lookup.csv",
    index=False,
)

# Spatially join the two GeoDataFrames based on the geometry intersection
join_result = gpd.sjoin(hex350_grid_GLA, bespoke, how="left", op="intersects")

# Select the 'Hex_ID', 'bespoke_ar_id', and 'geometry' columns from the join result
lookup_table = join_result[["hex_id", "bespoke_area_id", "name"]]

lookup_table["bespoke_area_id"] = lookup_table["bespoke_area_id"].astype("Int64")

# Save the lookup table as a CSV file
lookup_table.to_csv(
    "//DC1-FILE01/Intelligence$/Projects/2019-20/Covid-19 Busyness/data/reference_data/"
    "hex_bespoke_lookup.csv",
    index=False,
)
