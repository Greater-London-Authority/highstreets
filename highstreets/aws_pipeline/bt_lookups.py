import logging
import os
import geopandas as gpd
import warnings
from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine, text
from highstreets.data_source_sink.datawriter import DataWriter
from highstreets.data_source_sink.lookup_manager import LookupManager
from highstreets import config
from sqlalchemy import exc as sa_exc
# Suppress GeoPandas GEOS version warnings
warnings.filterwarnings('ignore', message='.*Shapely GEOS version.*incompatible.*')
# Suppress SQLAlchemy XML column warnings
warnings.filterwarnings(
    'ignore', category=sa_exc.SAWarning, message='.*Did not recognize type.*xml.*')
# Optional: Suppress all SQLAlchemy warnings if needed
# warnings.filterwarnings('ignore', category=sa_exc.SAWarning)
print("Warning filters applied for cleaner output")

# Load environment variables from .env file
load_dotenv()
# lookup for mcard refreshed and updated
warnings.filterwarnings('ignore')
base_dir = config.BASE_DIR


def main():
    data_writer = DataWriter()
    lookup_manager = LookupManager()
    df = lookup_manager.process_borough_hs_lookup()
    data_writer.truncate_and_load_to_postgres(
        df,
        table_name='econ_busyness_borough_hs_lookup_3',
        schema='gisapdata')
    lookup_manager.generate_all_quad_lookups()

    # lookups for BT hex refreshed and updated
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
        f"{base_dir}reference_data/shapefiles/hex350_grid_GLA.shp"
    )
    hex_400m_buffer1 = gpd.read_file(
        f"{base_dir}reference_data/shapefiles/hex_400m_buffer1.shp"
    )
    hex_400m_buffer1 = hex_400m_buffer1.rename(columns={"Hex_ID": "hex_id"})

    # Spatially join the two GeoDataFrames based on the geometry intersection
    join_result = gpd.sjoin(hex_400m_buffer1, highstreet, how="left", op="intersects")

    # Select the 'Hex_ID', 'bespoke_ar_id', and 'geometry' columns from the join result
    lookup_table = join_result[["hex_id", "highstreet_id", "highstreet_name"]]

    lookup_table["highstreet_id"] = lookup_table["highstreet_id"].astype("Int64")

    data_writer.truncate_and_load_to_postgres(
        lookup_table,
        table_name="econ_busyness_hex_highstreet_lookup",
        schema="gisapdata",
    )

    # # Save the lookup table as a CSV file
    lookup_table.to_csv(
        f"{base_dir}"
        f"reference_data/hex_highstreet_lookup.csv",
        index=False,
    )

    # Spatially join the two GeoDataFrames based on the geometry intersection
    join_result = gpd.sjoin(hex_400m_buffer1, tc, how="left", op="intersects")

    # Select the 'Hex_ID', 'bespoke_ar_id', and 'geometry' columns from the join result
    lookup_table = join_result[["hex_id", "tc_id", "tc_name"]]

    lookup_table["tc_id"] = lookup_table["tc_id"].astype("Int64")

    data_writer.truncate_and_load_to_postgres(
        lookup_table,
        table_name="econ_busyness_hex_towncentre_lookup",
        schema="gisapdata",
    )

    # # Save the lookup table as a CSV file
    lookup_table.to_csv(
        f"{base_dir}"
        f"reference_data/hex_towncentre_lookup.csv",
        index=False,
    )

    # Spatially join the two GeoDataFrames based on the geometry intersection
    join_result = gpd.sjoin(hex_400m_buffer1, bid, how="left", op="intersects")

    # Select the 'Hex_ID', 'bespoke_ar_id', and 'geometry' columns from the join result
    lookup_table = join_result[["hex_id", "bid_id", "bid_name"]]

    lookup_table["bid_id"] = lookup_table["bid_id"].astype("Int64")

    data_writer.truncate_and_load_to_postgres(
        lookup_table,
        table_name="econ_busyness_hex_bid_lookup",
        schema="gisapdata",
    )

    # # Save the lookup table as a CSV file
    lookup_table.to_csv(
        f"{base_dir}"
        f"reference_data/hex_bid_lookup.csv",
        index=False,
    )

    # Spatially join the two GeoDataFrames based on the geometry intersection
    join_result = gpd.sjoin(hex350_grid_GLA, bespoke, how="left", op="intersects")

    # Select the 'Hex_ID', 'bespoke_ar_id', and 'geometry' columns from the join result
    lookup_table = join_result[["hex_id", "bespoke_area_id", "name"]]

    lookup_table["bespoke_area_id"] = lookup_table["bespoke_area_id"].astype("Int64")

    data_writer.truncate_and_load_to_postgres(
        lookup_table,
        table_name="econ_busyness_hex_bespoke_lookup",
        schema="gisapdata",
    )

    # # Save the lookup table as a CSV file
    lookup_table.to_csv(
        f"{base_dir}"
        f"reference_data/hex_bespoke_lookup.csv",
        index=False,
    )

    # loading hsds_bid_hs_tc table to postgres
    data_writer.load_hsds_lookup_to_postgres()

    # Generate and upload master lookup tables
    logging.info("Generating master lookup tables...")
    quad_to_all = lookup_manager.generate_quad_to_all_lookup()
    hex_to_all = lookup_manager.generate_hex_to_all_lookup()

    if quad_to_all is not None:
        quad_csv = f"{base_dir}reference_data/quad_to_all_lookup.csv"
        data_writer.upload_data_to_lds(
            slug="spend-mastercard-retail-index-3-hourly",
            resource_title="quad_to_all_lookup.csv",
            file_path=quad_csv,
        )
        data_writer.upload_data_to_lds(
            slug="mastercard-retail-location-insights",
            resource_title="quad_to_all_lookup.csv",
            file_path=quad_csv,
        )
    if hex_to_all is not None:
        data_writer.upload_data_to_lds(
            slug="footfall-bt-people-counts-hsds",
            resource_title="hex_to_all_lookup.csv",
            file_path=f"{base_dir}reference_data/hex_to_all_lookup.csv",
        )


if __name__ == "__main__":
    main()
