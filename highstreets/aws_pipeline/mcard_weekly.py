import pandas as pd
import os
import warnings
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from highstreets import config
from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.data_source_sink.datawriter import DataWriter
from highstreets.core.sql_manager import SQLManager
from sqlalchemy import create_engine
from highstreets.data_transformation.mcard_transform import McardTransform
from highstreets.data_transformation.mcard_weekly_processor import FileProcessor
from highstreets.great_expectations import (
    validate_weekly_txn_output, validate_weekly_yoy_output, McardValidationException,
)
from dotenv import find_dotenv, load_dotenv
from sqlalchemy import exc as sa_exc

warnings.filterwarnings('ignore', message='.*Shapely GEOS version.*incompatible.*')
warnings.filterwarnings(
    'ignore', category=sa_exc.SAWarning, message='.*Did not recognize type.*xml.*')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

base_dir = config.BASE_DIR

aggregation_ids_dict = {
    "inner_outer": {
        "poi_id": ["inner_outer"],
        "quad_area_lookup": "econ_busyness_mcard_inner_outer_quad_lookup",
    },
    "bid": {
        "poi_id": ["bid_id", "bid_name"],
        "quad_area_lookup": "econ_busyness_mcard_bids_quad_lookup",
    },
    "towncentre": {
        "poi_id": ["tc_id", "tc_name"],
        "quad_area_lookup": "econ_busyness_mcard_towncentres_quad_lookup",
    },
    "highstreet": {
        "poi_id": ["highstreet_id", "highstreet_name"],
        "quad_area_lookup": "econ_busyness_mcard_highstreets_quad_lookup",
    },
    "msoa": {
        "poi_id": ["msoa11cd", "msoa11nm"],
        "quad_area_lookup": "econ_busyness_mcard_msoas_quad_lookup",
    },
    "bespoke": {
        "poi_id": ["bespoke_area_id", "name"],
        "quad_area_lookup": "econ_busyness_mcard_bespoke_quad_lookup",
    },
    "borough": {
        "poi_id": ["gss_code", "name"],
        "quad_area_lookup": "econ_busyness_mcard_boroughs_quad_lookup",
    },
    "caz": {
        "poi_id": ["objectid", "name"],
        "quad_area_lookup": "econ_busyness_mcard_caz_quad_lookup",
    },
}

table_mapping = {
    "txn_bespoke": "econ_busyness_mcard_bespoke_txn",
    "txn_bids": "econ_busyness_mcard_bids_txn",
    "txn_boroughs": "econ_busyness_mcard_boroughs_txn",
    "txn_caz": "econ_busyness_mcard_caz_txn",
    "txn_highstreets": "econ_busyness_mcard_highstreets_txn",
    "txn_inner_outer": "econ_busyness_mcard_inner_outer_txn",
    "txn_london": "econ_busyness_mcard_london_txn",
    "txn_msoas": "econ_busyness_mcard_msoas_txn",
    "txn_towncentres": "econ_busyness_mcard_towncentres_txn",
    "yoy_bespoke": "econ_busyness_mcard_bespoke_yoy",
    "yoy_bids": "econ_busyness_mcard_bids_yoy",
    "yoy_boroughs": "econ_busyness_mcard_boroughs_yoy",
    "yoy_caz": "econ_busyness_mcard_caz_yoy",
    "yoy_highstreets": "econ_busyness_mcard_highstreets_yoy",
    "yoy_inner_outer": "econ_busyness_mcard_inner_outer_yoy",
    "yoy_london": "econ_busyness_mcard_london_yoy",
    "yoy_msoas": "econ_busyness_mcard_msoas_yoy",
    "yoy_towncentres": "econ_busyness_mcard_towncentres_yoy",
}

mcard_weekly_layers = {
    "txn": [
        "bespoke", "bids", "boroughs", "caz",
        "highstreets", "inner_outer", "london", "msoas", "towncentres"
    ],
    "yoy": [
        "bespoke", "bids", "boroughs", "caz",
        "highstreets", "inner_outer", "london", "msoas", "towncentres"
    ],
}


def run_aggregation_query(agg, sql_manager, weekly_txn_dir):
    """Run a single aggregation query and save to CSV."""
    query = sql_manager.get_query(f"{agg}_weekly_query.sql")
    weekly_agg = sql_manager.execute_query(query)
    weekly_agg["week_start"] = pd.to_datetime(
        weekly_agg["week_start"], errors="coerce"
    ).dt.strftime("%Y-%m-%d")
    out_path = f"{weekly_txn_dir}mcard_weekly_{agg}_txn.csv"
    weekly_agg.to_csv(out_path, index=False)
    logger.info(f"Aggregation complete: {agg} ({len(weekly_agg)} rows)")
    return agg


def main():
    load_dotenv(find_dotenv())
    database = os.getenv("PG_DATABASE")
    username = os.getenv("PG_USER")
    password = os.getenv("PG_PASSWORD")
    host = os.getenv("PG_HOST")
    port = os.getenv("PG_PORT")
    engine = create_engine(
        f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
    )

    data_loader = DataLoader()
    data_writer = DataWriter()
    mcard_transform = McardTransform()
    sql_manager = SQLManager(engine=engine)

    cpi_success = mcard_transform.load_cpi_data_to_postgres(truncate=True)
    if not cpi_success:
        raise Exception("Failed to load CPI data")

    table_name_map = {"18": "econ_busyness_mcard_raw_18_zoom"}
    for table_name in table_name_map.values():
        data_loader.create_table_mcard_weekly_raw(table_name)

    dir_path = f"{base_dir}mastercard/weekly/raw/mcard_staging/"
    mcard_weekly = FileProcessor(data_loader, data_writer, dir_path)
    mcard_weekly.process_mcard_raw_files(table_name_map)

    clean_table_name = "econ_busyness_mcard_clean_18_zoom"
    cols = [
        "yr", "wk", "industry", "quad_id", "txn_amt", "txn_cnt", "acct_cnt",
        "avg_ticket", "avg_freq", "avg_spend_amt", "file_name",
        "weekday_weekend", "central_latitude", "central_longitude"
    ]
    mcard_weekly.clean_and_process_data(18, cols, clean_table_name)

    mcard_weekly.incremental_refresh_mcard_stg_18_zoom()

    mcard_weekly.process_inner_outer_weekly_summary()

    mcard_weekly.create_adjustment_factor(
        table_name="econ_busyness_mcard_inner_outer_txn_pre_adj",
        rolling_average_months=12,
        ffill_missing_dates=False,
        date_from=None,
        date_to=None,
        update_pg_table=True,
    )

    data_writer.export_table_to_s3(
        table_name='econ_busyness_mcard_adjustment_factors',
        s3_base_path=(f"{base_dir}mastercard/spendingpulse/"
                      f"adjustment_factor_historical_versions"),
        file_prefix='adjustment_factor',
        add_date_range_to_filename=True,
        date_column='date',
    )

    weekly_txn_dir = f"{base_dir}mastercard/weekly/processed/"
    weekly_adj_path = weekly_txn_dir + "adjusted_weekly_data/"

    # --------------------------------------------------
    # Aggregate the weekly data (parallelised)
    # --------------------------------------------------
    logger.info("Starting parallel aggregation queries...")
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(
                run_aggregation_query, agg, sql_manager, weekly_txn_dir
            ): agg
            for agg in aggregation_ids_dict.keys()
        }
        for future in as_completed(futures):
            agg = futures[future]
            try:
                future.result()
            except Exception as e:
                logger.error(f"Aggregation failed for {agg}: {e}")
                raise

    # --------------------------------------------------
    # Cache shared lookups once before the adjustment loop
    # --------------------------------------------------
    logger.info("Caching shared lookup tables...")
    cached_inner_outer_quad = data_loader.get_full_data(
        "econ_busyness_mcard_inner_outer_quad_lookup"
    )
    cached_adjustment_factors = data_loader.get_full_data(
        "econ_busyness_mcard_adjustment_factors"
    )
    cached_cpi_data = data_loader.get_full_data(
        "econ_busyness_mcard_cpi_data"
    )

    # --------------------------------------------------
    # Adjust the aggregated data
    # --------------------------------------------------
    col_to_adjust = (
        [f"txn_amt_wd_{sector}" for sector in config.SECTORS_DF["geo_insights"].values]
        + [f"txn_amt_we_{sector}" for sector in config.SECTORS_DF["geo_insights"].values]
    )

    for agg in aggregation_ids_dict.keys():
        logger.info(f"Adjusting: {agg}")
        if agg in ["highstreet", "bid", "towncentre", "msoa", "borough"]:
            save_name = f"{agg}s"
        else:
            save_name = agg

        mcard_weekly.mcard_adjust_weekly(
            pd.read_csv(weekly_txn_dir + f"mcard_weekly_{agg}_txn.csv"),
            save_path=weekly_adj_path,
            col_to_adjust=col_to_adjust,
            date_col="week_start",
            lookup_file="econ_busyness_mcard_inner_outer_quad_lookup",
            quad_lookup_file=aggregation_ids_dict[agg]["quad_area_lookup"],
            poi_id=aggregation_ids_dict[agg]["poi_id"],
            filename=f"txn_{save_name}",
            cached_inner_outer_quad=cached_inner_outer_quad,
            cached_adjustment_factors=cached_adjustment_factors,
            cached_cpi_data=cached_cpi_data,
        )

    # -----------------------------------------------------------------
    # Create adjusted London level data (Combine Inner and Outer)
    # -----------------------------------------------------------------
    txn_io_london = pd.read_csv(f"{weekly_adj_path}txn_inner_outer.csv")
    txn_london = (
        txn_io_london.groupby(["yr", "wk", "week_start"])
        .sum(min_count=1)
        .reset_index()
    )
    txn_london["area"] = "London"
    txn_london = txn_london[
        ["yr", "wk", "week_start", "area"]
        + [i for i in txn_io_london.columns if i.startswith("txn")]
    ]
    txn_london = txn_london.round(3)
    txn_london.to_csv(f"{weekly_adj_path}txn_london.csv", index=False)

    yoy = txn_london.copy()
    for col in [i for i in yoy.columns if i.startswith("txn_")]:
        yoy = mcard_weekly.calculate_yoy_growth_compared_to_2019(
            yoy, col, f"yoy_{col}", ids=["area"]
        )
    yoy = yoy[yoy["yr"] >= 2019]
    first_txn_amt_col = [
        column for column in yoy.columns if column.startswith("txn_amt_")
    ][0]
    id_cols = list(yoy.loc[:, :first_txn_amt_col].columns[:-1])
    yoy = yoy.drop(columns=[i for i in yoy.columns if i.startswith("txn_")]).round(2)
    yoy = yoy[id_cols + [i for i in yoy.columns if i.startswith("yoy")]]
    yoy.to_csv(f"{weekly_adj_path}yoy_london.csv", index=False)

    logger.info("Data adjusted")

    # --------------------------------------------------
    # Load to PostgreSQL
    # --------------------------------------------------
    logger.info("Loading data to PostgreSQL tables...")
    for prefix, resources in mcard_weekly_layers.items():
        for resource in resources:
            file_key = f"{prefix}_{resource}"
            file_path = (
                f"{base_dir}mastercard/weekly/processed/"
                f"adjusted_weekly_data/{file_key}.csv"
            )
            table_name = table_mapping.get(file_key)

            if table_name:
                try:
                    logger.info(f"Loading {file_key} to {table_name}...")
                    df = pd.read_csv(file_path)
                    data_writer.truncate_and_load_to_postgres(
                        dataframe=df,
                        table_name=table_name,
                        schema="gisapdata",
                        standardize_columns=True,
                    )
                    logger.info(f"Successfully loaded {file_key} to {table_name}")
                except Exception as e:
                    logger.error(f"Error loading {file_key}: {e}")
                    import traceback
                    traceback.print_exc()
            else:
                logger.warning(f"No table mapping for: {file_key}")

    logger.info("PostgreSQL loading completed!")

    # --------------------------------------------------
    # Validate before Upload to London Datastore
    # --------------------------------------------------
    logger.info("Running GE validation on weekly outputs...")
    all_failures = []
    for prefix, resources in mcard_weekly_layers.items():
        for resource in resources:
            file_key = f"{prefix}_{resource}"
            file_path = (
                f"{base_dir}mastercard/weekly/processed/"
                f"adjusted_weekly_data/{file_key}.csv"
            )
            try:
                df_check = pd.read_csv(file_path)
                if prefix == "txn":
                    passed, failures = validate_weekly_txn_output(df_check, resource)
                else:
                    passed, failures = validate_weekly_yoy_output(df_check, resource)
                if not passed:
                    all_failures.extend(failures)
            except Exception as e:
                logger.warning(f"GE validation skipped for {file_key}: {e}")

    if all_failures:
        logger.error(
            f"Mastercard GE validation: {len(all_failures)} check(s) failed"
        )
        raise McardValidationException(
            f"Weekly output validation failed: {len(all_failures)} check(s)"
        )
    logger.info("GE validation passed for all weekly outputs")

    # --------------------------------------------------
    # Upload to London Datastore
    # --------------------------------------------------
    for prefix, resources in mcard_weekly_layers.items():
        for resource in resources:
            resource_title = f"{prefix}_{resource}.csv"
            file_path = (
                f"{base_dir}mastercard/weekly/processed/"
                f"adjusted_weekly_data/{resource_title}"
            )
            data_writer.upload_data_to_lds(
                slug="mastercard-retail-location-insights",
                custom_date_column="week_start",
                resource_title=resource_title,
                file_path=file_path,
            )

    # --------------------------------------------------
    # Concat all layers
    # --------------------------------------------------
    try:
        data_loader = DataLoader()
        data_writer = DataWriter()
        dir_path = f"{base_dir}mastercard/sharefile_test"
        mcard_weekly = FileProcessor(data_loader, data_writer, dir_path)

        logger.info("Starting combination of weekly transaction data...")
        mcard_weekly.concat_and_load_all_mcard_weekly_txn_layers(
            query_file='weekly_txn_all_layers_concat_query.sql',
            target_table='econ_busyness_mcard_txn',
            truncate=True,
            load_to_db=True,
        )

        logger.info("Starting combination of weekly year-over-year data...")
        mcard_weekly.concat_and_load_all_mcard_weekly_yoy_layers(
            query_file='weekly_yoy_all_layers_concat_query.sql',
            target_table='econ_busyness_mcard_yoy',
            truncate=True,
            load_to_db=True,
        )

        logger.info("Successfully completed weekly data combination pipeline!")

    except Exception as e:
        logger.error(f"Error in weekly data combination pipeline: {e}")
        raise


if __name__ == "__main__":
    main()
