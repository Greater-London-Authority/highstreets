import os
import logging
import time
import warnings

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import fsspec
from sqlalchemy import exc as sa_exc

from highstreets import config
from highstreets.data_source_sink.dataloader import DataLoader

warnings.filterwarnings('ignore', message='.*Shapely GEOS version.*incompatible.*')
warnings.filterwarnings(
    'ignore', category=sa_exc.SAWarning, message='.*Did not recognize type.*xml.*')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
logger = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 60  # seconds; doubles each attempt

S3_BASE = f"{config.BASE_DIR}bt/raw/catchment"

POI_TYPES = [
    "towncentres", "bids", "highstreets", "major parks",
    "borough", "custom", "gla boundary",
]

DATASETS = ["visitor", "worker"]

PARQUET_SCHEMA = pa.schema([
    ('poi_id', pa.string()),
    ('poi_name', pa.string()),
    ('home_lsoa', pa.string()),
    ('day_name', pa.string()),
    ('time_indicator', pa.string()),
    ('unique_volume', pa.float64()),
    ('dwell_time', pa.float64()),
])


def partition_path(dataset: str, poi_type: str, year: int, month: int) -> str:
    safe_poi = poi_type.replace(" ", "_")
    return (
        f"{S3_BASE}/{dataset}/poi_type={safe_poi}/"
        f"year={year}/month={month:02d}/data.snappy.parquet"
    )


def file_exists_s3(path: str) -> bool:
    fs = fsspec.filesystem('s3')
    s3_key = path.replace("s3://", "")
    return fs.exists(s3_key)


def write_parquet_to_s3(df: pd.DataFrame, path: str) -> None:
    table = pa.Table.from_pandas(df, schema=PARQUET_SCHEMA, preserve_index=False)
    fs = fsspec.filesystem('s3')
    with fs.open(path, 'wb') as f:
        pq.write_table(table, f, compression='snappy')
    logger.info(f"  Written {len(df):,} rows -> {path}")


def fetch_with_retry(data_loader: DataLoader, dataset: str,
                     month_str: str) -> list:
    """Fetch catchment data with exponential backoff on transient failures."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if dataset == "visitor":
                return data_loader.get_catchment_visitor_data(
                    month_str, month_str)
            else:
                return data_loader.get_catchment_worker_data(
                    month_str, month_str)
        except Exception as e:
            if attempt == MAX_RETRIES:
                raise
            wait = RETRY_BACKOFF_BASE * (2 ** (attempt - 1))
            logger.warning(
                f"  Attempt {attempt}/{MAX_RETRIES} failed: {e}. "
                f"Retrying in {wait}s ..."
            )
            time.sleep(wait)


def process_dataset(data_loader: DataLoader, dataset: str,
                    month_str: str, year: int, month: int) -> None:
    missing = [
        pt for pt in POI_TYPES
        if not file_exists_s3(partition_path(dataset, pt, year, month))
    ]

    if not missing:
        logger.info(
            f"All {dataset} partitions exist for {month_str}, skipping")
        return

    logger.info(
        f"Missing {len(missing)} partition(s) for {dataset} {month_str}: "
        f"{missing}"
    )

    logger.info(f"Fetching {dataset} data for {month_str} ...")
    raw = fetch_with_retry(data_loader, dataset, month_str)

    df = pd.DataFrame(raw)
    logger.info(f"  Received {len(df):,} rows x {len(df.columns)} columns")

    df['unique_volume'] = pd.to_numeric(df['unique_volume'], errors='coerce').astype('float64')
    df['dwell_time'] = pd.to_numeric(df['dwell_time'], errors='coerce').astype('float64')

    for poi_type, group_df in df.groupby('poi_type'):
        path = partition_path(dataset, poi_type, year, month)
        out_df = group_df.drop(columns=['poi_type', 'month']).reset_index(drop=True)
        write_parquet_to_s3(out_df, path)

    logger.info(f"Completed {dataset} for {month_str}")


def main():
    start_date = os.getenv('START_DATE')
    end_date = os.getenv('END_DATE')

    if not start_date or not end_date:
        raise ValueError(
            "Both START_DATE and END_DATE environment variables must be set.")

    target_month = pd.Timestamp(start_date)
    year = target_month.year
    month = target_month.month
    month_str = target_month.strftime("%Y-%m-%d")

    logger.info(f"Catchment pipeline: target month = {month_str}")

    data_loader = DataLoader()

    for dataset in DATASETS:
        process_dataset(data_loader, dataset, month_str, year, month)

    logger.info("Catchment pipeline complete.")


if __name__ == "__main__":
    main()
