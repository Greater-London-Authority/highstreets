import os
import warnings
from highstreets import config
from highstreets.core.sql_manager import SQLManager
from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.data_source_sink.datawriter import DataWriter
from highstreets.data_transformation.dailytransform import DailyTransform
from sqlalchemy import exc as sa_exc
# Suppress GeoPandas GEOS version warnings
warnings.filterwarnings('ignore', message='.*Shapely GEOS version.*incompatible.*')
# Suppress SQLAlchemy XML column warnings
warnings.filterwarnings(
    'ignore', category=sa_exc.SAWarning, message='.*Did not recognize type.*xml.*')
# Optional: Suppress all SQLAlchemy warnings if needed
# warnings.filterwarnings('ignore', category=sa_exc.SAWarning)
print("Warning filters applied for cleaner output")

base_dir = config.BASE_DIR


def main():
    # Initialise data loader
    data_loader = DataLoader()

    # Get the start_date and end_date from environment variables
    start_date = os.getenv('START_DATE')
    end_date = os.getenv('END_DATE')

    if not start_date or not end_date:
        raise ValueError(
            "Both START_DATE and END_DATE environment variables must be provided.")

    # Debugging log for clarity
    print(f"Making API request for daily totals data"
          f" with start_date: {start_date}, end_date: {end_date}")

    # Retrieve BT footfall data using API within the specified date range
    data = data_loader.get_bt_daily_aggregate_customer_shapes(str(start_date), str(end_date))

    # Instantiate Daily transform
    daily_transform = DailyTransform()
    # daily total count - aggregated shapes api by BT
    data = daily_transform.raw_bt_daily_preprocess_data(data)

    # Instantiate data writer
    data_writer = DataWriter()
    data_writer.append_data_to_postgres(
        data, table_name="econ_busyness_bt_daily_agg_cust_raw")

    sql_manager = SQLManager()
    enrichment_query = sql_manager.get_query(
        'daily_agg_borough_enriched', category='bt')

    data_writer.export_table_by_partition_to_s3(
        table_name='econ_busyness_bt_daily_agg_cust_raw',
        partition_column='poi_type',
        s3_base_path=f"{base_dir}bt/processed/daily",
        file_prefix='BT_daily_agg_counts',
        source_query=enrichment_query
    )

    poi_types = data_writer.get_distinct_values(
        'econ_busyness_bt_daily_agg_cust_raw', 'poi_type')
    for poi_type in poi_types:
        safe_name = poi_type.replace(' ', '_')
        data_writer.upload_data_to_lds(
            slug="footfall-bt-daily-people-counts-hsds",
            custom_date_column="count_date",
            resource_title=f"BT_daily_agg_counts_{safe_name}.csv",
            file_path=(
                f"{base_dir}"
                f"bt/processed/daily/BT_daily_agg_counts_{safe_name}.csv"
            ),
        )


if __name__ == "__main__":
    main()
