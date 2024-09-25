import os
from highstreets import config
from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.data_source_sink.datawriter import DataWriter
from highstreets.data_transformation.dailytransform import DailyTransform

base_dir = config.BASE_DIR

# Initialise data loader
data_loader = DataLoader()

# Get the start_date and end_date from environment variables
start_date = os.environ.get("START_DATE")
end_date = os.environ.get("END_DATE")

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

data_daily_full_range = data_loader.get_full_data(
    "econ_busyness_bt_daily_agg_cust_raw")

# writing to csv by year
data_writer.write_hex_to_csv_by_year(
    data_daily_full_range,
    output_dir=f"{base_dir}/Projects/2019-20/"
    "Covid-19 Busyness/data/BT/Processed/daily",
    custom_file_name="BT_daily_agg_counts",
)

# offloading to London datastore
data_writer.upload_data_to_lds(
    slug="footfall-bt-daily-people-counts-hsds",
    custom_date_column="count_date",
    resource_title="BT_daily_agg_counts_2022.csv",
    file_path=(
        "//onelondon.tfl.local/gla/INTELLIGENCE/Projects/2019-20/Covid-19 Busyness/data/"
        "BT/Processed/daily/BT_daily_agg_counts_2022.csv"
    ),
)
data_writer.upload_data_to_lds(
    slug="footfall-bt-daily-people-counts-hsds",
    custom_date_column="count_date",
    resource_title="BT_daily_agg_counts_2023.csv",
    file_path=(
        "//onelondon.tfl.local/gla/INTELLIGENCE/Projects/2019-20/Covid-19 Busyness/data/"
        "BT/Processed/daily/BT_daily_agg_counts_2023.csv"
    ),
)
data_writer.upload_data_to_lds(
    slug="footfall-bt-daily-people-counts-hsds",
    custom_date_column="count_date",
    resource_title="BT_daily_agg_counts_2024.csv",
    file_path=(
        "//onelondon.tfl.local/gla/INTELLIGENCE/Projects/2019-20/Covid-19 Busyness/data/"
        "BT/Processed/daily/BT_daily_agg_counts_2024.csv"
    ),
)
