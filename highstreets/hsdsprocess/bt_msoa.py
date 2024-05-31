import os

from highstreets import config
from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.data_source_sink.datawriter import DataWriter
from highstreets.data_transformation.msoatransform import MsoaTransform

base_dir = config.BASE_DIR

# Initialise data loader
data_loader = DataLoader()

# Get the start_date and end_date from environment variables
start_date = os.environ.get("START_DATE")
end_date = os.environ.get("END_DATE")

# Retrieve BT MSOA footfall data using API within the specified date range
data = data_loader.get_msoa_data(str(start_date), str(end_date))

# Instantiate MSOATransform class for data transformation
msoa_transform = MsoaTransform()

# Transform the received data
transformed_data = msoa_transform.raw_msoa_transform_data(data)

# Initialize DataWriter for data storage
data_writer = DataWriter()

# Append transformed data to PostgreSQL table
data_writer.append_data_to_postgres(transformed_data, "bt_footfall_msoa_hourly")

# Retrieve full range data from PostgreSQL
msoa_full_range = data_loader.get_full_data("bt_footfall_msoa_hourly")

# Write full range data to CSVs: writes to Q drive
data_writer.write_threehourly_hs_to_csv(msoa_full_range, "bt")

# update data in London Datastore along with start and end dates
data_writer.upload_data_to_lds(
    slug="footfall-bt-people-counts-hsds",
    resource_title="msoa_hourly_counts.csv",
    source="BT",
    poi_type="msoa",
    df=msoa_full_range,
    file_name="msoa_hourly_counts",
)

Westminster_msoa = [
    "Westminster 001",
    "Westminster 002",
    "Westminster 003",
    "Westminster 004",
    "Westminster 005",
    "Westminster 006",
    "Westminster 007",
    "Westminster 008",
    "Westminster 009",
    "Westminster 010",
    "Westminster 011",
    "Westminster 012",
    "Westminster 013",
    "Westminster 014",
    "Westminster 015",
    "Westminster 016",
    "Westminster 017",
    "Westminster 018",
    "Westminster 019",
    "Westminster 020",
    "Westminster 021",
    "Westminster 022",
    "Westminster 023",
    "Westminster 024",
]

ucl_geetanjli_msoa = msoa_full_range[
    msoa_full_range["msoa_name"].isin(Westminster_msoa)
][
    [
        "msoa_id",
        "msoa_name",
        "count_date",
        "day",
        "hour",
        "resident",
        "visitor",
        "worker",
        "loyalty_percentage",
        "dwell_time",
    ]
].to_csv(
    f"{base_dir}/Projects/2019-20/Covid-19 Busyness/data/"
    "BT/Processed/msoa/UCL/Geetanjli/ucl_bt_msoa_hourly_counts.csv",
    index=False,
)

# Offloading the hex data filtered to Westminster to datastore page
data_writer.upload_data_to_lds(
    slug="ucl---geetanjli-rani",
    resource_title="ucl_bt_msoa_hourly_counts.csv",
    df=ucl_geetanjli_msoa,
    file_path=(
        "//onelondon.tfl.local/gla/INTELLIGENCE/Projects/2019-20/Covid-19 Busyness/data/"
        "BT/Processed/msoa/UCL/Geetanjli/ucl_bt_msoa_hourly_counts.csv"
    ),
)
