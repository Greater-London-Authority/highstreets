import os
import warnings
from highstreets import config
from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.data_source_sink.datawriter import DataWriter
from highstreets.data_transformation.msoatransform import MsoaTransform
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

# Initialise data loader
data_loader = DataLoader()

# Get the start_date and end_date from environment variables
start_date = os.getenv('START_DATE')
end_date = os.getenv('END_DATE')

if not start_date or not end_date:
    raise ValueError(
        "Both START_DATE and END_DATE environment variables must be provided.")


print(f"Making API request with start_date: {start_date}, end_date: {end_date}")

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

# Export transformed data to S3
latest_file_path = data_writer.export_table_to_s3(
    table_name='bt_footfall_msoa_hourly',
    s3_base_path=(f"{base_dir}bt/processed/msoa"),
    file_prefix='msoa_hourly_counts',
    add_date_range_to_filename=True,
    date_column='count_date')

data_writer.upload_data_to_lds(
    slug="footfall-bt-people-counts-hsds",
    resource_title="msoa_hourly_counts.csv",
    file_path=latest_file_path
)
