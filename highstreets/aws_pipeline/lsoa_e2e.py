import os
import glob
import fsspec
from highstreets import config
from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.data_source_sink.datawriter import DataWriter
from highstreets.data_transformation.lsoatransform import LsoaTransform

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
data = data_loader.get_lsoa_data(str(start_date), str(end_date))

# Instantiate MSOATransform class for data transformation
lsoa_transform = LsoaTransform()

# Transform the received data
transformed_data = lsoa_transform.raw_lsoa_transform_data(data)

# Initialize DataWriter for data storage
data_writer = DataWriter()

# Append transformed data to PostgreSQL table
data_writer.append_data_to_postgres(transformed_data, "bt_footfall_lsoa_hourly")

data_writer.export_table_by_year_half_to_s3(
    table_name='bt_footfall_lsoa_hourly',
    date_column='count_date',
    s3_base_path=f"{base_dir}bt/processed/lsoa/chunks",
    file_prefix='lsoa_hourly_counts')

# Define the directory containing the CSV files
# Define the directory containing the CSV files (make sure to include base_dir correctly)
lsoa_directory = os.path.join(base_dir, "bt/processed/lsoa/chunks")

# Check if the directory is an S3 path
if lsoa_directory.startswith("s3://"):
    # Use fsspec to list files in the S3 bucket
    fs = fsspec.filesystem("s3")
    csv_files = fs.glob(f"{lsoa_directory}/lsoa_hourly_counts_*.csv")
    # Add 's3://' prefix to files that might not have it
    csv_files = [
        file if file.startswith("s3://") else f"s3://{file}" for file in csv_files]
else:
    # Use glob for local file system
    csv_files = glob.glob(os.path.join(lsoa_directory, "lsoa_hourly_counts_*.csv"))

# Iterate through the list of CSV files and use the data_writer.
# upload_data_to_lds function
for csv_file in csv_files:
    # For S3, ensure the file path is accessible via fsspec
    if csv_file.startswith("s3://"):
        # Use fsspec to open the S3 file path
        fs = fsspec.filesystem("s3")
        with fs.open(csv_file, 'rb') as f:
            # Extract the file name from the S3 path
            file_name = os.path.basename(csv_file)

            # Extract the resource title from the file name
            resource_title = file_name

            # Call the function to upload the data
            data_writer.upload_data_to_lds(
                slug='footfall-bt-people-counts-hsds',
                resource_title=resource_title,
                file_path=csv_file
            )
    else:
        # For local files, use the regular file path
        file_name = os.path.basename(csv_file)
        resource_title = file_name

        # Call the function to upload the data
        data_writer.upload_data_to_lds(
            slug='footfall-bt-people-counts-hsds',
            resource_title=resource_title,
            file_path=csv_file
        )
