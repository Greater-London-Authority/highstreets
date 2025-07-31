import os
import glob
from highstreets import config
from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.data_source_sink.datawriter import DataWriter
from highstreets.data_transformation.lsoatransform import LsoaTransform

base_dir = config.BASE_DIR

# Initialise data loader
data_loader = DataLoader()

# Get the start_date and end_date from environment variables
start_date = os.environ.get("START_DATE")
end_date = os.environ.get("END_DATE")

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

# Retrieve full range data from PostgreSQL
lsoa_full_range = data_loader.get_full_data("bt_footfall_lsoa_hourly")

# Write full range data to CSVs 6 monthly: writes to Q drive
data_writer.write_to_csv_by_year_half(
    lsoa_full_range,
    f"{base_dir}/Projects/2019-20/Covid-19 Busyness/data/BT/" "Processed/lsoa/chunks",
    custom_file_name="lsoa_hourly_counts"
)

# Define the directory containing the CSV files
lsoa_directory = (f"{base_dir}/Projects/2019-20/Covid-19 Busyness/"
                  "data/BT/Processed/lsoa/chunks")

# Get a list of all CSV files starting with 'lsoa_hourly_counts_' in the directory
csv_files = glob.glob(os.path.join(lsoa_directory, 'lsoa_hourly_counts_*.csv'))

# Iterate through the list of CSV files and use the
# data_writer.upload_data_to_lds function
for csv_file in csv_files:
    # Extract the file name from the path
    file_name = os.path.basename(csv_file)

    # Extract the resource title from the file name
    resource_title = file_name

    # Call the function to upload the data
    data_writer.upload_data_to_lds(
        slug='footfall-bt-people-counts-hsds',
        resource_title=resource_title,
        file_path=csv_file
    )
