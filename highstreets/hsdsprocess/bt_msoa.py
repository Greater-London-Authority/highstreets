import os

from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.data_source_sink.datawriter import DataWriter
from highstreets.data_transformation.msoatransform import MsoaTransform

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
