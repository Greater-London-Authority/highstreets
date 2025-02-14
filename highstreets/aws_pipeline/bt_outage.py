import os
import pandas as pd
from highstreets import config
from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.data_source_sink.datawriter import DataWriter


base_dir = config.BASE_DIR

# Initialise data loader
data_loader = DataLoader()

# Get the start_date and end_date from environment variables
start_date = os.getenv('START_DATE')
end_date = os.getenv('END_DATE')

# Retrieve BT MSOA footfall data using API within the specified date range
data = data_loader.get_bt_outage_history_data(str(start_date), str(end_date))

data = pd.DataFrame(data)

data = pd.DataFrame(data)
data = data.rename(columns={"date": "count_date"})
data = data[data['region'] == 'London'].reset_index(drop=True)

# Initialize DataWriter for data storage
data_writer = DataWriter()

# Append transformed data to PostgreSQL table
data_writer.append_data_to_postgres(data, "econ_busyness_bt_outage_data")

# Retrieve full range data from PostgreSQL
bt_outage_data_full_range = data_loader.get_full_data("econ_busyness_bt_outage_data")

# filtering all holba site footfall data and writing it to csv
bt_outage_data_full_range.to_csv(
    f"{base_dir}"
    "bt/processed/outage/"
    "bt_outage_data.csv",
    index=False,
)

# Offloading Holba site data to datastore
data_writer.upload_data_to_lds(
    slug="footfall-bt-people-counts-hsds",
    resource_title="bt_outage_data.csv",
    file_path=(
        f"{base_dir}"
        "bt/processed/outage/"
        "bt_outage_data.csv"
    ),
)
