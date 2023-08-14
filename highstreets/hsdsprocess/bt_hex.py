import os

import pandas as pd

from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.data_source_sink.datawriter import DataWriter
from highstreets.data_source_sink.gischangetracking import DataProcessor
from highstreets.data_transformation.hextransform import HexTransform

# Initialise data loader
data_loader = DataLoader()

# Get the start_date and end_date from environment variables
start_date = os.environ.get("START_DATE")
end_date = os.environ.get("END_DATE")

# Retrieve BT footfall data using API within the specified date range
data = data_loader.get_hex_data(str(start_date), str(end_date))

# Initialize HexTransform for data transformation
hex_transform = HexTransform()

# Transform the received data
transformed_data = hex_transform.transform_data(data)
print(transformed_data)

# Initialize DataWriter for data storage
data_writer = DataWriter()

# Append transformed data to PostgreSQL table
data_writer.append_data_to_postgres(
    transformed_data, "temp_bt_footfall_tfl_hex_3hourly"
)

# get full range BT hex data from postgres and write to csv
tfl_hex_full_range = data_loader.get_full_data("temp_bt_footfall_tfl_hex_3hourly")

# Retrieve full range BT hex data from PostgreSQL and write to CSV
data_writer.write_hex_to_csv_by_year(
    tfl_hex_full_range,
    "/mnt/q/Projects/2019-20/Covid-19 Busyness" "/data/BT/test/test2",
)

# Initialize DataProcessor to obtain new hex IDs from tracking table
data_processor = DataProcessor()
data_processor.process_changes()

# Separate hex IDs from different types of layers
hs_hex_insert = pd.DataFrame(
    data_processor.hs_hex_ids, columns=["hex_id", "highstreet_id"]
)
# (similar for new_tc_data, new_bid_data, new_bespoke_data)
tc_hex_insert = pd.DataFrame(data_processor.tc_hex_ids, columns=["hex_id", "tc_id"])
bid_hex_insert = pd.DataFrame(data_processor.bid_hex_ids, columns=["hex_id", "bid_id"])
bespoke_hex_insert = pd.DataFrame(
    data_processor.bespoke_hex_ids, columns=["hex_id", "bespoke_area_id"]
)

# Merge new hex IDs with full range data for each type of layer
new_hs_data = hs_hex_insert.merge(
    tfl_hex_full_range, left_on="hex_id", right_on="hex_id", how="left"
)[
    [
        "hex_id",
        "count_date",
        "day",
        "time_indicator",
        "resident",
        "worker",
        "visitor",
        "loyalty_percentage",
        "dwell_time",
    ]
]
new_tc_data = tc_hex_insert.merge(
    tfl_hex_full_range, left_on="hex_id", right_on="hex_id", how="left"
)[
    [
        "hex_id",
        "count_date",
        "day",
        "time_indicator",
        "resident",
        "worker",
        "visitor",
        "loyalty_percentage",
        "dwell_time",
    ]
]
new_bid_data = bid_hex_insert.merge(
    tfl_hex_full_range, left_on="hex_id", right_on="hex_id", how="left"
)[
    [
        "hex_id",
        "count_date",
        "day",
        "time_indicator",
        "resident",
        "worker",
        "visitor",
        "loyalty_percentage",
        "dwell_time",
    ]
]
new_bespoke_data = bespoke_hex_insert.merge(
    tfl_hex_full_range, left_on="hex_id", right_on="hex_id", how="left"
)[
    [
        "hex_id",
        "count_date",
        "day",
        "time_indicator",
        "resident",
        "worker",
        "visitor",
        "loyalty_percentage",
        "dwell_time",
    ]
]

# Apply transformations to obtain 3-hourly aggregates for different layers
hs = hex_transform.highstreet_threehourly_transform(transformed_data)
tc = hex_transform.towncentre_threehourly_transform(transformed_data)
bid = hex_transform.bid_threehourly_transform(transformed_data)
bespoke = hex_transform.bespoke_threehourly_transform(transformed_data)

# Transform new boundary data for each layer based on IDs
bespoke_new = pd.DataFrame()
hs_new = pd.DataFrame()
tc_new = pd.DataFrame()
bid_new = pd.DataFrame()
# (conditional transformations similar for each layer)
if not new_bespoke_data.empty:
    bespoke_new = hex_transform.bespoke_threehourly_transform(new_bespoke_data)
    bespoke_new = bespoke_new[
        bespoke_new["bespoke_area_id"].isin(
            bespoke_hex_insert["bespoke_area_id"].unique()
        )
    ]
if not new_hs_data.empty:
    hs_new = hex_transform.highstreet_threehourly_transform(new_hs_data)
    hs_new = hs_new[
        hs_new["highstreet_id"].isin(hs_hex_insert["highstreet_id"].unique())
    ]
if not new_tc_data.empty:
    tc_new = hex_transform.towncentre_threehourly_transform(new_tc_data)
    tc_new = tc_new[tc_new["tc_id"].isin(tc_hex_insert["tc_id"].unique())]
if not new_bid_data.empty:
    bid_new = hex_transform.bid_threehourly_transform(new_bid_data)
    bid_new = bid_new[bid_new["bid_id"].isin(bid_hex_insert["bid_id"].unique())]

# Remove duplicated data in latest BT data already present in new boundary data
hs = hs[~hs["highstreet_id"].isin(hs_hex_insert["highstreet_id"].unique())]
# (similar operations for tc, bid, bespoke)
tc = tc[~tc["tc_id"].isin(tc_hex_insert["tc_id"].unique())]
bid = bid[~bid["bid_id"].isin(bid_hex_insert["bid_id"].unique())]
bespoke = bespoke[
    ~bespoke["bespoke_area_id"].isin(bespoke_hex_insert["bespoke_area_id"].unique())
]

# Append new boundary data to PostgreSQL if not already present
data_sources = [
    (
        hs_new,
        hs_hex_insert,
        "highstreet_id",
        "temp_econ_busyness_bt_highstreets_3hourly_counts",
    ),
    (
        tc_new,
        tc_hex_insert,
        "tc_id",
        "temp_econ_busyness_bt_towncentres_3hourly_counts",
    ),
    (bid_new, bid_hex_insert, "bid_id", "temp_econ_busyness_bt_bids_3hourly_counts"),
    (
        bespoke_new,
        bespoke_hex_insert,
        "bespoke_area_id",
        "temp_econ_busyness_bt_bespokes_3hourly_counts",
    ),
]

for data, hex_insert, id_column, table_name in data_sources:
    # ... (append data with ID check)
    # ... (append transformed data without check)
    ids_to_check = hex_insert[id_column].unique()
    if data_writer.append_data_with_id_check(data, ids_to_check, id_column, table_name):
        layer = id_column.split("_")[0]  # Extract 'highstreet', 'tc', etc.
        data = (
            data.assign(layer=layer)
            .rename(columns={id_column: "id", f"{layer}_name": "name"})[
                [
                    "count_date",
                    "hours",
                    "id",
                    "name",
                    "layer",
                    "resident",
                    "visitor",
                    "worker",
                    "ave_loyalty_percentage",
                    "ave_dwell_time",
                ]
            ]
            .sort_values("count_date")
        )
        data_writer.append_data_without_check(
            data, "temp_econ_busyness_bt_3hourly_counts"
        )


# Append latest transformed BT data of different layers to
# respective tables in PostgreSQL
data_writer.append_data_to_postgres(hs, "econ_busyness_bt_highstreets_3hourly_counts")
# (similar operations for tc, bid, bespoke)
data_writer.append_data_to_postgres(tc, "econ_busyness_bt_towncentres_3hourly_counts")
data_writer.append_data_to_postgres(bid, "econ_busyness_bt_bids_3hourly_counts")
data_writer.append_data_to_postgres(bespoke, "econ_busyness_bt_bespokes_3hourly_counts")

# Retrieve full range data from PostgreSQL
hs_full_range = data_loader.get_full_data("econ_busyness_bt_highstreets_3hourly_counts")
tc_full_range = data_loader.get_full_data("econ_busyness_bt_towncentres_3hourly_counts")
bid_full_range = data_loader.get_full_data("econ_busyness_bt_bids_3hourly_counts")
bespoke_full_range = data_loader.get_full_data(
    "econ_busyness_bt_bespokes" "_3hourly_counts"
)

# Write full range data to CSVs
data_writer.write_threehourly_hs_to_csv(hs_full_range)
data_writer.write_threehourly_hs_to_csv(tc_full_range)
data_writer.write_threehourly_hs_to_csv(bid_full_range)
data_writer.write_threehourly_hs_to_csv(bespoke_full_range)

# Concatenate latest data from different layers
econ_busyness_bt_3hourly_counts = pd.concat(
    [
        hs.assign(layer="highstreets").rename(
            columns={"highstreet_id": "id", "highstreet_name": "name"}
        ),
        tc.assign(layer="towncentres").rename(
            columns={"tc_id": "id", "tc_name": "name"}
        ),
        bid.assign(layer="bids").rename(columns={"bid_id": "id", "bid_name": "name"}),
        bespoke.assign(layer="bespoke").rename(
            columns={"bespoke_area_id": "id", "bespoke_name": "name"}
        ),
    ]
)

# Select columns for appending to PostgreSQL
econ_busyness_bt_3hourly_counts = econ_busyness_bt_3hourly_counts[
    [
        "count_date",
        "hours",
        "id",
        "name",
        "layer",
        "resident",
        "visitor",
        "worker",
        "ave_loyalty_percentage",
        "ave_dwell_time",
    ]
].sort_values("count_date")

# Append concatenated data to PostgreSQL
data_writer.append_data_to_postgres(
    econ_busyness_bt_3hourly_counts, "temp_econ_busyness_bt_3hourly_counts"
)
