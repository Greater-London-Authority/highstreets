import os

import pandas as pd

from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.data_source_sink.datawriter import DataWriter
from highstreets.data_transformation.hextransform import HexTransform

data_loader = DataLoader()

# Get the start_date and end_date
start_date = os.environ.get("START_DATE")
end_date = os.environ.get("END_DATE")

data = data_loader.get_hex_data(str(start_date), str(end_date))
hex_transform = HexTransform()
transformed_data = hex_transform.transform_data(data)
print(transformed_data)
data_writer = DataWriter()
# data_writer.append_data_to_postgres(
#     transformed_data, "temp_bt_footfall_tfl_hex_3hourly"
# )

hs = hex_transform.highstreet_threehourly_transform(transformed_data)
tc = hex_transform.towncentre_threehourly_transform(transformed_data)
bid = hex_transform.bid_threehourly_transform(transformed_data)
bespoke = hex_transform.bespoke_threehourly_transform(transformed_data)

data_writer.write_threehourly_hs_to_csv(hs)
data_writer.write_threehourly_hs_to_csv(tc)
data_writer.write_threehourly_hs_to_csv(bid)
data_writer.write_threehourly_hs_to_csv(bespoke)

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

data_writer.append_data_to_postgres(
    econ_busyness_bt_3hourly_counts, "temp_econ_busyness_bt_3hourly_counts"
)
