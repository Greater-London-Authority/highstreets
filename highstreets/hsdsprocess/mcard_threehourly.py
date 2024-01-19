import pandas as pd

from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.data_source_sink.datawriter import DataWriter
from highstreets.data_transformation.mcard_transform import McardTransform

data_loader = DataLoader()
mcard_latest_df = data_loader.mcard_3hourly_latest_data_read(
    "//DC1-FILE01/Intelligence$/Projects/2019-20/Covid-19 Busyness/"
    "data/mastercard/sharefile_3hr_timeslot"
)

mcard_transform = McardTransform()
mcard_latest_df_transformed = mcard_transform.preprocess_mcard_data(mcard_latest_df)

data_writer = DataWriter()
data_writer.append_data_to_postgres(
    mcard_latest_df_transformed, "econ_busyness_mrli_3hourly"
)

# Retrieve full range mastercard 3hrly quad data from PostgreSQL and write to CSV
mrli_full_range_df = data_loader.get_full_data("econ_busyness_mrli_3hourly")

data_writer.write_hex_to_csv_by_year(
    mrli_full_range_df,
    output_dir="//DC1-FILE01/Intelligence$/Projects/2019-20/Covid-19 Busyness/data/"
    "mastercard/Processed/MRLI_3yr_compressed",
    custom_file_name="MRLI_3yr_compressed",
)

mrli_hs_full_range = mcard_transform.mcard_highstreet_threehourly_transform(
    mrli_full_range_df
)
mrli_tc_full_range = mcard_transform.mcard_towncentre_threehourly_transform(
    mrli_full_range_df
)
mrli_bid_full_range = mcard_transform.mcard_bid_threehourly_transform(
    mrli_full_range_df
)
mrli_bespoke_full_range = mcard_transform.mcard_bespoke_threehourly_transform(
    mrli_full_range_df
)

data_writer.truncate_and_load_to_postgres(
    mrli_hs_full_range,
    table_name="econ_busyness_mcard_highstreets_3hourly_txn",
    schema="gisapdata",
)
data_writer.truncate_and_load_to_postgres(
    mrli_tc_full_range,
    table_name="econ_busyness_mcard_towncentres_3hourly_txn",
    schema="gisapdata",
)
data_writer.truncate_and_load_to_postgres(
    mrli_bid_full_range,
    table_name="econ_busyness_mcard_bids_3hourly_txn",
    schema="gisapdata",
)
data_writer.truncate_and_load_to_postgres(
    mrli_bespoke_full_range,
    table_name="econ_busyness_mcard_bespokes_3hourly_txn",
    schema="gisapdata",
)


data_writer.write_threehourly_hs_to_csv(mrli_bespoke_full_range, "mastercard")
data_writer.write_threehourly_hs_to_csv(mrli_hs_full_range, "mastercard")
data_writer.write_threehourly_hs_to_csv(mrli_tc_full_range, "mastercard")
data_writer.write_threehourly_hs_to_csv(mrli_bid_full_range, "mastercard")
data_writer.write_threehourly_hs_to_csv(mrli_bespoke_full_range, "mastercard")

# sub-licensing agreement for colliers
# process HSDS data for the HOLBA sites
# select ids cooresponding to HOLBA sites
holba_ids = [112, 113, 114, 115, 116, 117, 118, 197]

# filtering all holba site footfall data and writing it to csv
mrli_bespoke_full_range[
    mrli_bespoke_full_range["bespoke_area_id"].isin(holba_ids)
].to_csv(
    "//DC1-FILE01/Intelligence$/Projects/2019-20/Covid-19 Busyness"
    "/data/mastercard/Processed/bespoke/"
    "Colliers agreement - Holba sites/"
    "colliers_hsds_mcard_3hourly_txn.csv",
    index=False,
)

# Andrew Scott Project
ltn_ids = [220, 221, 222, 223, 224, 225, 226, 227, 228]
# filtering ltn weekly transaction data and writing it to csv
mrli_bespoke_full_range[
    mrli_bespoke_full_range["bespoke_area_id"].isin(ltn_ids)
].to_csv(
    "//DC1-FILE01/Intelligence$/Projects/"
    "2019-20/Covid-19 Busyness/data/mastercard/Processed/bespoke/"
    "LTN/ltn_hsds_mcard_3hourly_txn.csv",
    index=False,
)

# Concatenate latest data from different layers
econ_busyness_mcard_3hourly_txn = pd.concat(
    [
        mrli_hs_full_range.assign(layer="highstreets").rename(
            columns={
                "highstreet_id": "id",
                "highstreet_name": "name",
                "txn_amt": "txn_amt_retail",
            }
        ),
        mrli_tc_full_range.assign(layer="towncentres").rename(
            columns={"tc_id": "id", "tc_name": "name", "txn_amt": "txn_amt_retail"}
        ),
        mrli_bid_full_range.assign(layer="bids").rename(
            columns={"bid_id": "id", "bid_name": "name", "txn_amt": "txn_amt_retail"}
        ),
        mrli_bespoke_full_range.assign(layer="bespoke").rename(
            columns={
                "bespoke_area_id": "id",
                "bespoke_name": "name",
                "txn_amt": "txn_amt_retail",
            }
        ),
    ]
)

# Select columns for appending to PostgreSQL
econ_busyness_mcard_3hourly_txn = econ_busyness_mcard_3hourly_txn[
    [
        "count_date",
        "hours",
        "id",
        "name",
        "layer",
        "txn_amt_retail",
    ]
].sort_values(["count_date", "layer", "id"])

data_writer.truncate_and_load_to_postgres(
    econ_busyness_mcard_3hourly_txn,
    table_name="econ_busyness_mcard_3hourly_txn",
    schema="gisapdata",
)
