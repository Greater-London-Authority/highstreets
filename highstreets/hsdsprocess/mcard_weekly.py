import pandas as pd

from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.data_source_sink.datawriter import DataWriter
from highstreets.data_transformation.mcard_transform import McardTransform

# instantiate the classes
data_loader = DataLoader()
data_writer = DataWriter()
mcard_transform = McardTransform()


mcard_london_txn = data_loader.get_full_data("econ_busyness_mcard_london_txn")
mcard_caz_txn = data_loader.get_full_data("econ_busyness_mcard_caz_txn")
mcard_bespoke_txn = data_loader.get_full_data("econ_busyness_mcard_bespoke_txn")
mcard_towncentres_txn = data_loader.get_full_data("econ_busyness_mcard_towncentres_txn")
mcard_bids_txn = data_loader.get_full_data("econ_busyness_mcard_bids_txn")
mcard_highstreets_txn = data_loader.get_full_data("econ_busyness_mcard_highstreets_txn")


# Define the columns and their data types
columns = [
    "yr",
    "wk",
    "week_start",
    "txn_amt_wd_eating",
    "txn_amt_we_eating",
    "txn_amt_wd_apparel",
    "txn_amt_we_apparel",
    "txn_amt_wd_retail",
    "txn_amt_we_retail",
    "txn_cnt_wd_eating",
    "txn_cnt_we_eating",
    "txn_cnt_wd_apparel",
    "txn_cnt_we_apparel",
    "txn_cnt_wd_retail",
    "txn_cnt_we_retail",
    "txn_amt_wd_retail_adj",
    "txn_amt_we_retail_adj",
]

dtypes = {
    "yr": int,
    "wk": int,
    "week_start": "datetime64",
    "txn_amt_wd_eating": int,
    "txn_amt_we_eating": int,
    "txn_amt_wd_apparel": int,
    "txn_amt_we_apparel": int,
    "txn_amt_wd_retail": int,
    "txn_amt_we_retail": int,
    "txn_cnt_wd_eating": int,
    "txn_cnt_we_eating": int,
    "txn_cnt_wd_apparel": int,
    "txn_cnt_we_apparel": int,
    "txn_cnt_wd_retail": int,
    "txn_cnt_we_retail": int,
    "txn_amt_wd_retail_adj": int,
    "txn_amt_we_retail_adj": int,
}

# Create a dictionary to map columns for easier iteration
column_mapping = {
    "eating": ["txn_amt_wd", "txn_amt_we", "txn_cnt_wd", "txn_cnt_we"],
    "apparel": ["txn_amt_wd", "txn_amt_we", "txn_cnt_wd", "txn_cnt_we"],
    "retail": ["txn_amt_wd", "txn_amt_we", "txn_cnt_wd", "txn_cnt_we"],
    "retail_adj": ["txn_amt_wd", "txn_amt_we"],
}


# Create a DataFrame from the data with correct data types
df_london = pd.DataFrame(mcard_london_txn, columns=columns).astype(dtypes)

df_caz = (
    pd.DataFrame(mcard_caz_txn, columns=columns)
    .astype(dtypes)
    .assign(layer="caz", name="caz", id=mcard_caz_txn["objectid"])
)
df_bespoke = (
    pd.DataFrame(mcard_bespoke_txn, columns=columns)
    .astype(dtypes)
    .assign(
        layer="bespoke",
        name=mcard_bespoke_txn["name"],
        id=mcard_bespoke_txn["bespoke_area_id"],
    )
)
df_tc = (
    pd.DataFrame(mcard_towncentres_txn, columns=columns)
    .astype(dtypes)
    .assign(
        layer="towncentres",
        name=mcard_towncentres_txn["tc_name"],
        id=mcard_towncentres_txn["tc_id"],
    )
)
df_bid = (
    pd.DataFrame(mcard_bids_txn, columns=columns)
    .astype(dtypes)
    .assign(layer="bids", name=mcard_bids_txn["bid_name"], id=mcard_bids_txn["bid_id"])
)
df_hs = (
    pd.DataFrame(mcard_highstreets_txn, columns=columns)
    .astype(dtypes)
    .assign(
        layer="highstreets",
        name=mcard_highstreets_txn["highstreet_name"],
        id=mcard_highstreets_txn["highstreet_id"],
    )
)


# Calculate annual change for each transaction type and area
# caz, bespoke, towncentre, bid, highstreet
for txn_type, cols in column_mapping.items():
    for col in cols:
        annual_change_col = f"annual_change_{col}_{txn_type}"
        df_processed_yoy_growth = pd.concat(
            [
                mcard_transform.calculate_yoy_growth(
                    df_caz, f"{col}_{txn_type}", annual_change_col
                ),
                mcard_transform.calculate_yoy_growth(
                    df_bespoke, f"{col}_{txn_type}", annual_change_col
                ),
                mcard_transform.calculate_yoy_growth(
                    df_tc, f"{col}_{txn_type}", annual_change_col
                ),
                mcard_transform.calculate_yoy_growth(
                    df_bid, f"{col}_{txn_type}", annual_change_col
                ),
                mcard_transform.calculate_yoy_growth(
                    df_hs, f"{col}_{txn_type}", annual_change_col
                ),
            ],
            ignore_index=True,
        )


# Select and print the specified output columns
output_columns = ["yr", "wk", "week_start", "id", "name", "layer"] + [
    f"annual_change_{col}_{txn_type}"
    for txn_type, cols in column_mapping.items()
    for col in cols
]

# truncate and write full range data to PG Tables
data_writer.truncate_and_load_to_postgres(
    df_processed_yoy_growth[output_columns],
    "econ_busyness_mcard_annual_change",
    schema="gisapdata",
)
