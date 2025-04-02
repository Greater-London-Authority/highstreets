import pandas as pd

from highstreets import config
from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.data_source_sink.datawriter import DataWriter
from highstreets.data_transformation.mcard_transform import McardTransform

base_dir = config.BASE_DIR

data_loader = DataLoader()
mcard_latest_df = data_loader.mcard_3hourly_latest_data_read(
    f"{base_dir}mastercard/mrli_3hourly/raw/"
)

mcard_transform = McardTransform()
mcard_latest_df_transformed = mcard_transform.preprocess_mcard_data(mcard_latest_df)

data_writer = DataWriter()
data_writer.append_data_to_postgres(
    mcard_latest_df_transformed, "econ_busyness_mrli_3hourly"
)

# Retrieve full range mastercard 3hrly quad data from PostgreSQL and write to CSV
mrli_full_range_df = data_loader.get_full_data("econ_busyness_mrli_3hourly")

# Transform full range mastercard 3hrly data with added txn adjusted column
spend_adj_full_range = mcard_transform.mcard_adjust(
    mrli_full_range_df, col_to_adjust='txn_amt',
    adj_col='adjustment_factor_retail', date_col='count_date')
spend_adj_full_range = spend_adj_full_range[
    ['ldn_ref', 'quad_id', 'count_date', 'hours', 'txn_amt', 'txn_cnt', 'txn_amt_adj']]
adjustment_factor = pd.read_csv(config.ADJUSTMENT_FACTOR_DIR)

# Find the maximum year and month in mastercard ajustment data
max_year = adjustment_factor['yr'].max()
max_month = adjustment_factor[adjustment_factor['yr'] == max_year]['month'].max()

# Find cut-off date for the month (last day of the month)
cutoff_date = pd.Timestamp(
    year=max_year, month=max_month, day=1) + pd.offsets.MonthEnd(0)

# filtering spend data until the maximum month and year in spend pulse data
spend_adj_full_range = spend_adj_full_range[
    spend_adj_full_range['count_date'] <= cutoff_date]

data_writer.append_data_to_postgres(
    spend_adj_full_range, "econ_busyness_mrli_3hourly_adj")

data_writer.write_hex_to_csv_by_year(
    spend_adj_full_range,
    output_dir=f"{base_dir}"
    "mastercard/mrli_3hourly/processed/MRLI_3yr_compressed",
    custom_file_name="MRLI_3yr_compressed_adj",
)

data_writer.write_hex_to_csv_by_year(
    mrli_full_range_df,
    output_dir=f"{base_dir}"
    "mastercard/mrli_3hourly/processed/MRLI_3yr_compressed",
    custom_file_name="MRLI_3yr_compressed",
)
data_writer.upload_data_to_lds(
    slug="spend-mastercard-retail-index-3-hourly",
    resource_title="MRLI_3yr_compressed_2022.csv",
    file_path=(
        f"{base_dir}"
        f"mastercard/mrli_3hourly/processed/MRLI_3yr_compressed/"
        f"MRLI_3yr_compressed_2022.csv"
    ),
)

data_writer.upload_data_to_lds(
    slug="spend-mastercard-retail-index-3-hourly",
    resource_title="MRLI_3yr_compressed_2023.csv",
    file_path=(
        f"{base_dir}"
        f"mastercard/mrli_3hourly/processed/MRLI_3yr_compressed/"
        f"MRLI_3yr_compressed_2023.csv"
    ),
)

data_writer.upload_data_to_lds(
    slug="spend-mastercard-retail-index-3-hourly",
    resource_title="MRLI_3yr_compressed_2024.csv",
    file_path=(
        f"{base_dir}"
        f"mastercard/mrli_3hourly/processed/MRLI_3yr_compressed/"
        f"MRLI_3yr_compressed_2024.csv"
    ),
)

data_writer.upload_data_to_lds(
    slug="spend-mastercard-retail-index-3-hourly",
    resource_title="MRLI_3yr_compressed_2025.csv",
    file_path=(
        f"{base_dir}"
        f"mastercard/mrli_3hourly/processed/MRLI_3yr_compressed/"
        f"MRLI_3yr_compressed_2025.csv"
    ),
)

data_writer.upload_data_to_lds(
    slug="spend-mastercard-retail-index-3-hourly",
    resource_title="MRLI_3yr_compressed_adj_2022.csv",
    file_path=(
        f"{base_dir}"
        f"mastercard/mrli_3hourly/processed/MRLI_3yr_compressed/"
        f"MRLI_3yr_compressed_adj_2022.csv"
    ),
)

data_writer.upload_data_to_lds(
    slug="spend-mastercard-retail-index-3-hourly",
    resource_title="MRLI_3yr_compressed_adj_2023.csv",
    file_path=(
        f"{base_dir}"
        f"mastercard/mrli_3hourly/processed/MRLI_3yr_compressed/"
        f"MRLI_3yr_compressed_adj_2023.csv"
    ),
)

data_writer.upload_data_to_lds(
    slug="spend-mastercard-retail-index-3-hourly",
    resource_title="MRLI_3yr_compressed_adj_2024.csv",
    file_path=(
        f"{base_dir}"
        f"mastercard/mrli_3hourly/processed/MRLI_3yr_compressed/"
        f"MRLI_3yr_compressed_adj_2024.csv"
    ),
)

data_writer.upload_data_to_lds(
    slug="spend-mastercard-retail-index-3-hourly",
    resource_title="MRLI_3yr_compressed_adj_2025.csv",
    file_path=(
        f"{base_dir}"
        f"mastercard/mrli_3hourly/processed/MRLI_3yr_compressed/"
        f"MRLI_3yr_compressed_adj_2025.csv"
    ),
)

mrli_hs_full_range = mcard_transform.mcard_highstreet_threehourly_transform(
    spend_adj_full_range
)
mrli_tc_full_range = mcard_transform.mcard_towncentre_threehourly_transform(
    spend_adj_full_range
)
mrli_bid_full_range = mcard_transform.mcard_bid_threehourly_transform(
    spend_adj_full_range
)
mrli_bespoke_full_range = mcard_transform.mcard_bespoke_threehourly_transform(
    spend_adj_full_range
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
data_writer.write_threehourly_hs_to_csv(mrli_bespoke_full_range, "mastercard_3hourly")
data_writer.write_threehourly_hs_to_csv(mrli_hs_full_range, "mastercard_3hourly")
data_writer.write_threehourly_hs_to_csv(mrli_tc_full_range, "mastercard_3hourly")
data_writer.write_threehourly_hs_to_csv(mrli_bid_full_range, "mastercard_3hourly")


# update data in London Datastore along with start and end dates
data_writer.upload_data_to_lds(
    slug="spend-mastercard-retail-index-3-hourly",
    resource_title="highstreets_3hourly_txn.csv",
    source="mastercard_3hourly",
    poi_type="highstreet",
    df=mrli_hs_full_range,
    file_name="highstreet_3hourly_txn",
)

data_writer.upload_data_to_lds(
    slug="spend-mastercard-retail-index-3-hourly",
    resource_title="bespoke_3hourly_txn.csv",
    source="mastercard_3hourly",
    poi_type="bespoke",
    df=mrli_bespoke_full_range,
    file_name="bespoke_3hourly_txn",
)

data_writer.upload_data_to_lds(
    slug="spend-mastercard-retail-index-3-hourly",
    resource_title="towncentres_3hourly_txn.csv",
    source="mastercard_3hourly",
    poi_type="towncentre",
    df=mrli_tc_full_range,
    file_name="towncentre_3hourly_txn",
)

data_writer.upload_data_to_lds(
    slug="spend-mastercard-retail-index-3-hourly",
    resource_title="bids_3hourly_txn.csv",
    source="mastercard_3hourly",
    poi_type="bid",
    df=mrli_bid_full_range,
    file_name="bid_3hourly_txn",
)


quad_borough_lookup = pd.read_csv(
    f"{base_dir}"
    "reference_data/mcard_grid_ldn_ref_HS_TC_BID_CAZ_Borough_lookup2.csv",
    usecols=['quad_id', 'borough_name']
)

# sub-license: westminster University

data_writer.upload_data_to_lds(
    slug="westminster-university",
    resource_title="Mastercard_3hourly_2022.csv",
    file_path=(
        f"{base_dir}"
        f"mastercard/mrli_3hourly/processed/"
        f"MRLI_3yr_compressed/MRLI_3yr_compressed_adj_2022.csv"
    ),
)

data_writer.upload_data_to_lds(
    slug="westminster-university",
    resource_title="Mastercard_3hourly_2023.csv",
    file_path=(
        f"{base_dir}"
        f"mastercard/mrli_3hourly/processed/MRLI_3yr_compressed/"
        f"MRLI_3yr_compressed_adj_2023.csv"
    ),
)

data_writer.upload_data_to_lds(
    slug="westminster-university",
    resource_title="Mastercard_3hourly_2024.csv",
    file_path=(
        f"{base_dir}"
        f"mastercard/mrli_3hourly/processed/MRLI_3yr_compressed/"
        f"MRLI_3yr_compressed_adj_2024.csv"
    ),
)

data_writer.upload_data_to_lds(
    slug="westminster-university",
    resource_title="Mastercard_3hourly_2025.csv",
    file_path=(
        f"{base_dir}"
        f"mastercard/mrli_3hourly/processed/MRLI_3yr_compressed/"
        f"MRLI_3yr_compressed_adj_2025.csv"
    ),
)


# sub-licensing agreement for colliers
# process HSDS data for the HOLBA sites
# select ids cooresponding to HOLBA sites
holba_ids = [112, 113, 114, 115, 116, 117, 118, 197]

# filtering all holba site footfall data and writing it to csv
mrli_bespoke_full_range[
    mrli_bespoke_full_range["bespoke_area_id"].isin(holba_ids)
].to_csv(
    f"{base_dir}"
    "mastercard/mrli_3hourly/processed/bespoke/"
    "Colliers agreement - Holba sites/"
    "colliers_hsds_mcard_3hourly_txn.csv",
    index=False,
)

# Offloading Holba Site 3hourly txn data to datastore
data_writer.upload_data_to_lds(
    slug="colliers---hsds",
    resource_title="colliers_hsds_mcard_3hourly_txn.csv",
    df=mrli_bespoke_full_range[
        mrli_bespoke_full_range["bespoke_area_id"].isin(holba_ids)
    ],
    file_path=(
        f"{base_dir}"
        "mastercard/mrli_3hourly/processed/bespoke/"
        "Colliers agreement - Holba sites/"
        "colliers_hsds_mcard_3hourly_txn.csv"
    ),
)

# Sublicenses - Fitzrovia & Knightsbridge

fitzrovia_ids = [21, 77]
knightsbridge_ids = [64, 69]

BIDS_quad_lookup = data_loader.get_full_data("econ_busyness_mcard_BIDs_quad_lookup")
BIDS_quad_lookup['bid_id'] = BIDS_quad_lookup['bid_id'].astype('Int64')
BIDS_quad_lookup['quad_id'] = BIDS_quad_lookup['quad_id'].astype('Int64')

fitzrovia_mrli = spend_adj_full_range.merge(
    BIDS_quad_lookup[BIDS_quad_lookup["bid_id"].isin(fitzrovia_ids)],
    left_on="quad_id",
    right_on="quad_id",
    how="right",
)
knightsbridge_mrli = spend_adj_full_range.merge(
    BIDS_quad_lookup[BIDS_quad_lookup["bid_id"].isin(knightsbridge_ids)],
    left_on="quad_id",
    right_on="quad_id",
    how="right",
)

columns_mrli_bid = [
    "ldn_ref",
    "quad_id",
    "bid_name",
    "count_date",
    "hours",
    "txn_amt",
    "txn_cnt",
    "txn_amt_adj",
]

fitzrovia_mrli[columns_mrli_bid].assign(hours=lambda x: "'" + x["hours"])[
    columns_mrli_bid
].to_csv(
    f"{base_dir}"
    "mastercard/mrli_3hourly/processed/MRLI_3yr_compressed/Fitzrovia/"
    "Fitzrovia_mcard_quad_3hourly_txn.csv",
    index=False,
)
knightsbridge_mrli[columns_mrli_bid].assign(hours=lambda x: "'" + x["hours"])[
    columns_mrli_bid
].to_csv(
    f"{base_dir}"
    "mastercard/mrli_3hourly/processed/MRLI_3yr_compressed/knightsbridge/"
    "Knightsbridge_mcard_quad_3hourly_txn.csv",
    index=False,
)

# Offloading Fitzrovia 3hourly txn data to datastore
data_writer.upload_data_to_lds(
    slug="rendle-intelligence-for-fitzrovia-partnership",
    resource_title="Fitzrovia_mcard_quad_3hourly_txn.csv",
    df=fitzrovia_mrli[columns_mrli_bid],
    file_path=(
        f"{base_dir}"
        "mastercard/mrli_3hourly/processed/MRLI_3yr_compressed/Fitzrovia/"
        "Fitzrovia_mcard_quad_3hourly_txn.csv"
    ),
)

# Offloading Knightsbridge 3hourly txn data to datastore
data_writer.upload_data_to_lds(
    slug="rendle-intelligence-for-knightsbridge-partnership",
    resource_title="Knightsbridge_mcard_quad_3hourly_txn.csv",
    df=knightsbridge_mrli[columns_mrli_bid],
    file_path=(
        f"{base_dir}"
        "mastercard/mrli_3hourly/processed/MRLI_3yr_compressed/knightsbridge/"
        "Knightsbridge_mcard_quad_3hourly_txn.csv"
    ),
)

# sublicense - jon_puleson
jon_puleston_ids = [46]

jon_puleston_mrli = spend_adj_full_range.merge(
    BIDS_quad_lookup[BIDS_quad_lookup["bid_id"].isin(jon_puleston_ids)],
    left_on="quad_id",
    right_on="quad_id",
    how="right",
)

jon_puleston_mrli[columns_mrli_bid].assign(hours=lambda x: "'" + x["hours"])[
    columns_mrli_bid
].to_csv(
    f"{base_dir}"
    "mastercard/mrli_3hourly/processed/MRLI_3yr_compressed/jon_puleson/"
    "jon_puleson_mcard_quad_3hourly_txn.csv",
    index=False,
)
# Offloading jon_puleson data to datastore
data_writer.upload_data_to_lds(
    slug="jon-puleston-for-station-to-station-bid",
    resource_title="jon_puleson_mcard_quad_3hourly_txn.csv",
    file_path=(
        f"{base_dir}"
        "mastercard/mrli_3hourly/processed/MRLI_3yr_compressed/jon_puleson/"
        "jon_puleson_mcard_quad_3hourly_txn.csv"
    ),
)

# towncentre quad data request aveson young

aveson_young_tc_ids = [23, 33, 29, 37, 28, 46, 31]


# TownCentres_quad_lookup = pd.read_csv(
#     f"{base_dir}"
#     "reference_data/TownCentres_quad_lookup.csv"
# )
TownCentres_quad_lookup = data_loader.get_full_data(
    "econ_busyness_mcard_TownCentres_quad_lookup")
TownCentres_quad_lookup['tc_id'] = TownCentres_quad_lookup['tc_id'].astype('Int64')
TownCentres_quad_lookup['quad_id'] = TownCentres_quad_lookup['quad_id'].astype('Int64')

aveson_young_tc = spend_adj_full_range.merge(
    TownCentres_quad_lookup[TownCentres_quad_lookup["tc_id"].isin(aveson_young_tc_ids)],
    left_on="quad_id",
    right_on="quad_id",
    how="right",
)
columns_mrli_tc = [
    "ldn_ref",
    "quad_id",
    "tc_name",
    "count_date",
    "hours",
    "txn_amt",
    "txn_cnt",
    "txn_amt_adj",
]

aveson_young_tc[columns_mrli_tc].assign(hours=lambda x: "'" + x["hours"])[
    columns_mrli_tc
].to_csv(
    f"{base_dir}"
    "mastercard/mrli_3hourly/processed/MRLI_3yr_compressed/aveson_young/"
    "aveson_young_tc_mcard_quad_3hourly_txn.csv",
    index=False,
)
data_writer.upload_data_to_lds(
    slug="avison-young",
    resource_title="avison_young_tc_mcard_quad_3hourly_txn.csv",
    file_path=(
        f"{base_dir}"
        "mastercard/mrli_3hourly/processed/MRLI_3yr_compressed/"
        "aveson_young/"
        "aveson_young_tc_mcard_quad_3hourly_txn.csv"
    ),
)

# bespoke areas - quads
# sublicense - aveson young
aveson_young_bespoke_ids = [249]

bespoke_quad_lookup = data_loader.get_full_data(
    "econ_busyness_mcard_bespoke_quad_lookup")
bespoke_quad_lookup['quad_id'] = bespoke_quad_lookup['quad_id'].astype('Int64')
bespoke_quad_lookup['bespoke_area_id'] = bespoke_quad_lookup[
    'bespoke_area_id'].astype('Int64')
# bespoke_quad_lookup = pd.read_csv(
#     f"{base_dir}"
#     "reference_data/bespoke_quad_lookup.csv"
# )
aveson_young_bespoke = spend_adj_full_range.merge(
    bespoke_quad_lookup[bespoke_quad_lookup[
        "bespoke_area_id"].isin(aveson_young_bespoke_ids)],
    left_on="quad_id",
    right_on="quad_id",
    how="right",
)
columns_mrli_bespoke = [
    "ldn_ref",
    "quad_id",
    "name",
    "count_date",
    "hours",
    "txn_amt",
    "txn_cnt",
    "txn_amt_adj",
]

aveson_young_bespoke[columns_mrli_bespoke].assign(hours=lambda x: "'" + x["hours"])[
    columns_mrli_bespoke
].to_csv(
    f"{base_dir}"
    "mastercard/mrli_3hourly/processed/MRLI_3yr_compressed/aveson_young/"
    "aveson_young_bespoke_mcard_quad_3hourly_txn.csv",
    index=False,
)

data_writer.upload_data_to_lds(
    slug="avison-young",
    resource_title="avison_young_bespoke_mcard_quad_3hourly_txn.csv",
    file_path=(
        f"{base_dir}"
        "mastercard/mrli_3hourly/processed/MRLI_3yr_compressed/"
        "aveson_young/"
        "aveson_young_bespoke_mcard_quad_3hourly_txn.csv"
    ),
)

# Concatenate latest data from different layers
econ_busyness_mcard_3hourly_txn = pd.concat(
    [
        mrli_hs_full_range.assign(layer="highstreets").rename(
            columns={
                "highstreet_id": "id",
                "highstreet_name": "name",
                "txn_amt": "txn_amt_retail",
                "txn_amt_adj": "txn_amt_retail_adj"
            }
        ),
        mrli_tc_full_range.assign(layer="towncentres").rename(
            columns={"tc_id": "id", "tc_name": "name", "txn_amt": "txn_amt_retail",
                     "txn_amt_adj": "txn_amt_retail_adj"}
        ),
        mrli_bid_full_range.assign(layer="bids").rename(
            columns={"bid_id": "id", "bid_name": "name", "txn_amt": "txn_amt_retail",
                     "txn_amt_adj": "txn_amt_retail_adj"}
        ),
        mrli_bespoke_full_range.assign(layer="bespoke").rename(
            columns={
                "bespoke_area_id": "id",
                "bespoke_name": "name",
                "txn_amt": "txn_amt_retail",
                "txn_amt_adj": "txn_amt_retail_adj"
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
        "txn_amt_retail_adj",
    ]
].sort_values(["count_date", "layer", "id"])

data_writer.truncate_and_load_to_postgres(
    econ_busyness_mcard_3hourly_txn,
    table_name="econ_busyness_mcard_3hourly_txn",
    schema="gisapdata",
)
