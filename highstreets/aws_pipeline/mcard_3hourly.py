from highstreets.core.utils import list_files
import os
import re
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

cpi_success = mcard_transform.load_cpi_data_to_postgres(truncate=True)
if not cpi_success:
    raise Exception("Failed to load CPI data")

adjustment_success = mcard_transform.adjust_mcard_data_sql()
if not adjustment_success:
    raise Exception("Failed to adjust Mastercard data")


data_writer.export_table_by_year_to_s3(
    table_name='econ_busyness_mrli_3hourly_adj',
    date_column='count_date',
    s3_base_path=f"{base_dir}mastercard/mrli_3hourly/processed/MRLI_3yr_compressed",
    file_prefix='MRLI_3yr_compressed_adj',
    latest=True
)

# add here to offload hex data to s3
data_writer.export_table_by_year_to_s3(
    table_name='econ_busyness_mrli_3hourly',
    date_column='count_date',
    s3_base_path=f"{base_dir}mastercard/mrli_3hourly/processed/MRLI_3yr_compressed",
    file_prefix='MRLI_3yr_compressed',
    latest=True
)

# automatic upload for Mastercard 3-hourly data to London Datastore

# Base paths for regular and adjusted files
base_path = f"{base_dir}mastercard/mrli_3hourly/processed/MRLI_3yr_compressed/"

# Pattern for file types to upload
file_patterns = [
    "MRLI_3yr_compressed_\\d{4}\\.csv",
    "MRLI_3yr_compressed_adj_\\d{4}\\.csv"
]

for pattern in file_patterns:
    # Get files matching the current pattern
    matching_files = list_files(base_path, pattern)

    for file_path in matching_files:
        # Extract filename from full path
        file_name = os.path.basename(file_path)

        # Extract year from filename using regex
        year_match = re.search(r'(\d{4})\.csv$', file_name)
        if year_match:
            year = year_match.group(1)

            data_writer.upload_data_to_lds(
                slug="spend-mastercard-retail-index-3-hourly",
                resource_title=file_name,
                file_path=f"s3://{file_path}"
            )
            print(f"Uploaded {file_name} for year {year}")


mcard_transform.fetch_and_transform_mcard_data(
    transform_layer='quad_hs_transform_query.sql',
    table_name='econ_busyness_mcard_highstreets_3hourly_txn',
    truncate=True
)
mcard_transform.fetch_and_transform_mcard_data(
    transform_layer='quad_tc_transform_query.sql',
    table_name='econ_busyness_mcard_towncentres_3hourly_txn',
    truncate=True
)
mcard_transform.fetch_and_transform_mcard_data(
    transform_layer='quad_bid_transform_query.sql',
    table_name='econ_busyness_mcard_bids_3hourly_txn',
    truncate=True
)
mcard_transform.fetch_and_transform_mcard_data(
    transform_layer='quad_bespoke_transform_query.sql',
    table_name='econ_busyness_mcard_bespokes_3hourly_txn',
    truncate=True
)


data_writer.export_table_to_s3(table_name='econ_busyness_mcard_bids_3hourly_txn',
                               s3_base_path=(f"{base_dir}mastercard/mrli_3hourly/"
                                             f"processed""/bid"),
                               file_prefix='bid_3hourly_txn',
                               add_date_range_to_filename=True,
                               date_column='count_date')
data_writer.export_table_to_s3(table_name='econ_busyness_mcard_highstreets_3hourly_txn',
                               s3_base_path=(f"{base_dir}mastercard/mrli_3hourly/"
                                             f"processed""/highstreet"),
                               file_prefix='highstreet_3hourly_txn',
                               add_date_range_to_filename=True,
                               date_column='count_date')
data_writer.export_table_to_s3(table_name='econ_busyness_mcard_towncentres_3hourly_txn',
                               s3_base_path=(f"{base_dir}mastercard/mrli_3hourly/"
                                             f"processed""/towncentre"),
                               file_prefix='towncentre_3hourly_txn',
                               add_date_range_to_filename=True,
                               date_column='count_date')
data_writer.export_table_to_s3(table_name='econ_busyness_mcard_bespokes_3hourly_txn',
                               s3_base_path=(f"{base_dir}mastercard/mrli_3hourly/"
                                             f"processed""/bespoke"),
                               file_prefix='bespoke_3hourly_txn',
                               add_date_range_to_filename=True,
                               date_column='count_date')


mrli_hs_full_range = data_loader.get_full_data(
    "econ_busyness_mcard_highstreets_3hourly_txn")
mrli_tc_full_range = data_loader.get_full_data(
    "econ_busyness_mcard_towncentres_3hourly_txn")
mrli_bid_full_range = data_loader.get_full_data(
    "econ_busyness_mcard_bids_3hourly_txn")
mrli_bespoke_full_range = data_loader.get_full_data(
    "econ_busyness_mcard_bespokes_3hourly_txn")
spend_adj_full_range = data_loader.get_full_data(
    "econ_busyness_mrli_3hourly_adj")


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
mcard_transform.concat_and_load_all_mcard_quad_layers(
    query_file='quad_all_layer_concat_query.sql',
    target_table='econ_busyness_mcard_3hourly_txn',
    truncate=True,
    load_to_db=True
)
