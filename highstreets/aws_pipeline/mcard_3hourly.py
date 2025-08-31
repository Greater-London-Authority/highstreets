from highstreets.core.utils import list_files
import os
import re
import warnings
from highstreets import config
from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.data_source_sink.datawriter import DataWriter
from highstreets.data_transformation.mcard_transform import McardTransform
from sqlalchemy import exc as sa_exc
# Suppress GeoPandas GEOS version warnings
warnings.filterwarnings('ignore', message='.*Shapely GEOS version.*incompatible.*')
# Suppress SQLAlchemy XML column warnings
warnings.filterwarnings(
    'ignore', category=sa_exc.SAWarning, message='.*Did not recognize type.*xml.*')
# Optional: Suppress all SQLAlchemy warnings if needed
# warnings.filterwarnings('ignore', category=sa_exc.SAWarning)
print("Warning filters applied for cleaner output")

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
    latest=True,
    apostrophe_columns=['hours']
)

# add here to offload hex data to s3
data_writer.export_table_by_year_to_s3(
    table_name='econ_busyness_mrli_3hourly',
    date_column='count_date',
    s3_base_path=f"{base_dir}mastercard/mrli_3hourly/processed/MRLI_3yr_compressed",
    file_prefix='MRLI_3yr_compressed',
    latest=True,
    apostrophe_columns=['hours']
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


latest_file_path_bid = data_writer.export_table_to_s3(
    table_name='econ_busyness_mcard_bids_3hourly_txn',
    s3_base_path=(f"{base_dir}mastercard/mrli_3hourly/processed/bid"),
    file_prefix='bid_3hourly_txn',
    add_date_range_to_filename=True,
    date_column='count_date',
    apostrophe_columns=['hours'])
latest_file_path_highstreet = data_writer.export_table_to_s3(
    table_name='econ_busyness_mcard_highstreets_3hourly_txn',
    s3_base_path=(f"{base_dir}mastercard/mrli_3hourly/processed/highstreet"),
    file_prefix='highstreet_3hourly_txn',
    add_date_range_to_filename=True,
    date_column='count_date',
    apostrophe_columns=['hours'])
latest_file_path_towncentre = data_writer.export_table_to_s3(
    table_name='econ_busyness_mcard_towncentres_3hourly_txn',
    s3_base_path=(f"{base_dir}mastercard/mrli_3hourly/processed/towncentre"),
    file_prefix='towncentre_3hourly_txn',
    add_date_range_to_filename=True,
    date_column='count_date',
    apostrophe_columns=['hours'])
latest_file_path_bespoke = data_writer.export_table_to_s3(
    table_name='econ_busyness_mcard_bespokes_3hourly_txn',
    s3_base_path=(f"{base_dir}mastercard/mrli_3hourly/processed/bespoke"),
    file_prefix='bespoke_3hourly_txn',
    add_date_range_to_filename=True,
    date_column='count_date',
    apostrophe_columns=['hours'])

# upload to lds

data_writer.upload_data_to_lds(
    slug="spend-mastercard-retail-index-3-hourly",
    resource_title="bids_3hourly_txn.csv",
    file_path=latest_file_path_bid
)
data_writer.upload_data_to_lds(
    slug="spend-mastercard-retail-index-3-hourly",
    resource_title="highstreets_3hourly_txn.csv",
    file_path=latest_file_path_highstreet
)

data_writer.upload_data_to_lds(
    slug="spend-mastercard-retail-index-3-hourly",
    resource_title="towncentres_3hourly_txn.csv",
    file_path=latest_file_path_towncentre
)

data_writer.upload_data_to_lds(
    slug="spend-mastercard-retail-index-3-hourly",
    resource_title="bespoke_3hourly_txn.csv",
    file_path=latest_file_path_bespoke
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

# Concatenate latest data from different layers
mcard_transform.concat_and_load_all_mcard_quad_layers(
    query_file='quad_all_layer_concat_query.sql',
    target_table='econ_busyness_mcard_3hourly_txn',
    truncate=True,
    load_to_db=True
)
