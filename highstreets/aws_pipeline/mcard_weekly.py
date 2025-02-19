import pandas as pd

from highstreets import config
from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.data_source_sink.datawriter import DataWriter
from highstreets.data_transformation.mcard_transform import McardTransform

from highstreets.data_transformation.mcard_weekly_processor import FileProcessor
base_dir = config.BASE_DIR


# instantiate the classes
data_loader = DataLoader()
data_writer = DataWriter()
mcard_transform = McardTransform()
dir_path = f"{base_dir}mastercard/sharefile_test"
mcard_weekly = FileProcessor(data_loader, data_writer, dir_path)

# Adjustment Factor generation using Spending Pulse
mcard_weekly.create_adjustment_factor(table_name='econ_busyness_mcard_raw_18_zoom',
                                      rolling_average_months=12,
                                      ffill_missing_dates=False)

mcard_london_txn = data_loader.get_full_data("econ_busyness_mcard_london_txn")
mcard_caz_txn = data_loader.get_full_data("econ_busyness_mcard_caz_txn")
mcard_bespoke_txn = data_loader.get_full_data("econ_busyness_mcard_bespoke_txn")
mcard_towncentres_txn = data_loader.get_full_data("econ_busyness_mcard_towncentres_txn")
mcard_bids_txn = data_loader.get_full_data("econ_busyness_mcard_bids_txn")
mcard_highstreets_txn = data_loader.get_full_data("econ_busyness_mcard_highstreets_txn")
borough_quad_lookup = data_loader.get_full_data(
    'econ_busyness_mcard_Boroughs_quad_lookup')
inner_outer_quad_lookup = data_loader.get_full_data(
    'econ_busyness_mcard_Inner_Outer_quad_lookup')
bespoke_quad_lookup = data_loader.get_full_data(
    'econ_busyness_mcard_bespoke_quad_lookup')
highstreet_quad_lookup = data_loader.get_full_data(
    'econ_busyness_mcard_Highstreets_quad_lookup')
bid_quad_lookup = data_loader.get_full_data(
    'econ_busyness_mcard_BIDs_quad_lookup')
msoa_quad_lookup = data_loader.get_full_data(
    'econ_busyness_mcard_MSOAs_quad_lookup')
towncentre_quad_lookup = data_loader.get_full_data(
    'econ_busyness_mcard_TownCentres_quad_lookup')
caz_quad_lookup = data_loader.get_full_data(
    'econ_busyness_mcard_CAZ_quad_lookup')

# Define a configuration dictionary for each dataset
datasets = [
    {
        "data_key": "econ_busyness_mcard_boroughs_txn",
        "poi_id": "name",
        "quad_lookup_file": borough_quad_lookup,
        "filename": "txn_boroughs"
    },
    {
        "data_key": "econ_busyness_mcard_bespoke_txn",
        "poi_id": "name",
        "quad_lookup_file": bespoke_quad_lookup,
        "filename": "txn_bespoke"
    },
    {
        "data_key": "econ_busyness_mcard_msoas_txn",
        "poi_id": "msoa11nm",
        "quad_lookup_file": msoa_quad_lookup,
        "filename": "txn_msoas"
    },
    {
        "data_key": "econ_busyness_mcard_caz_txn",
        "poi_id": "name",
        "quad_lookup_file": caz_quad_lookup,
        "filename": "txn_caz"
    },
    {
        "data_key": "econ_busyness_mcard_towncentres_txn",
        "poi_id": "tc_name",
        "quad_lookup_file": towncentre_quad_lookup,
        "filename": "txn_towncentres"
    },
    {
        "data_key": "econ_busyness_mcard_highstreets_txn",
        "poi_id": "highstreet_name",
        "quad_lookup_file": highstreet_quad_lookup,
        "filename": "txn_highstreets"
    },
    {
        "data_key": "econ_busyness_mcard_bids_txn",
        "poi_id": "bid_name",
        "quad_lookup_file": bid_quad_lookup,
        "filename": "txn_bids",
        "unique_index": 2  # Custom index for unique extraction
    }
]

# Loop through each dataset and process it
for dataset in datasets:
    txn = data_loader.get_full_data(dataset["data_key"])
    poi_id = dataset["poi_id"]
    # Handle unique indexing logic
    unique_index = dataset.get("unique_index", 0)  # Default to 0 if not specified
    poi_name = txn[poi_id].unique()[unique_index]

    mcard_weekly.mcard_adjust_weekly(
        txn,
        col_to_adjust=[
            'txn_amt_wd_retail', 'txn_amt_we_retail',
            'txn_amt_wd_eating', 'txn_amt_we_eating',
            'txn_amt_wd_apparel', 'txn_amt_we_apparel'
        ],
        date_col='week_start',
        lookup_file=inner_outer_quad_lookup,
        quad_lookup_file=dataset["quad_lookup_file"],
        poi_id=poi_id,
        filename=dataset["filename"]
    )

# Process the 'inner_outer' dataset separately
txn = data_loader.get_full_data('econ_busyness_mcard_inner_outer_txn')
poi_id = 'inner_outer'

adj_inner_outer = mcard_weekly.mcard_adjust_weekly(
    txn,
    col_to_adjust=[
        'txn_amt_wd_retail', 'txn_amt_we_retail',
        'txn_amt_wd_eating', 'txn_amt_we_eating',
        'txn_amt_wd_apparel', 'txn_amt_we_apparel'
    ],
    date_col='week_start',
    lookup_file=None,
    quad_lookup_file="",
    poi_id=poi_id,
    filename='txn_inner_outer'
)

# Post-adjustment processing for 'inner_outer'
adj_inner_outer = adj_inner_outer.groupby(['week_start', 'yr', 'wk']).sum().reset_index()
adj_inner_outer['area'] = 'London'
adj_inner_outer = adj_inner_outer[['yr', 'wk', 'week_start', 'area'] + [
    col for col in adj_inner_outer if col.startswith('txn_')]]
adj_inner_outer.to_csv(f"{base_dir}mastercard/weekly/processed/txn_london.csv",
                       index=False)

# Calculate YoY
filename = 'txn_london'
yoy = adj_inner_outer.copy()
for col in [col for col in yoy.columns if col.startswith('txn_')]:
    yoy = mcard_weekly.calculate_yoy_growth_compared_to_2019(
        yoy, col, f'yoy_{col}', ids='area')

yoy = yoy[yoy['yr'] != 2018].drop(
    columns=[col for col in yoy.columns if col.startswith('txn_')]).reset_index(
        drop=True).round(3)
yoy.to_csv(
    f"{base_dir}mastercard/weekly/processed/yoy{filename.split('txn')[1]}.csv",
    index=False)

# below few lines of code is reading mastercard weekly txn bespoke data
# And filtering the data for various users

# reading full range txn_bespoke data from mastercard directory
mcard_weekly = pd.read_csv(
    f"{base_dir}mastercard/weekly/processed/txn_bespoke.csv"
)

# sub-licensing agreement for colliers
# process HSDS data for the HOLBA sites
# select ids cooresponding to HOLBA sites
holba_ids = [112, 113, 114, 115, 116, 117, 118, 197]

# filtering all holba site weekly transaction data and writing it to csv
mcard_weekly[mcard_weekly["bespoke_area_id"].isin(holba_ids)].to_csv(
    f"{base_dir}"
    "mastercard/weekly/processed/bespoke/"
    "Colliers agreement - Holba sites/colliers_hsds_mcard_weekly_txn.csv",
    index=False,
)

# Offloading Holba Site 3hourly txn data to datastore
data_writer.upload_data_to_lds(
    slug="colliers---hsds",
    custom_date_column="week_start",
    resource_title="colliers_hsds_mcard_weekly_txn.csv",
    df=mcard_weekly[mcard_weekly["bespoke_area_id"].isin(holba_ids)],
    file_path=(
        f"{base_dir}"
        "mastercard/weekly/processed/bespoke/"
        "Colliers agreement - Holba sites/"
        "colliers_hsds_mcard_weekly_txn.csv"
    ),
)

# sub-licensing agreement for south bank - holba all sites
southbank_holba_ids = [197]

# filtering southbank all holba site weekly transaction data and writing it to csv
mcard_weekly[mcard_weekly["bespoke_area_id"].isin(southbank_holba_ids)].to_csv(
    f"{base_dir}"
    "mastercard/weekly/processed/bespoke/"
    "southbank/southbank_holba_mcard_weekly_txn.csv",
    index=False,
)
# Offloading southbank all Holba Site 3hourly txn data to datastore
data_writer.upload_data_to_lds(
    slug="-rendle-intelligence-for-southbank-bid",
    custom_date_column="week_start",
    resource_title="southbank_holba_mcard_weekly_txn.csv",
    df=mcard_weekly[mcard_weekly["bespoke_area_id"].isin(southbank_holba_ids)],
    file_path=(
        f"{base_dir}"
        "mastercard/weekly/processed/bespoke/"
        "southbank/"
        "southbank_holba_mcard_weekly_txn.csv"
    ),
)

# Sublicenses - Fitzrovia & Knightsbridge

fitzrovia_ids = [21, 77]
knightsbridge_ids = [64, 69]

# reading full range txn_bid data from mastercard directory
mcard_weekly_bid = pd.read_csv(
    f"{base_dir}mastercard/weekly/processed/txn_bids.csv"
)

# filtering all holba site weekly transaction data and writing it to csv
mcard_weekly_bid[mcard_weekly_bid["bid_id"].isin(fitzrovia_ids)].to_csv(
    f"{base_dir}"
    "mastercard/weekly/processed/bid/"
    "fitzrovia/Fitzrovia_hsds_mcard_weekly_txn.csv",
    index=False,
)
mcard_weekly_bid[mcard_weekly_bid["bid_id"].isin(knightsbridge_ids)].to_csv(
    f"{base_dir}"
    "mastercard/weekly/processed/bid/"
    "knightsbridge/Knightsbridge_hsds_mcard_weekly_txn.csv",
    index=False,
)

# Offloading Fitzrovia bid weekly txn data to datastore
data_writer.upload_data_to_lds(
    slug="rendle-intelligence-for-fitzrovia-partnership",
    custom_date_column="week_start",
    resource_title="Fitzrovia_hsds_mcard_weekly_txn.csv",
    df=mcard_weekly_bid[mcard_weekly_bid["bid_id"].isin(fitzrovia_ids)],
    file_path=(
        f"{base_dir}"
        "mastercard/weekly/processed/bid/fitzrovia/Fitzrovia_hsds_mcard_weekly_txn.csv"
    ),
)

# Offloading Knightsbridge bid weekly txn data to datastore
data_writer.upload_data_to_lds(
    slug="rendle-intelligence-for-knightsbridge-partnership",
    custom_date_column="week_start",
    resource_title="Knightsbridge_hsds_mcard_weekly_txn.csv",
    df=mcard_weekly_bid[mcard_weekly_bid["bid_id"].isin(knightsbridge_ids)],
    file_path=(
        f"{base_dir}"
        f"mastercard/weekly/processed/bid/knightsbridge/"
        f"Knightsbridge_hsds_mcard_weekly_txn.csv"
    ),
)

# sub-licensing agreement for south bank - holba all sites
southbank_bid_ids = [35, 23, 16, 24]

# filtering southbank all holba site weekly transaction data and writing it to csv
mcard_weekly_bid[mcard_weekly_bid["bid_id"].isin(southbank_bid_ids)].to_csv(
    f"{base_dir}"
    "mastercard/weekly/processed/bid/"
    "southbank/southbank_bids_mcard_weekly_txn.csv",
    index=False,
)

# Offloading southbank all Holba Site 3hourly txn data to datastore
data_writer.upload_data_to_lds(
    slug="-rendle-intelligence-for-southbank-bid",
    custom_date_column="week_start",
    resource_title="southbank_bids_mcard_weekly_txn.csv",
    df=mcard_weekly_bid[mcard_weekly_bid["bid_id"].isin(southbank_bid_ids)],
    file_path=(
        f"{base_dir}"
        "mastercard/weekly/processed/bid/"
        "southbank/"
        "southbank_bids_mcard_weekly_txn.csv"
    ),
)

# dict of different layers
mcard_weekly_layers = {
    "txn": [
        "bespoke", "bids", "boroughs", "caz",
        "highstreets", "inner_outer", "london", "msoas", "towncentres"
    ],
    "yoy": [
        "bespoke", "bids", "boroughs", "caz",
        "highstreets", "inner_outer", "london", "msoas", "towncentres"
    ]
}

# Loop through each layer and upload to lds
for prefix, resources in mcard_weekly_layers.items():
    for resource in resources:
        resource_title = f"{prefix}_{resource}.csv"
        file_path = f"{base_dir}mastercard/weekly/processed/{resource_title}"
        data_writer.upload_data_to_lds(
            slug="mastercard-retail-location-insights",
            custom_date_column="week_start",
            resource_title=resource_title,
            file_path=file_path
        )
