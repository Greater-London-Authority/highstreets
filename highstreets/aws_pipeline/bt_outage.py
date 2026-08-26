import os
import pandas as pd
import warnings
from highstreets import config
from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.data_source_sink.datawriter import DataWriter
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


def main():
    data_loader = DataLoader()

    start_date = os.getenv('START_DATE')
    end_date = os.getenv('END_DATE')
    if not start_date or not end_date:
        raise ValueError(
            "START_DATE and END_DATE environment variables are required"
        )

    data = data_loader.get_bt_outage_history_data(str(start_date), str(end_date))

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


if __name__ == "__main__":
    main()
