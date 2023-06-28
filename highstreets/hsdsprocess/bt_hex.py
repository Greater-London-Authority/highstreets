import argparse

from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.data_transformation.hextransform import HexTransform

# Create an argument parser
parser = argparse.ArgumentParser(description="Your script description")

# Add arguments
parser.add_argument("start_date", type=str, help="Start date in YYYY-MM-DD format")
parser.add_argument("end_date", type=str, help="End date in YYYY-MM-DD format")

# Parse the arguments
args = parser.parse_args()

# Access the argument values
start_date = args.start_date
end_date = args.end_date

data_loader = DataLoader(
    "https://api.business.bt.com/v1/footfall/reports/hex-grid/tfl?agg=time_indicator"
)

data = data_loader.get_hex_data(start_date, end_date)
hex_transform = HexTransform()
transformed_data = hex_transform.transform_data(data)
print(transformed_data)
