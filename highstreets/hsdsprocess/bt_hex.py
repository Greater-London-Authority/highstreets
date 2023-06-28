import os

from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.data_transformation.hextransform import HexTransform

data_loader = DataLoader(
    "https://api.business.bt.com/v1/footfall/reports/hex-grid/tfl?agg=time_indicator"
)

start_date = os.environ.get("START_DATE")
end_date = os.environ.get("END_DATE")
print(start_date)
data = data_loader.get_hex_data("2023-05-10", "2023-05-10")
hex_transform = HexTransform()
transformed_data = hex_transform.transform_data(data)
print(transformed_data)
