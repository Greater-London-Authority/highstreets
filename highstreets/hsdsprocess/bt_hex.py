from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.data_transformation.hextransform import HexTransform

data_loader = DataLoader(
    "https://api.business.bt.com/v1/footfall/reports/hex-grid/tfl?agg=time_indicator"
)
data = data_loader.get_hex_data("2023-01-01", "2023-01-01")
hex_transform = HexTransform()
transformed_data = hex_transform.transform_data(data)
print(transformed_data)
