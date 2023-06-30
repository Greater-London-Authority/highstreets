import os

from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.data_transformation.hextransform import HexTransform

data_loader = DataLoader()

# Get the start_date and end_date
start_date = os.environ.get("START_DATE")
end_date = os.environ.get("END_DATE")

data = data_loader.get_hex_data(str(start_date), str(end_date))
hex_transform = HexTransform()
transformed_data = hex_transform.transform_data(data)
print(transformed_data)
