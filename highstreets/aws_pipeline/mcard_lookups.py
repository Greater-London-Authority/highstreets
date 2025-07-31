from dotenv import load_dotenv
from highstreets import config
from highstreets.data_source_sink.datawriter import DataWriter
from highstreets.data_source_sink.lookup_manager import LookupManager
import warnings
from sqlalchemy import exc as sa_exc
# Suppress GeoPandas GEOS version warnings
warnings.filterwarnings('ignore', message='.*Shapely GEOS version.*incompatible.*')
# Suppress SQLAlchemy XML column warnings
warnings.filterwarnings(
    'ignore', category=sa_exc.SAWarning, message='.*Did not recognize type.*xml.*')
# Optional: Suppress all SQLAlchemy warnings if needed
# warnings.filterwarnings('ignore', category=sa_exc.SAWarning)
print("Warning filters applied for cleaner output")

# Load environment variables from .env file
load_dotenv()

warnings.filterwarnings('ignore')
base_dir = config.BASE_DIR

data_writer = DataWriter()
lookup_manager = LookupManager()
df = lookup_manager.process_borough_hs_lookup()
data_writer.truncate_and_load_to_postgres(
    df,
    table_name='econ_busyness_borough_hs_lookup_3',
    schema='gisapdata')
lookup_manager.generate_all_quad_lookups()
