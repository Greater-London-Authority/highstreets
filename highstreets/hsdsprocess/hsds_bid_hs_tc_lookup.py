import os

import geopandas as gpd
from sqlalchemy import create_engine, text

from highstreets.data_source_sink.datawriter import DataWriter

database = os.getenv("PG_DATABASE")
username = os.getenv("PG_USER")
password = os.getenv("PG_PASSWORD")
host = os.getenv("PG_HOST")
port = os.getenv("PG_PORT")

engine = create_engine(
    f"postgresql+psycopg2://{username}:{password}@" f"{host}:{port}/{database}"
)
# bid
query = "select * from " "hsds_bid_hs_tc_look"
hsds_bid_hs_tc_look_backup = gpd.GeoDataFrame.from_postgis(
    text(query), engine.connect(), geom_col="geom"
)

hsds_bid_hs_tc_look_backup.to_postgis(
    name="hsds_bid_hs_tc_backup",
    con=engine.connect(),
    if_exists="replace",
    index=False,
    schema="gisapdata",
)

data_writer = DataWriter()
data_writer.load_hsds_lookup_to_postgres()
