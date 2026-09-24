"""Module for managing lookup operations across different data sources."""

import os
import pandas as pd
import geopandas as gpd
import requests
from shapely.geometry import shape
from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine, text

from highstreets import config
from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.data_source_sink.datawriter import DataWriter
from highstreets.core.logger import setup_logger


load_dotenv(find_dotenv())


class LookupManager:
    """
    Manages lookup operations for various geographical and business entities.

    This class centralizes all lookup functionality that was previously scattered
    across different modules, providing a unified interface for lookup operations.
    """

    def __init__(self):
        """Initialize the LookupManager with database connection and other settings."""
        self.logger = setup_logger(__name__)

        # Database connection settings
        self.database = os.getenv("PG_DATABASE")
        self.username = os.getenv("PG_USER")
        self.password = os.getenv("PG_PASSWORD")
        self.host = os.getenv("PG_HOST")
        self.port = os.getenv("PG_PORT")

        # Create database engine
        self.engine = create_engine(
            f"postgresql://{self.username}:{self.password}@{self.host}:"
            f"{self.port}/{self.database}"
        )

        # Base directory for file operations
        self.base_dir = config.BASE_DIR

        # Data processing objects
        self.data_loader = DataLoader()
        self.data_writer = DataWriter()

        # Store global layer data and lookups
        self.layer_data = {}
        self.layer_lookups = {}

        self.logger.info("LookupManager initialized")

    def get_hex_lookup(self, lookup_type):
        # Load the .shp file using GeoPandas
        hex350_grid_GLA = gpd.read_file(
            f"{self.base_dir}"
            "reference_data/shapefiles/hex350_grid_GLA.shp"
        )
        hex_400m_buffer1 = gpd.read_file(
            f"{self.base_dir}"
            "reference_data/shapefiles/hex_400m_buffer1.shp"
        )
        hex_400m_buffer1 = hex_400m_buffer1.rename(columns={"Hex_ID": "hex_id"})

        if lookup_type == "highstreet":
            query = (
                "select highstreet_id, highstreet_name, geom "
                "from regen_high_streets_proposed_2"
            )
            highstreet = gpd.GeoDataFrame.from_postgis(
                text(query), self.engine.connect(), geom_col="geom"
            )
            # Spatially join the two GeoDataFrames based on the geometry intersection
            join_result = gpd.sjoin(
                hex_400m_buffer1, highstreet, how="left", op="intersects"
            )
            # Select the 'Hex_ID', 'highstreet_id', and 'geometry' columns
            lookup_table = join_result[["hex_id", "highstreet_id", "highstreet_name"]]
            lookup_table["highstreet_id"] = lookup_table["highstreet_id"].astype(
                "Int64"
            )
            return lookup_table
        elif lookup_type == "towncentre":
            query = "select tc_id, tc_name, geom from planning_town_centre_all_2026"
            tc = gpd.GeoDataFrame.from_postgis(
                text(query), self.engine.connect(), geom_col="geom"
            )
            # Spatially join the two GeoDataFrames based on the geometry intersection
            join_result = gpd.sjoin(hex_400m_buffer1, tc, how="left", op="intersects")
            lookup_table = join_result[["hex_id", "tc_id", "tc_name"]]
            lookup_table["tc_id"] = lookup_table["tc_id"].astype("Int64")
            return lookup_table
        elif lookup_type == "bespoke":
            query = (
                "select bespoke_area_id, name, "
                "geometry from econ_busyness_bespoke_focus_areas_live"
            )
            bespoke = gpd.GeoDataFrame.from_postgis(
                text(query), self.engine.connect(), geom_col="geometry"
            )
            # Spatially join the two GeoDataFrames based on the geometry intersection
            join_result = gpd.sjoin(
                hex350_grid_GLA, bespoke, how="left", op="intersects"
            )
            lookup_table = join_result[["hex_id", "bespoke_area_id", "name"]]
            lookup_table["bespoke_area_id"] = lookup_table["bespoke_area_id"].astype(
                "Int64"
            )
            return lookup_table
        elif lookup_type == "strategic":
            query = (
                "select strategic_id, strategic_name, geom "
                "from core_hsds_strategic_boundaries"
            )
            strategic = gpd.GeoDataFrame.from_postgis(
                text(query), self.engine.connect(), geom_col="geom"
            )
            join_result = gpd.sjoin(
                hex350_grid_GLA, strategic, how="left", op="intersects"
            )
            lookup_table = join_result[["hex_id", "strategic_id", "strategic_name"]]
            lookup_table["strategic_id"] = lookup_table["strategic_id"].astype(
                "Int64"
            )
            return lookup_table
        elif lookup_type == "bid":
            query = (
                "select bid_id, bid_name, geom "
                "from regen_business_improvement_districts_27700_live"
            )
            bid = gpd.GeoDataFrame.from_postgis(
                text(query), self.engine.connect(), geom_col="geom"
            )
            # Spatially join the two GeoDataFrames based on the geometry intersection
            join_result = gpd.sjoin(hex_400m_buffer1, bid, how="left", op="intersects")
            lookup_table = join_result[["hex_id", "bid_id", "bid_name"]]
            lookup_table["bid_id"] = lookup_table["bid_id"].astype("Int64")
            return lookup_table

    def get_query_context(self, layers=None):
        """
        Query context data for specified layers from GLA GIS service.

        Args:
            layers (list, optional): List of layer names to query.
            If None, queries all layers.
                Default is None.

        Returns:
            pd.DataFrame: Combined GeoDataFrame with all requested layers.
        """
        if layers is None:
            layers = ["BIDs",
                      "CAZ",
                      "Highstreets",
                      "TownCentres",
                      "Boroughs",
                      "MSOAs",
                      "Bespoke",
                      "Strategic"]

        layer_nums = {
            "BIDs": 1,
            "CAZ": 2,
            "Highstreets": 3,
            "TownCentres": 5,
            "Boroughs": 0,
            "MSOAs": 4,
            "Bespoke": 8,
            "Strategic": 13
        }

        layer_ids = {
            "BIDs": ["bid_id", "bid_name"],
            "CAZ": ["objectid", "name"],
            "Highstreets": ["highstreet_id", "highstreet_name"],
            "TownCentres": ["tc_id", "tc_name"],
            "Boroughs": ["gss_code", "name"],
            "MSOAs": ["msoa11cd", "msoa11nm"],
            "Bespoke": ["bespoke_area_id", "name"],
            "Strategic": ["strategic_id", "strategic_name"]
        }

        layer_df = []

        for layer in layers:
            layer_num = layer_nums[layer]
            layer_id = layer_ids[layer]

            service_query = (
                f"https://gis.london.gov.uk/arcgis/rest/services/apps"
                f"/Busyness_context/MapServer/"
                f"{layer_num}/query?where=1%3D1&outFields=*&f=geojson"
            )

            # Fetch the data
            response = requests.get(service_query)
            data = response.json()

            # Convert to GeoDataFrame
            features = data['features']
            geoms = [shape(feature['geometry']) for feature in features]
            records = [feature['properties'] for feature in features]
            df = gpd.GeoDataFrame(records, geometry=geoms, crs="EPSG:4326")

            # Specific adjustment for CAZ
            if layer == "CAZ":
                df['name'] = "CAZ"

            # Select and rename the columns
            df = df[layer_id + ['geometry']].rename(columns={
                layer_id[0]: 'id',
                layer_id[1]: 'name'
            })
            # Transform to British National Grid (EPSG:27700)
            df = df.to_crs(epsg=27700)

            # Add layer information
            df['layer'] = layer
            df['id'] = df['id'].astype(str)
            df['name'] = (df['name']
                         .str.replace("\u2019", "'")   # noqa
                         .str.replace("\u2018", "'")   # noqa
                         .str.replace("\u201C", '"')   # noqa
                         .str.replace("\u201D", '"')   # noqa
                         .str.replace("â€™", "'")      # noqa
                         .str.replace("â€˜", "'"))     # noqa
            layer_df.append(df)

        # Combine all layers into a single DataFrame
        df = pd.concat(layer_df, ignore_index=True)
        return df

    def process_borough_hs_lookup(self):
        """
        Process borough and high street lookup relationships.

        Returns:
            pd.DataFrame: Processed lookup table with borough and
            high street relationships.
        """
        context_areas = self.get_query_context(layers=[
            "BIDs", "Highstreets", "TownCentres", "Bespoke"])
        boroughs = self.get_query_context(layers=["Boroughs"])
        # Process context areas
        context_areas['name'] = (context_areas['name']
                                 .str.replace("\u2019", "'")
                                 .str.replace("\u2018", "'")
                                 .str.replace("\u00e2\u0080\u0099", "'")
                                 .str.replace("\u00e2\u0080\u0098", "'"))

        # Conditional assignment of names and IDs
        context_areas = context_areas.assign(
            highstreet_name=context_areas.apply(
                lambda x: x['name'] if x['layer'] == "Highstreets" else pd.NA, axis=1),
            bid_name=context_areas.apply(
                lambda x: x['name'] if x['layer'] == "BIDs" else pd.NA, axis=1),
            tc_name=context_areas.apply(
                lambda x: x['name'] if x['layer'] == "TownCentres" else pd.NA, axis=1),
            bespoke_name=context_areas.apply(
                lambda x: x['name'] if x['layer'] == "Bespoke" else pd.NA, axis=1),
            highstreet_id=context_areas.apply(
                lambda x: int(x['id']) if x[
                    'layer'] == "Highstreets" else pd.NA, axis=1),
            bid_id=context_areas.apply(
                lambda x: int(x['id']) if x['layer'] == "BIDs" else pd.NA, axis=1),
            tc_id=context_areas.apply(
                lambda x: int(x['id']) if x[
                    'layer'] == "TownCentres" else pd.NA, axis=1),
            bespoke_id=context_areas.apply(
                lambda x: int(x['id']) if x['layer'] == "Bespoke" else pd.NA, axis=1),
        )

        # Select and join with boroughs
        context_areas = context_areas.drop(columns=["name", "id", "layer"])

        # Apply a negative buffer to boroughs (to avoid slithers)
        boroughs['geometry'] = boroughs['geometry'].buffer(-29)

        # Spatial join with boroughs
        df = gpd.sjoin(context_areas, boroughs, how="inner", op="intersects")
        # drop geometry column
        df = df.drop(columns=["geometry"]).reset_index(drop=True)
        # rename columns
        df.rename(columns={"name": "borough_name", "id": "borough_code"}, inplace=True)

        df = df.sort_values(['bid_id', 'highstreet_id', 'tc_id', 'bespoke_id'])

        # Add objectid and select final columns
        df['objectid'] = range(1, len(df) + 1)
        df = df[['objectid', 'borough_name', 'borough_code', 'highstreet_name',
                 'highstreet_id', 'bid_name', 'bid_id', 'tc_name',
                'tc_id', 'bespoke_id', 'bespoke_name']]
        return df

    def generate_borough_lookup(self):
        """
        Generate borough lookup with centroid coordinates.

        Returns:
            pd.DataFrame: Borough lookup with coordinates
        """
        try:
            # Get borough data
            boroughs = self.get_query_context(layers=["Boroughs"])

            # Calculate centroid coordinates
            boroughs = boroughs.assign(
                x=boroughs.geometry.centroid.x,
                y=boroughs.geometry.centroid.y
            )

            # Drop unnecessary columns and rename for consistency
            borough_lookup = boroughs.drop(columns=['layer', 'geometry'])
            borough_lookup = borough_lookup.rename(columns={
                'id': 'gss_code',
                'name': 'borough_name'
            })

            # Select and order columns
            borough_lookup = borough_lookup[['gss_code', 'borough_name', 'x', 'y']]
            borough_lookup = borough_lookup.sort_values('borough_name')

            # Save to CSV
            output_path = f"{self.base_dir}mastercard/lookups/borough_lookup.csv"
            # borough_lookup.to_csv(output_path, index=False)
            self.logger.info(f"Saved borough lookup to {output_path}")

            # Write to PostgreSQL
            self.data_writer.truncate_and_load_to_postgres(
                dataframe=borough_lookup,
                table_name='econ_busyness_borough_lookup_test',
                schema="gisapdata"
            )
            self.logger.info("Wrote borough lookup to PostgreSQL")

            return borough_lookup

        except Exception as e:
            self.logger.error(f"Error generating borough lookup: {str(e)}")
            raise

    def query_mcard_grids(self):
        """
        Query Mastercard grid data from PostgreSQL.

        Returns:
            gpd.GeoDataFrame: GeoDataFrame containing quad grid data.
        """
        table_name = "econ_busyness_mcard_grids_zoom18_u_3"
        query = text(f"SELECT * FROM {table_name}")

        try:
            with self.engine.connect() as conn:
                df = gpd.read_postgis(query, conn, geom_col='geometry')
                self.logger.info(f"Successfully queried {len(df)} quad grid records")
                return df
        except Exception as e:
            self.logger.error(f"Error querying Mastercard grids: {str(e)}")
            raise

    def generate_layer_quad_lookup(self, layer, layer_id):
        """
        Generate quad lookup for a specific layer.

        Args:
            layer (str): Layer name (e.g., "BIDs", "CAZ", etc.)
            layer_id (list): List containing ID and name field names

        Returns:
            gpd.GeoDataFrame: Processed lookup for the specified layer
        """
        try:
            # Get context data for the layer
            df = self.get_query_context(layers=[layer])
            df = df.drop(columns=['layer'])
            df.columns = layer_id + ['geometry']
            df = df.to_crs(epsg=27700)

            # Store layer data in the instance dictionary
            self.layer_data[layer] = df

            # Spatial join with quads
            quads_join = gpd.sjoin(self.quads, df, how="inner", op='intersects')

            # Clean up result
            if 'index_right' in quads_join.columns:
                quads_join = quads_join.drop(columns=['index_right'])

            # Store in lookups dictionary
            self.layer_lookups[f"{layer}_lookup"] = quads_join

            # Save to CSV
            output_path = f"{self.base_dir}mastercard/lookups/{layer}_quad_lookup.csv"
            quads_join.drop(columns=['geometry']).drop_duplicates().to_csv(
                output_path, index=False)
            self.logger.info(f"Saved {layer} lookup to {output_path}")

            # Write to PostgreSQL
            self.data_writer.write_quad_lookup_to_postgres(
                quads_join.drop(columns=['geometry']).drop_duplicates(), layer)
            self.logger.info(f"Wrote {layer} lookup to PostgreSQL")

            return quads_join

        except Exception as e:
            self.logger.error(f"Error generating lookup for {layer}: {str(e)}")
            raise

    def add_borough_lookup(self, layer_name, layer_ids):
        """
        Add borough information to a layer lookup.

        Args:
            layer_name (str): Name of the layer
            layer_ids (dict): Dictionary mapping layer names to ID column names

        Returns:
            pd.DataFrame: Layer lookup with added borough information
        """
        try:
            # Get the original layer and borough data
            lookup = self.layer_data[layer_name]
            borough_layer = self.layer_data["Boroughs"]

            # Create a copy with centroids instead of original geometries
            layer_centroids = self.layer_data[layer_name].copy()
            layer_centroids.geometry = layer_centroids.geometry.centroid

            # Spatial join using centroids (use 'within' instead of 'intersects')
            layer_borough = gpd.sjoin(
                layer_centroids, borough_layer, how="inner", op='within')

            # In case a centroid is exactly on a boundary and
            # doesn't fall within any borough,
            # fallback to nearest borough
            if len(layer_borough) < len(lookup):
                missing_ids = (
                    set(lookup[layer_ids[layer_name][0]]) - set(
                        layer_borough[layer_ids[layer_name][0]]))
                self.logger.warning(
                    f"{len(missing_ids)} {layer_name} centroids did not"
                    f" fall within any borough.")

            # Calculate coordinates for the centroids
            layer_borough = layer_borough.assign(
                x=layer_borough.geometry.x,
                y=layer_borough.geometry.y
            ).drop(columns=['geometry'])

            # Rename the borough name column
            layer_borough = layer_borough.rename(columns={'name': 'borough'})

            # Select only the needed columns
            layer_borough = layer_borough[
                [layer_ids[layer_name][0], 'x', 'y', 'borough']]

            # Join with the lookup table
            layer_lookup = self.layer_lookups[f"{layer_name}_lookup"].merge(
                layer_borough, on=layer_ids[layer_name][0], how='left'
            )

            # Convert ID to integer and sort
            layer_lookup[layer_ids[layer_name][0]] = layer_lookup[
                layer_ids[layer_name][0]].astype(int)
            layer_lookup = layer_lookup.sort_values(layer_ids[layer_name][0])

            # Drop geometry column from the final output
            if 'geometry' in layer_lookup.columns:
                layer_lookup = layer_lookup.drop(columns=['geometry'])

            return layer_lookup

        except Exception as e:
            self.logger.error(f"Error adding borough lookup for {layer_name}: {str(e)}")
            raise

    def generate_london_lookup(self):
        """
        Generate London-wide lookup by dissolving borough boundaries.

        Returns:
            pd.DataFrame: London quad lookup
        """
        try:
            # Create London boundary by dissolving boroughs
            london = self.layer_data["Boroughs"].assign(area="London")
            london = london[['area', 'geometry']].dissolve(by='area', as_index=False)

            # Spatial join with quads
            quads_join = gpd.sjoin(self.quads, london, how="inner", op='intersects')
            quads_join = quads_join.drop(
                columns=['geometry', 'index_right']).drop_duplicates()

            # Save to CSV
            output_path = f"{self.base_dir}mastercard/lookups/london_quad_lookup.csv"
            quads_join.to_csv(output_path, index=False)
            self.logger.info(f"Saved London lookup to {output_path}")

            # Write to PostgreSQL
            self.data_writer.write_quad_lookup_to_postgres(quads_join, "london")
            self.logger.info("Wrote London lookup to PostgreSQL")

            return quads_join

        except Exception as e:
            self.logger.error(f"Error generating London lookup: {str(e)}")
            raise

    def generate_inner_outer_lookup(self):
        """
        Generate Inner/Outer London lookup.

        Returns:
            pd.DataFrame: Inner/Outer London quad lookup
        """
        try:
            # Inner/Outer London borough classification
            inner_outer_allocation = {
                "Inner": [
                    "City of London", "Camden", "Greenwich", "Hackney",
                    "Hammersmith and Fulham", "Islington", "Kensington and Chelsea",
                    "Lambeth", "Lewisham", "Newham",
                    "Southwark", "Tower Hamlets", "Wandsworth", "Westminster"
                ],
                "Outer": [
                    "Barking and Dagenham", "Barnet", "Bexley", "Brent", "Bromley",
                    "Croydon", "Ealing", "Enfield", "Haringey", "Harrow", "Havering",
                    "Hillingdon", "Hounslow", "Kingston upon Thames", "Merton",
                    "Redbridge", "Richmond upon Thames", "Sutton", "Waltham Forest"
                ]
            }

            # Create lookup DataFrame
            inner_outer_borough_lookup = pd.DataFrame({
                'name': (inner_outer_allocation['Inner']
                         + inner_outer_allocation['Outer']),
                'inner_outer': (['Inner'] * len(inner_outer_allocation['Inner'])
                                + ['Outer'] * len(inner_outer_allocation['Outer']))
            })

            # Join with boroughs lookup
            boroughs_lookup = self.layer_lookups["Boroughs_lookup"]
            inner_outer_quad_lookup = boroughs_lookup.merge(
                inner_outer_borough_lookup, left_on='name', right_on='name')
            inner_outer_quad_lookup = inner_outer_quad_lookup[
                ['quad_id', 'inner_outer']].drop_duplicates()

            # Save to CSV
            output_path = (f"{self.base_dir}mastercard/lookups/"
                           f"Inner_Outer_quad_lookup.csv")
            inner_outer_quad_lookup.to_csv(output_path, index=False)
            self.logger.info(f"Saved Inner/Outer London lookup to {output_path}")

            # Write to PostgreSQL
            self.data_writer.write_quad_lookup_to_postgres(
                inner_outer_quad_lookup, "Inner_Outer")
            self.logger.info("Wrote Inner/Outer London lookup to PostgreSQL")

            return inner_outer_quad_lookup

        except Exception as e:
            self.logger.error(f"Error generating Inner/Outer London lookup: {str(e)}")
            raise

    def generate_bespoke_lookup(self):
        """
        Generate bespoke areas quad lookup.

        Uses sjoin (intersects) for all bespoke areas uniformly,
        allowing quads to map to multiple overlapping bespoke areas.
        This is consistent with how all other layer types are handled.

        Returns:
            pd.DataFrame: Bespoke areas quad lookup
        """
        try:
            # Get bespoke areas data
            bespoke_areas = self.get_query_context(layers=["Bespoke"])

            # Clean names, rename columns, and add centroid coordinates
            bespoke_areas = bespoke_areas.rename(columns={'id': 'bespoke_area_id'})
            bespoke_areas = bespoke_areas.assign(
                x=bespoke_areas.geometry.centroid.x,
                y=bespoke_areas.geometry.centroid.y
            )
            bespoke_areas = bespoke_areas.drop(columns=['layer'])

            # Create a copy with centroids for borough assignment
            bespoke_centroids = bespoke_areas.copy()
            bespoke_centroids.geometry = bespoke_centroids.geometry.centroid

            # Join with boroughs using centroids
            boroughs = self.layer_data["Boroughs"].rename(columns={'name': 'borough'})
            bespoke_areas_with_borough = gpd.sjoin(
                bespoke_centroids, boroughs, how="inner", op='within')

            # Restore original geometries
            bespoke_areas_with_borough = bespoke_areas_with_borough.drop(
                columns=['geometry'])
            bespoke_areas_with_borough = bespoke_areas_with_borough.merge(
                bespoke_areas[['bespoke_area_id', 'geometry']], on='bespoke_area_id'
            )
            bespoke_areas_with_borough = gpd.GeoDataFrame(
                bespoke_areas_with_borough, geometry='geometry')

            # Ensure CRS matches between quads and bespoke areas
            self.quads = self.quads.to_crs(bespoke_areas_with_borough.crs)

            # Remove problematic index columns if they exist
            for col in ['index_left', 'index_right']:
                if col in bespoke_areas_with_borough.columns:
                    bespoke_areas_with_borough = bespoke_areas_with_borough.drop(
                        columns=[col])
                if col in self.quads.columns:
                    self.quads = self.quads.drop(columns=[col])

            # Spatial join — all bespoke areas uniformly
            bespoke_join = gpd.sjoin(
                self.quads, bespoke_areas_with_borough,
                how="inner", op='intersects')

            # Drop geometry and clean up
            bespoke_lookup = bespoke_join.drop(columns=['geometry'])
            if 'index_right' in bespoke_lookup.columns:
                bespoke_lookup = bespoke_lookup.drop(columns=['index_right'])

            # Ensure bespoke_area_id is integer and sort
            bespoke_lookup['bespoke_area_id'] = bespoke_lookup[
                'bespoke_area_id'].astype(int)
            bespoke_lookup = bespoke_lookup.sort_values('bespoke_area_id')

            # Save to file
            output_path = f"{self.base_dir}mastercard/lookups/bespoke_quad_lookup.csv"
            bespoke_lookup.to_csv(output_path, index=False)
            self.logger.info(f"Saved bespoke areas lookup to {output_path}")

            # Write to PostgreSQL
            self.data_writer.write_quad_lookup_to_postgres(bespoke_lookup, "bespoke")
            self.logger.info("Wrote bespoke areas lookup to PostgreSQL")

            return bespoke_lookup

        except Exception as e:
            self.logger.error(f"Error generating bespoke areas lookup: {str(e)}")
            raise

    def generate_strategic_lookup(self):
        """
        Generate strategic areas quad lookup.

        Uses sjoin (intersects) for all strategic areas uniformly,
        allowing quads to map to multiple overlapping strategic areas.

        Returns:
            pd.DataFrame: Strategic areas quad lookup
        """
        try:
            strategic_areas = self.get_query_context(layers=["Strategic"])

            strategic_areas = strategic_areas.rename(
                columns={'id': 'strategic_id', 'name': 'strategic_name'})
            strategic_areas = strategic_areas.assign(
                x=strategic_areas.geometry.centroid.x,
                y=strategic_areas.geometry.centroid.y
            )
            strategic_areas = strategic_areas.drop(columns=['layer'])

            # Create a copy with centroids for borough assignment
            strategic_centroids = strategic_areas.copy()
            strategic_centroids.geometry = strategic_centroids.geometry.centroid

            # Join with boroughs using centroids
            boroughs = self.layer_data["Boroughs"].rename(
                columns={'name': 'borough'})
            strategic_with_borough = gpd.sjoin(
                strategic_centroids, boroughs, how="inner", op='within')

            # Restore original geometries
            strategic_with_borough = strategic_with_borough.drop(
                columns=['geometry'])
            strategic_with_borough = strategic_with_borough.merge(
                strategic_areas[['strategic_id', 'geometry']],
                on='strategic_id'
            )
            strategic_with_borough = gpd.GeoDataFrame(
                strategic_with_borough, geometry='geometry')

            # Ensure CRS matches
            self.quads = self.quads.to_crs(strategic_with_borough.crs)

            # Remove problematic index columns if they exist
            for col in ['index_left', 'index_right']:
                if col in strategic_with_borough.columns:
                    strategic_with_borough = strategic_with_borough.drop(
                        columns=[col])
                if col in self.quads.columns:
                    self.quads = self.quads.drop(columns=[col])

            # Spatial join
            strategic_join = gpd.sjoin(
                self.quads, strategic_with_borough,
                how="inner", op='intersects')

            # Drop geometry and clean up
            strategic_lookup = strategic_join.drop(columns=['geometry'])
            if 'index_right' in strategic_lookup.columns:
                strategic_lookup = strategic_lookup.drop(
                    columns=['index_right'])

            strategic_lookup['strategic_id'] = strategic_lookup[
                'strategic_id'].astype(int)
            strategic_lookup = strategic_lookup.sort_values('strategic_id')

            # Save to file
            output_path = (
                f"{self.base_dir}mastercard/lookups/"
                f"strategic_quad_lookup.csv"
            )
            strategic_lookup.to_csv(output_path, index=False)
            self.logger.info(
                f"Saved strategic areas lookup to {output_path}")

            # Write to PostgreSQL
            self.data_writer.write_quad_lookup_to_postgres(
                strategic_lookup, "strategic")
            self.logger.info(
                "Wrote strategic areas lookup to PostgreSQL")

            return strategic_lookup

        except Exception as e:
            self.logger.error(
                f"Error generating strategic areas lookup: {str(e)}")
            raise

    def generate_town_centres_in_caz(self):
        """
        Generate lookup of Town Centres in the CAZ.

        Returns:
            pd.DataFrame: Town centres in CAZ lookup
        """
        try:
            # Spatial join between Town Centres and CAZ
            caz_tcs = gpd.sjoin(
                self.layer_data["TownCentres"],
                self.layer_data["CAZ"],
                how='inner',
                op='intersects'
            )[['tc_id', 'tc_name']].drop_duplicates().reset_index(drop=True)

            # Convert tc_id to integer and sort
            caz_tcs['tc_id'] = caz_tcs['tc_id'].astype(int)
            caz_tcs = caz_tcs.sort_values('tc_id')

            # Save to CSV
            output_path = f"{self.base_dir}mastercard/lookups/Towncenters_in_caz.csv"
            caz_tcs.to_csv(output_path, index=False)
            self.data_writer.truncate_and_load_to_postgres(
                dataframe=caz_tcs,
                table_name='econ_busyness_mcard_towncentre_caz_lookup',
                schema="gisapdata")
            self.logger.info(f"Saved Town Centres in CAZ lookup to {output_path}")

            return caz_tcs

        except Exception as e:
            self.logger.error(f"Error generating Town Centres in CAZ lookup: {str(e)}")
            raise

    def generate_all_quad_lookups(self):
        """
        Generate all quad lookups in one go.

        This method orchestrates the generation of all lookup tables.
        """
        try:
            self.logger.info("Starting generation of all quad lookups")

            # Layer definitions
            layer_ids = {
                "BIDs": ["bid_id", "bid_name"],
                "CAZ": ["objectid", "name"],
                "Highstreets": ["highstreet_id", "highstreet_name"],
                "TownCentres": ["tc_id", "tc_name"],
                "Boroughs": ["gss_code", "name"],
                "MSOAs": ["msoa11cd", "msoa11nm"]
            }

            # Query the quad grids
            self.quads = self.query_mcard_grids()
            # Transform to British National Grid
            self.quads = gpd.GeoDataFrame(self.quads).to_crs(27700)
            self.quads = self.quads[['quad_id', 'geometry']]
            self.logger.info(f"Loaded {len(self.quads)} quads")

            # Generate layer lookups
            for layer, layer_id in layer_ids.items():
                self.logger.info(f"Generating lookup for {layer}")
                self.generate_layer_quad_lookup(layer, layer_id)

            # Generate Town Centres in CAZ lookup
            self.generate_town_centres_in_caz()

            # Generate London lookup
            self.generate_london_lookup()

            # Generate Inner/Outer London lookup
            self.generate_inner_outer_lookup()

            # Generate bespoke areas lookup
            self.generate_bespoke_lookup()

            # Generate strategic areas lookup
            self.generate_strategic_lookup()

            # Add borough information to Highstreets and TownCentres
            highstreet_borough = self.add_borough_lookup("Highstreets", layer_ids)
            self.data_writer.write_quad_lookup_to_postgres(
                highstreet_borough, "Highstreets")

            towncentre_borough = self.add_borough_lookup("TownCentres", layer_ids)
            self.data_writer.write_quad_lookup_to_postgres(
                towncentre_borough, "TownCentres")

            self.logger.info("Successfully generated all quad lookups")

        except Exception as e:
            self.logger.error(f"Error generating quad lookups: {str(e)}")
            raise

    def generate_quad_to_all_lookup(self):
        """Generate a unified quad-to-all lookup joining all quad lookup tables.

        Performs LEFT JOINs on quad_id across all 9 area lookup tables so every
        quad appears even if it only belongs to one layer. Writes result to PG,
        S3, and returns the DataFrame.
        """
        self.logger.info("Generating quad-to-all master lookup...")
        quad_tables = {
            "highstreets": ("econ_busyness_mcard_highstreets_quad_lookup",
                            ["highstreet_id", "highstreet_name"]),
            "bids": ("econ_busyness_mcard_bids_quad_lookup",
                     ["bid_id", "bid_name"]),
            "towncentres": ("econ_busyness_mcard_towncentres_quad_lookup",
                            ["tc_id", "tc_name"]),
            "boroughs": ("econ_busyness_mcard_boroughs_quad_lookup",
                         ["gss_code"]),
            "caz": ("econ_busyness_mcard_caz_quad_lookup",
                    ["objectid"]),
            "msoas": ("econ_busyness_mcard_msoas_quad_lookup",
                      ["msoa11cd", "msoa11nm"]),
            "bespoke": ("econ_busyness_mcard_bespoke_quad_lookup",
                        ["bespoke_area_id"]),
            "strategic": ("econ_busyness_mcard_strategic_quad_lookup",
                          ["strategic_id", "strategic_name"]),
            "inner_outer": ("econ_busyness_mcard_inner_outer_quad_lookup",
                            ["inner_outer"]),
        }

        ambiguous_cols = {"name", "borough", "x", "y"}

        base_df = None
        for layer_name, (table_name, id_cols) in quad_tables.items():
            try:
                df = self.data_loader.get_full_data(table_name)
                keep_cols = ["quad_id"] + [
                    c for c in id_cols if c in df.columns
                ]
                extra_geo = [c for c in ["x", "y", "borough", "name"]
                             if c in df.columns and c not in keep_cols]
                keep_cols += extra_geo
                df = df[keep_cols].drop_duplicates(subset=["quad_id"] + id_cols[:1])

                rename_map = {}
                for c in extra_geo:
                    if c in ambiguous_cols:
                        rename_map[c] = f"{layer_name}_{c}"
                df = df.rename(columns=rename_map)

                if base_df is None:
                    base_df = df
                else:
                    base_df = base_df.merge(df, on="quad_id", how="outer")
            except Exception as e:
                self.logger.warning(
                    f"Could not load {table_name} for quad-to-all: {e}"
                )

        if base_df is not None:
            base_df = base_df.sort_values("quad_id").reset_index(drop=True)
            table_name = "econ_busyness_mcard_quad_to_all_lookup"
            self.data_writer.truncate_and_load_to_postgres(
                base_df, table_name, schema="gisapdata"
            )
            s3_path = (
                f"{config.BASE_DIR}reference_data/quad_to_all_lookup.csv"
            )
            base_df.to_csv(s3_path, index=False)
            self.logger.info(
                f"quad-to-all lookup: {len(base_df)} rows -> PG + S3"
            )
        return base_df

    def generate_hex_to_all_lookup(self):
        """Generate a unified hex-to-all lookup joining all hex lookup tables.

        Performs LEFT JOINs on hex_id across all 5 hex lookup tables.
        Writes result to PG, S3, and returns the DataFrame.
        """
        self.logger.info("Generating hex-to-all master lookup...")
        hex_tables = {
            "highstreet": ("econ_busyness_hex_highstreet_lookup",
                           ["highstreet_id", "highstreet_name"]),
            "towncentre": ("econ_busyness_hex_towncentre_lookup",
                           ["tc_id", "tc_name"]),
            "bid": ("econ_busyness_hex_bid_lookup",
                    ["bid_id", "bid_name"]),
            "bespoke": ("econ_busyness_hex_bespoke_lookup",
                        ["bespoke_area_id", "name"]),
            "strategic": ("econ_busyness_hex_strategic_lookup",
                          ["strategic_id", "strategic_name"]),
        }

        ambiguous_cols = {"name", "borough", "x", "y"}

        base_df = None
        for layer_name, (table_name, id_cols) in hex_tables.items():
            try:
                df = self.data_loader.get_full_data(table_name)
                keep_cols = ["hex_id"] + [
                    c for c in id_cols if c in df.columns
                ]
                extra = [c for c in ["borough", "x", "y"]
                         if c in df.columns and c not in keep_cols]
                keep_cols += extra
                df = df[keep_cols].drop_duplicates(subset=["hex_id"] + id_cols[:1])

                rename_map = {}
                for c in id_cols + extra:
                    if c in df.columns and c in ambiguous_cols:
                        rename_map[c] = f"{layer_name}_{c}"
                df = df.rename(columns=rename_map)

                if base_df is None:
                    base_df = df
                else:
                    base_df = base_df.merge(df, on="hex_id", how="outer")
            except Exception as e:
                self.logger.warning(
                    f"Could not load {table_name} for hex-to-all: {e}"
                )

        if base_df is not None:
            base_df = base_df.sort_values("hex_id").reset_index(drop=True)
            table_name = "econ_busyness_hex_to_all_lookup"
            self.data_writer.truncate_and_load_to_postgres(
                base_df, table_name, schema="gisapdata"
            )
            s3_path = (
                f"{config.BASE_DIR}reference_data/hex_to_all_lookup.csv"
            )
            base_df.to_csv(s3_path, index=False)
            self.logger.info(
                f"hex-to-all lookup: {len(base_df)} rows -> PG + S3"
            )
        return base_df

    # =========================================================================
    # LDC VALIDATED ADDRESS LOOKUP
    # =========================================================================

    def fetch_validated_addresses(self) -> pd.DataFrame:
        """Fetch validated premises addresses from the external hsds_dev DB.

        Connects to a separate PostgreSQL instance (HSDS_PG_* credentials),
        reads validated records from
        hsds_dev.core_premises_geometry_points, deduplicates on
        premises_id (keeping latest validation_date), and returns a
        clean DataFrame ready for merging onto the LDC clean output.

        Returns an empty DataFrame (with expected columns) if:
        - External DB credentials are not configured
        - The external DB is unreachable
        - The table is empty or does not exist

        Returns:
            pd.DataFrame with columns:
                premises_id, uprn, latitude, longitude, validation_date
        """
        empty = pd.DataFrame(
            columns=["premises_id", "uprn", "latitude",
                     "longitude", "validation_date"]
        )

        # --- Check credentials ---
        if not all([
            config.HSDS_PG_DATABASE,
            config.HSDS_PG_USER,
            config.HSDS_PG_PASSWORD,
            config.HSDS_PG_HOST,
        ]):
            self.logger.warning(
                "External DB credentials not configured — "
                "validated address lookup skipped"
            )
            return empty

        # --- Create engine with connection timeout ---
        ext_engine = create_engine(
            f"postgresql+psycopg2://{config.HSDS_PG_USER}:"
            f"{config.HSDS_PG_PASSWORD}@{config.HSDS_PG_HOST}:"
            f"{config.HSDS_PG_PORT}/{config.HSDS_PG_DATABASE}",
            connect_args={"connect_timeout": 10},
        )

        qualified_table = (
            f"{config.HSDS_VALIDATED_SCHEMA}."
            f"{config.HSDS_VALIDATED_TABLE}"
        )

        try:
            query = text(
                f"SELECT premises_id, uprn, latitude, longitude, "
                f"       validation_date "
                f"FROM {qualified_table} "
                f"WHERE validation_status = 'validated'"
            )
            with ext_engine.connect() as conn:
                validated_df = pd.read_sql(query, conn)

        except Exception as exc:
            self.logger.warning(
                f"Could not read validated addresses from "
                f"{qualified_table} — returning empty. Error: {exc}"
            )
            return empty
        finally:
            ext_engine.dispose()

        if validated_df.empty:
            self.logger.info(
                "No validated address records found in "
                f"{qualified_table}"
            )
            return empty

        self.logger.info(
            f"Loaded {len(validated_df):,} validated address records "
            f"from {qualified_table}"
        )

        # --- Dedup: keep latest validation per premises_id ---
        if validated_df["premises_id"].duplicated().any():
            dup_count = validated_df["premises_id"].duplicated().sum()
            self.logger.info(
                f"Deduplicating {dup_count:,} duplicate premises_id "
                f"entries — keeping latest validation_date"
            )
            validated_df = (
                validated_df
                .sort_values("validation_date", ascending=False)
                .drop_duplicates(subset="premises_id", keep="first")
            )
            self.logger.info(
                f"After dedup: {len(validated_df):,} unique premises"
            )

        # --- Log validation date range for audit ---
        vd = validated_df["validation_date"].dropna()
        if len(vd) > 0:
            self.logger.info(
                f"Validation date range: {vd.min()} to {vd.max()}"
            )

        # --- Type alignment ---
        validated_df["premises_id"] = pd.to_numeric(
            validated_df["premises_id"], errors="coerce"
        )

        return validated_df
