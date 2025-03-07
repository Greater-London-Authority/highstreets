from pathlib import Path
from typing import Dict
import logging
import importlib.resources as pkg_resources
from highstreets import sql  # Import the sql package directory

class SQLManager:
    """Manages SQL queries for the application."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.query_cache: Dict[str, str] = {}

    def get_query(self, query_name: str) -> str:
        """
        Retrieves SQL query from package resources.
        
        Args:
            query_name: Name of the query file without .sql extension
                      (e.g., 'hex_tc_transform')
        
        Returns:
            str: The SQL query string
            
        Raises:
            FileNotFoundError: If the query file doesn't exist in package
        """
        # Add .sql extension if not present
        if not query_name.endswith('.sql'):
            query_name += '.sql'

        try:
            # Use package resources to get the SQL file
            with pkg_resources.files(sql).joinpath(f'queries/bt/{query_name}').open('r') as f:
                query = f.read()
                
            self.logger.debug(f"Successfully loaded query: {query_name}")
            return query

        except Exception as e:
            self.logger.error(f"Failed to load query {query_name}: {str(e)}")
            raise FileNotFoundError(
                f"SQL query file '{query_name}' not found in package resources"
            ) from e