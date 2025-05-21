from typing import Dict
import logging
import importlib.resources as pkg_resources
from highstreets import sql  # Import the sql package directory


class SQLManager:
    """Manages SQL queries for the application."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.query_cache: Dict[str, str] = {}
        # Define base directories to search for SQL files
        self.search_paths = [
            'queries/bt',
            'queries/mcard',
            'queries/mcard/weekly',
            'queries/mcard/threehourly',  # Add other paths as needed
        ]

    def get_query(self, query_name: str, category: str = None) -> str:
        """
        Retrieves SQL query from package resources.

        Args:
            query_name: Name of the query file with or without .sql extension
            category: Optional category path (e.g., 'mcard/weekly')
                    If not provided, all search paths are checked

        Returns:
            str: The SQL query string

        Raises:
            FileNotFoundError: If the query file doesn't exist in package
        """
        # Check if query is already in cache
        if query_name in self.query_cache:
            return self.query_cache[query_name]

        # Add .sql extension if not present
        if not query_name.endswith('.sql'):
            query_name += '.sql'

        # Use specified category or search through all paths
        paths_to_search = [f'queries/{category}'] if category else self.search_paths

        # Try each path until query is found
        errors = []
        for path in paths_to_search:
            try:
                with pkg_resources.files(sql).joinpath(
                        f'{path}/{query_name}'
                ).open('r') as f:
                    query = f.read()

                # Cache the query for future use
                self.query_cache[query_name] = query
                self.logger.debug(f"Successfully loaded query: {path}/{query_name}")
                return query
            except Exception as e:
                errors.append(f"{path}: {str(e)}")
                continue  # Try next path

        # If we reach here, query was not found in any path
        error_msg = (f"SQL query file '{query_name}' not found in any of"
                     f" the search paths:\n" + "\n".join(errors))
        self.logger.error(error_msg)
        raise FileNotFoundError(error_msg)
