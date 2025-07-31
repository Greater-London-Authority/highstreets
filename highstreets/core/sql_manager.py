from typing import Dict, List, Optional, Generator
import logging
import importlib.resources as pkg_resources
from highstreets import sql
import time
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from contextlib import contextmanager
from datetime import datetime


class SQLManager:
    """
    SQL query manager for the application.

    Provides comprehensive query management including caching, validation,
    execution, performance monitoring, and metadata management.
    """

    def __init__(self, engine: Optional[Engine] = None):
        self.logger = logging.getLogger(__name__)
        self.query_cache: Dict[str, str] = {}
        self.query_metadata_cache: Dict[str, Dict] = {}
        self.performance_stats: Dict[str, List[Dict]] = {}
        self.query_history: List[Dict] = []
        self.engine = engine

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
        paths_to_search = ([f'queries/{category}'] if category
                           else self.search_paths)

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

    def execute_query(self, query_name: str, engine: Optional[Engine] = None,
                      **params) -> pd.DataFrame:
        """
        Execute a query and return results as DataFrame.
        Args:
            query_name: Name of the query to execute
            engine: Optional SQLAlchemy engine (uses self.engine if not provided)
            **params: Query parameters

        Returns:
            Query results as pandas DataFrame
        """
        start_time = time.time()
        execution_engine = engine or self.engine

        if not execution_engine:
            raise ValueError("No database engine provided")

        try:
            if query_name.endswith('.sql'):
                query = self.get_query(query_name)
            else:
                query = query_name
            with execution_engine.connect() as conn:
                result = pd.read_sql(text(query), conn, params=params)

            execution_time = time.time() - start_time
            self._log_query_execution(query_name, execution_time, 'SUCCESS',
                                      len(result))

            return result

        except Exception as e:
            execution_time = time.time() - start_time
            self._log_query_execution(query_name, execution_time, 'ERROR', 0)
            self.logger.error(f"Error executing query {query_name}: {e}")
            raise

    def execute_query_chunked(self, query_name: str, chunk_size: int = 10000,
                              engine: Optional[Engine] = None,
                              **params) -> Generator[pd.DataFrame, None, None]:
        """
        Execute query in chunks for large datasets.

        Args:
            query_name: Name of the query to execute
            chunk_size: Number of rows per chunk
            engine: Optional SQLAlchemy engine
            **params: Query parameters

        Yields:
            DataFrame chunks
        """
        execution_engine = engine or self.engine
        if not execution_engine:
            raise ValueError("No database engine provided")

        query = self.format_query(query_name, **params)

        with execution_engine.connect() as conn:
            for chunk in pd.read_sql(text(query), conn, params=params,
                                     chunksize=chunk_size):
                yield chunk

    def clear_cache(self, query_name: str = None) -> None:
        """
        Clear query cache for specific query or all queries.

        Args:
            query_name: Optional specific query to clear from cache
        """
        if query_name:
            self.query_cache.pop(query_name, None)
            self.query_metadata_cache.pop(query_name, None)
            self.logger.info(f"Cleared cache for query: {query_name}")
        else:
            self.query_cache.clear()
            self.query_metadata_cache.clear()
            self.logger.info("Cleared all query caches")

    def get_cache_stats(self) -> Dict:
        """
        Get cache hit/miss statistics.

        Returns:
            Dictionary with cache statistics
        """
        return {
            'cached_queries': len(self.query_cache),
            'cache_size_bytes': sum(len(q.encode('utf-8'))
                                    for q in self.query_cache.values()),
            'metadata_cache_size': len(self.query_metadata_cache)
        }

    def get_query_history(self, limit: int = 100) -> List[Dict]:
        """
        Get query execution history.

        Args:
            limit: Maximum number of history entries to return

        Returns:
            List of query execution history entries
        """
        return self.query_history[-limit:]

    def get_performance_stats(self, query_name: str = None) -> Dict:
        """
        Get performance statistics for queries.

        Args:
            query_name: Optional specific query name

        Returns:
            Performance statistics
        """
        if query_name:
            stats = self.performance_stats.get(query_name, [])
            if stats:
                execution_times = [s['execution_time'] for s in stats]
                return {
                    'query_name': query_name,
                    'total_executions': len(stats),
                    'avg_execution_time': (sum(execution_times) / len(execution_times)),
                    'min_execution_time': min(execution_times),
                    'max_execution_time': max(execution_times),
                    'success_rate': (len([s for s in stats
                                          if s['status'] == 'SUCCESS']) / len(stats))
                }
            return {'query_name': query_name, 'total_executions': 0}
        else:
            return {query: self.get_performance_stats(query)
                    for query in self.performance_stats.keys()}

    def search_queries(self, search_term: str) -> List[str]:
        """
        Search for queries by name or content.

        Args:
            search_term: Term to search for

        Returns:
            List of matching query names
        """
        matches = []
        all_queries = self.list_available_queries()

        for category, queries in all_queries.items():
            for query in queries:
                # Search in query name
                if search_term.lower() in query.lower():
                    matches.append(f"{category}/{query}")
                    continue

                # Search in query content
                try:
                    content = self.get_query(query,
                                             category.replace('queries/', ''))
                    if search_term.lower() in content.lower():
                        matches.append(f"{category}/{query}")
                except Exception:
                    continue

        return matches

    def _log_query_execution(self, query_name: str, execution_time: float,
                             status: str, row_count: int = 0) -> None:
        """Internal method to log query execution."""
        execution_record = {
            'query_name': query_name,
            'execution_time': execution_time,
            'status': status,
            'row_count': row_count,
            'timestamp': datetime.now().isoformat()
        }

        # Add to history
        self.query_history.append(execution_record)

        # Keep only last 1000 entries
        if len(self.query_history) > 1000:
            self.query_history = self.query_history[-1000:]

        # Add to performance stats
        if query_name not in self.performance_stats:
            self.performance_stats[query_name] = []
        self.performance_stats[query_name].append(execution_record)

        # Keep only last 100 entries per query
        if len(self.performance_stats[query_name]) > 100:
            self.performance_stats[query_name] = (
                self.performance_stats[query_name][-100:])

    @contextmanager
    def query_context(self, **context_params):
        """
        Context manager for query execution with common parameters.

        Usage:
            with sql_manager.query_context(schema='public',
                                           date_from='2024-01-01'):
                result = sql_manager.execute_query('my_query')
        """
        old_context = getattr(self, '_context_params', {})
        self._context_params = {**old_context, **context_params}
        try:
            yield self
        finally:
            self._context_params = old_context
