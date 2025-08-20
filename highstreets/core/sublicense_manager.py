"""
Enhanced Sublicense Manager

This module provides industrial-standard sublicense management capabilities
using database-side filtering and YAML configuration.

Updated to use:
- YAML-based configuration
- Database-side SQL filtering
- Flexible data source handling
- Performance monitoring
"""

import os
import yaml
import logging
import pandas as pd
import fsspec
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
import time
from sqlalchemy import create_engine
from highstreets.core.sql_manager import SQLManager
from highstreets.data_source_sink.datawriter import DataWriter
from highstreets import config


class SublicenseManager:
    """
    Enhanced sublicense manager with database-driven processing
    and YAML configuration support.
    """

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the sublicense manager.

        Args:
            config_path: Path to sublicense YAML configuration file
        """
        self.logger = self._setup_logging()

        # Configuration path - use settings directory
        self.config_path = config_path or os.path.join(
            os.path.dirname(__file__), 'settings', 'sublicenses.yaml'
        )

        # Load configuration
        self.config = self._load_config()
        self.global_config = self.config.get('config', {})
        self.sublicenses = self.config.get('sublicenses', {})

        # Initialize components
        self.data_writer = DataWriter()
        self.base_dir = config.BASE_DIR
        self.database = os.getenv("PG_DATABASE")
        self.username = os.getenv("PG_USER")
        self.password = os.getenv("PG_PASSWORD")
        self.host = os.getenv("PG_HOST")
        self.port = os.getenv("PG_PORT")
        self.engine = create_engine(
            f"postgresql+psycopg2://{self.username}:{self.password}@"
            f"{self.host}:{self.port}/{self.database}"
        )
        self.sql_manager = SQLManager(engine=self.engine)

        # Performance tracking
        self._performance_stats = {
            'queries_executed': 0,
            'total_rows_processed': 0,
            'files_created': 0,
            'uploads_completed': 0
        }

        self.logger.info(
            f"Initialized SublicenseManager with {len(self.sublicenses)} sublicenses")

    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration"""
        logger = logging.getLogger(__name__)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger

    def _load_config(self) -> Dict[str, Any]:
        """Load sublicense configuration from YAML file"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as file:
                config_data = yaml.safe_load(file)
            self.logger.info(f"Loaded sublicense configuration from {self.config_path}")
            return config_data
        except FileNotFoundError:
            self.logger.error(f"Configuration file not found: {self.config_path}")
            raise
        except yaml.YAMLError as e:
            self.logger.error(f"Invalid YAML in configuration file: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to load configuration: {str(e)}")
            raise

    def _format_ids_for_sql(self, ids: List[Union[int, str]]) -> str:
        """Format a list of IDs for SQL IN clause"""
        if not ids:
            return ""

        formatted_ids = []
        for id_val in ids:
            if isinstance(id_val, str) and not id_val.isdigit():
                formatted_ids.append(f"'{id_val}'")
            else:
                formatted_ids.append(str(id_val))

        return ', '.join(formatted_ids)

    def _build_query_from_template(
            self, template: str, sublicense_config: Dict[str, Any],
            year: Optional[int] = None) -> str:
        """
        Build SQL query from template using sublicense configuration.

        Args:
            template: SQL query template with placeholders
            sublicense_config: Sublicense configuration dictionary
            year: Optional year filter for yearly data

        Returns:
            Formatted SQL query string
        """
        try:
            filters = sublicense_config.get('filters', {})

            # Base parameters
            query_params = {
                'schema': self.global_config.get('base_schema', 'gisapdata')
            }

            # Add formatted ID lists for each filter type
            for filter_key, filter_values in filters.items():
                if isinstance(filter_values, list):
                    query_params[filter_key] = self._format_ids_for_sql(filter_values)
                else:
                    query_params[filter_key] = filter_values

            # Handle year filtering for Westminster University type data
            if year is not None:
                query_params['year'] = year

            formatted_query = template.format(**query_params)
            return formatted_query

        except KeyError as e:
            self.logger.error(f"Missing parameter in query template: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Error building query from template: {str(e)}")
            raise

    def execute_sublicense_query(self, sublicense_name: str, query_type: str,
                                 year: Optional[int] = None) -> Optional[pd.DataFrame]:
        """
        Execute a specific query for a sublicense using database-side filtering.

        Args:
            sublicense_name: Name of the sublicense
            query_type: Type of query to execute (from query_templates)
            year: Optional year filter

        Returns:
            DataFrame with filtered results or None if no data
        """
        start_time = time.time()

        sublicense_config = self.sublicenses.get(sublicense_name)
        if not sublicense_config:
            raise ValueError(
                f"Sublicense '{sublicense_name}' not found in configuration")

        query_templates = sublicense_config.get('query_templates', {})
        template = query_templates.get(query_type)

        if not template:
            self.logger.warning(
                f"Query type '{query_type}' not found for sublicense"
                f" '{sublicense_name}'")
            return None

        try:
            # Build query from template
            query = self._build_query_from_template(template, sublicense_config, year)

            self.logger.info(f"Executing {query_type} query for {sublicense_name}")
            self.logger.debug(f"SQL Query: {query}")

            # Execute query
            df = self.sql_manager.execute_query(query)

            execution_time = time.time() - start_time
            self._performance_stats['queries_executed'] += 1

            if df is not None and not df.empty:
                row_count = len(df)
                self._performance_stats['total_rows_processed'] += row_count

                self.logger.info(
                    f"Query successful: {row_count} rows retrieved"
                    f" in {execution_time:.2f}s "
                    f"for {sublicense_name} - {query_type}"
                )
                return df
            else:
                self.logger.warning(
                    f"No data returned for {sublicense_name} - {query_type}")
                return None

        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(
                f"Query execution failed after {execution_time:.2f}s "
                f"for {sublicense_name} - {query_type}: {str(e)}"
            )
            return None

    def save_sublicense_data(self, df: pd.DataFrame,
                             sublicense_name: str,
                             output_key: str,
                             apply_formatting: bool = True) -> Optional[str]:
        """
        Save sublicense data to file with proper formatting.
        Supports both local and S3 file systems.

        Args:
            df: DataFrame to save
            sublicense_name: Name of the sublicense
            output_key: Key for output configuration
            apply_formatting: Whether to apply data-specific formatting

        Returns:
            Full file path if successful, None otherwise
        """
        try:
            sublicense_config = self.sublicenses.get(sublicense_name)
            if not sublicense_config:
                raise ValueError(f"Sublicense '{sublicense_name}' not found")

            output_configs = sublicense_config.get('output_configs', {})
            output_config = output_configs.get(output_key)

            if not output_config:
                self.logger.warning(
                    f"No output configuration found for {output_key}"
                    f" in {sublicense_name}")
                return None

            # Build file path
            file_path = output_config.get('file_path', '')
            resource_title = output_config.get('resource_title', '')

            # Handle both local and S3 paths
            if self.base_dir.startswith('s3://'):
                # For S3 paths, use forward slashes and join properly
                base_path = self.base_dir.rstrip('/')
                file_path_clean = file_path.strip('/')
                full_path = f"{base_path}/{file_path_clean}/{resource_title}"
                fs = fsspec.filesystem('s3')
            else:
                # For local paths, use os.path.join
                full_path = os.path.join(self.base_dir, file_path, resource_title)
                fs = fsspec.filesystem('file')
                # Ensure directory exists for local filesystem
                os.makedirs(os.path.dirname(full_path), exist_ok=True)

            # Apply data-specific formatting
            if apply_formatting:
                df = self._apply_data_formatting(df, output_key, sublicense_name,
                                                 output_config)

            # Save to CSV using fsspec for both local and S3 compatibility
            with fs.open(full_path, 'w', newline='') as f:
                df.to_csv(f, index=False)

            self._performance_stats['files_created'] += 1

            self.logger.info(
                f"Saved {len(df)} rows to {full_path} for {sublicense_name}")
            return full_path

        except Exception as e:
            self.logger.error(
                f"Failed to save data for {sublicense_name} - {output_key}: {str(e)}")
            return None

    def _apply_data_formatting(self, df: pd.DataFrame, output_key: str,
                               sublicense_name: str, output_config: Dict[str, Any]) -> pd.DataFrame:  # noqa: E501
        """
        Apply data-specific formatting based on YAML configuration.

        Args:
            df: DataFrame to format
            output_key: Output configuration key
            sublicense_name: Name of the sublicense
            output_config: Output configuration dictionary

        Returns:
            Formatted DataFrame
        """
        df_copy = df.copy()

        # Get formatting configuration - check output-specific first, then global
        output_formatting = output_config.get('data_formatting', {})
        global_formatting = self.global_config.get('data_formatting', {})

        # Find the appropriate formatting config
        formatting_config = None

        # 1. Check output-specific formatting first
        if output_formatting:
            formatting_config = output_formatting
            self.logger.debug(f"Using output-specific formatting for {output_key}")

        # 2. Check global formatting by output_key
        elif output_key in global_formatting:
            formatting_config = global_formatting[output_key]
            self.logger.debug(f"Using global formatting for {output_key}")

        # 3. Check global formatting by data source type (bt_, mastercard_, etc.)
        else:
            for data_type, config_yaml in global_formatting.items():
                if data_type in output_key.lower():
                    formatting_config = config_yaml
                    self.logger.debug(
                        f"Using global data type formatting for {data_type}")
                    break

        # Apply formatting ONLY if configuration is found
        if formatting_config:
            # Apply column renames first
            column_renames = formatting_config.get('column_renames', {})
            if column_renames:
                df_copy = df_copy.rename(columns=column_renames)
                self.logger.debug(f"Applied column renames: {column_renames}")

            # Apply apostrophe formatting
            apostrophe_columns = formatting_config.get('add_apostrophe_columns', [])
            for col in apostrophe_columns:
                if col in df_copy.columns:
                    df_copy = df_copy.assign(
                        **{col: lambda x, c=col: "'" + x[c].astype(str)})
                    self.logger.debug(f"Added apostrophes to column: {col}")
                else:
                    self.logger.warning(
                        f"Column {col} not found for apostrophe formatting")
        else:
            # No formatting applied - just return the original DataFrame
            self.logger.debug(
                f"No formatting configuration found for {output_key},"
                f" returning data as-is")

        return df_copy

    def upload_to_datastore(self, df: pd.DataFrame, sublicense_name: str,
                            output_key: str, file_path: Optional[str] = None) -> bool:
        """
        Upload sublicense data to London Datastore.

        Args:
            df: DataFrame to upload
            sublicense_name: Name of the sublicense
            output_key: Key for output configuration
            file_path: Optional file path if data was saved to file

        Returns:
            True if successful, False otherwise
        """
        try:
            sublicense_config = self.sublicenses.get(sublicense_name)
            if not sublicense_config:
                raise ValueError(f"Sublicense '{sublicense_name}' not found")

            output_configs = sublicense_config.get('output_configs', {})
            output_config = output_configs.get(output_key)

            if not output_config:
                self.logger.warning(f"No output configuration found for {output_key}")
                return False

            slug = sublicense_config.get('slug')
            if not slug:
                self.logger.warning(f"No slug configured for {sublicense_name}")
                return False

            # Prepare upload parameters
            upload_params = {
                'slug': slug,
                'resource_title': output_config.get('resource_title'),
                'df': df
            }

            # Add file path if provided
            if file_path:
                upload_params['file_path'] = file_path

            # Add custom date column if specified
            custom_date_column = output_config.get('custom_date_column')
            if custom_date_column:
                upload_params['custom_date_column'] = custom_date_column

            # Set up log capture to detect silent failures
            import logging
            import io

            # Create a string buffer to capture log messages
            log_capture = io.StringIO()
            log_handler = logging.StreamHandler(log_capture)
            log_handler.setLevel(logging.ERROR)

            # Add handler to root logger to capture upload errors
            root_logger = logging.getLogger()
            root_logger.addHandler(log_handler)

            upload_start_time = time.time()

            try:
                self.data_writer.upload_data_to_lds(**upload_params)
                upload_time = time.time() - upload_start_time

                # Remove the log handler
                root_logger.removeHandler(log_handler)

                # Check if any errors were logged during upload
                log_contents = log_capture.getvalue()
                if "Failed to upload data to LDS" in log_contents:
                    self.logger.error(
                        f"Upload failed for {sublicense_name} - {output_key}: "
                        f"detected error in logs"
                    )
                    return False

                # If no errors in logs and reasonable time, consider it successful
                # Only increment counter if upload actually succeeded
                self._performance_stats['uploads_completed'] += 1

                self.logger.info(
                    f"Successfully uploaded {output_config.get('resource_title')} "
                    f"for {sublicense_name} to datastore in {upload_time:.2f}s"
                )
                return True

            except Exception as upload_error:
                # Remove the log handler in case of exception
                root_logger.removeHandler(log_handler)

                self.logger.error(
                    f"Upload failed with exception for {sublicense_name}: "
                    f"{str(upload_error)}"
                )
                return False
            finally:
                # Ensure log handler is always removed
                if log_handler in root_logger.handlers:
                    root_logger.removeHandler(log_handler)
                log_capture.close()

        except Exception as e:
            self.logger.error(
                f"Failed to upload to datastore for {sublicense_name}"
                f" - {output_key}: {str(e)}")
            return False

    def process_sublicense_complete(self, sublicense_name: str,
                                    save_files: bool = True,
                                    upload_to_datastore: bool = True) -> Dict[str, Any]:
        """
        Complete processing of a sublicense - execute queries, save files, and upload.

        Args:
            sublicense_name: Name of the sublicense to process
            save_files: Whether to save data to files
            upload_to_datastore: Whether to upload to London Datastore

        Returns:
            Processing results dictionary
        """
        start_time = time.time()

        self.logger.info(
            f"Starting complete processing for sublicense: {sublicense_name}")

        sublicense_config = self.sublicenses.get(sublicense_name)
        if not sublicense_config:
            raise ValueError(
                f"Sublicense '{sublicense_name}' not found in configuration")

        # Check if sublicense is active
        if sublicense_config.get('status') != 'active':
            self.logger.warning(f"Sublicense {sublicense_name} is not active, skipping")
            return {'success': False, 'reason': 'inactive'}

        results = {
            'sublicense': sublicense_name,
            'start_time': datetime.now(),
            'queries_executed': 0,
            'files_saved': 0,
            'datastore_uploads': 0,
            'errors': [],
            'processed_items': []
        }

        query_templates = sublicense_config.get('query_templates', {})
        # output_configs = sublicense_config.get('output_configs', {})

        # Process each query template
        for query_type, template in query_templates.items():
            try:
                # Execute query for this template
                df = self.execute_sublicense_query(sublicense_name, query_type)
                if df is not None:
                    self._process_query_result(
                        df, sublicense_name, query_type,
                        results, save_files, upload_to_datastore
                    )
                else:
                    results['errors'].append(f"No data returned for {query_type}")

                results['queries_executed'] += 1

            except Exception as e:
                error_msg = f"Error processing {query_type}: {str(e)}"
                self.logger.error(error_msg)
                results['errors'].append(error_msg)

        processing_time = time.time() - start_time
        results['processing_time'] = processing_time
        results['success'] = len(results['errors']) == 0

        self.logger.info(
            f"Completed processing {sublicense_name}: "
            f"{results['queries_executed']} queries, "
            f"{results['files_saved']} files, "
            f"{results['datastore_uploads']} uploads "
            f"in {processing_time:.2f}s"
        )

        return results

    def _process_query_result(self, df: pd.DataFrame, sublicense_name: str,
                              query_type: str, results: Dict,
                              save_files: bool, upload_to_datastore: bool):
        """Process the result of a query execution"""
        sublicense_config = self.sublicenses.get(sublicense_name)
        output_configs = sublicense_config.get('output_configs', {})

        # Find matching output configuration
        output_key = self._find_matching_output_key(query_type, output_configs)

        if not output_key:
            self.logger.warning(f"No matching output configuration for {query_type}")
            return

        file_path = None

        # Save to file if requested
        if save_files:
            file_path = self.save_sublicense_data(df, sublicense_name, output_key)
            if file_path:
                results['files_saved'] += 1

        # Upload to datastore if requested
        if upload_to_datastore:
            if self.upload_to_datastore(df, sublicense_name, output_key, file_path):
                results['datastore_uploads'] += 1

        results['processed_items'].append({
            'query_type': query_type,
            'output_key': output_key,
            'rows': len(df),
            'file_saved': file_path is not None,
            'uploaded': upload_to_datastore
        })

    def _find_matching_output_key(
            self, query_type: str, output_configs: Dict) -> Optional[str]:
        """Find the matching output configuration key for a query type"""
        # Direct match
        if query_type in output_configs:
            return query_type

        # Split query type into meaningful parts
        query_parts = query_type.split('_')

        # Try progressive matching - start with full query, then remove parts from end
        for i in range(len(query_parts), 0, -1):
            partial_key = '_'.join(query_parts[:i])
            if partial_key in output_configs:
                self.logger.debug(
                    f"Found partial match: {query_type} -> {partial_key}")
                return partial_key

        # Try matching with common prefixes
        for output_key in output_configs.keys():
            output_parts = output_key.split('_')

            # Check if the output key is a prefix of the query type
            if len(output_parts) <= len(query_parts):
                if output_parts == query_parts[:len(output_parts)]:
                    self.logger.debug(
                        f"Found prefix match: {query_type} -> {output_key}")
                    return output_key

            # Check if they share significant common parts
            common_parts = set(output_parts) & set(query_parts)
            if common_parts:
                # If we have at least one part longer than 3 chars, or 2+ parts in common
                significant_parts = [p for p in common_parts if len(p) > 3]
                if significant_parts or len(common_parts) >= 2:
                    self.logger.debug(
                        f"Found semantic match: {query_type} -> {output_key}")
                    return output_key

        # Special handling for yearly data
        if 'yearly' in query_type:
            for output_key in output_configs.keys():
                if any(year in output_key for year in ['2022', '2023', '2024', '2025']):
                    return output_key

        self.logger.warning(f"No matching output key found for query type: {query_type}")
        self.logger.debug(f"Available output keys: {list(output_configs.keys())}")
        return None

    def process_all_sublicenses(self, include_inactive: bool = False,
                                save_files: bool = True,
                                upload_to_datastore: bool = True) -> Dict[str, Any]:
        """
        Process all sublicenses in the configuration.

        Args:
            include_inactive: Whether to process inactive sublicenses
            save_files: Whether to save data to files
            upload_to_datastore: Whether to upload to London Datastore

        Returns:
            Overall processing results
        """
        start_time = time.time()

        self.logger.info("Starting processing for all sublicenses")

        overall_results = {
            'start_time': datetime.now(),
            'sublicense_results': {},
            'summary': {
                'total_sublicenses': 0,
                'successful_sublicenses': 0,
                'total_queries': 0,
                'total_files': 0,
                'total_uploads': 0,
                'total_errors': 0
            }
        }

        for sublicense_name, sublicense_config in self.sublicenses.items():
            # Skip inactive sublicenses unless explicitly requested
            if not include_inactive and sublicense_config.get('status') != 'active':
                self.logger.info(f"Skipping inactive sublicense: {sublicense_name}")
                continue

            try:
                results = self.process_sublicense_complete(
                    sublicense_name, save_files, upload_to_datastore
                )
                overall_results['sublicense_results'][sublicense_name] = results

                # Update summary
                overall_results['summary']['total_sublicenses'] += 1
                if results.get('success'):
                    overall_results['summary']['successful_sublicenses'] += 1

                overall_results['summary']['total_queries'] += results.get(
                    'queries_executed', 0)
                overall_results['summary']['total_files'] += results.get(
                    'files_saved', 0)
                overall_results['summary']['total_uploads'] += results.get(
                    'datastore_uploads', 0)
                overall_results['summary']['total_errors'] += len(
                    results.get('errors', []))

            except Exception as e:
                error_msg = f"Critical error processing {sublicense_name}: {str(e)}"
                self.logger.error(error_msg)
                overall_results['sublicense_results'][sublicense_name] = {
                    'success': False,
                    'error': error_msg
                }

        total_time = time.time() - start_time
        overall_results['total_processing_time'] = total_time
        overall_results['performance_stats'] = self._performance_stats

        summary = overall_results['summary']
        self.logger.info(
            f"Completed processing all sublicenses: "
            f"{summary['successful_sublicenses']}/"
            f"{summary['total_sublicenses']} successful, "
            f"{summary['total_queries']} queries executed, "
            f"{summary['total_files']} files created, "
            f"{summary['total_uploads']} datastore uploads, "
            f"{summary['total_errors']} errors "
            f"in {total_time:.2f}s"
        )

        return overall_results

    def get_sublicense_info(
            self, sublicense_name: Optional[str] = None) -> Dict[str, Any]:
        """Get information about sublicense(s)"""
        if sublicense_name:
            config = self.sublicenses.get(sublicense_name, {})
            if config:
                return {
                    'name': sublicense_name,
                    'slug': config.get('slug'),
                    'description': config.get('description'),
                    'status': config.get('status'),
                    'contact': config.get('contact'),
                    'data_sources': config.get('data_sources', []),
                    'filters': config.get('filters', {}),
                    'query_count': len(config.get('query_templates', {})),
                    'output_count': len(config.get('output_configs', {}))
                }
            else:
                return {'error': f'Sublicense {sublicense_name} not found'}
        else:
            active_count = len([
                s for s in self.sublicenses.values()
                if s.get('status') == 'active'
            ])

            return {
                'total_sublicenses': len(self.sublicenses),
                'active_sublicenses': active_count,
                'inactive_sublicenses': len(self.sublicenses) - active_count,
                'sublicense_names': list(self.sublicenses.keys()),
                'performance_stats': self._performance_stats
            }

    def validate_configuration(self) -> Dict[str, Any]:
        """Validate the sublicense configuration"""
        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'sublicense_validation': {}
        }

        # Validate global configuration
        if not self.global_config.get('base_schema'):
            validation_results['errors'].append(
                "Missing base_schema in global configuration")
        # Validate each sublicense
        for name, sublicense_config in self.sublicenses.items():
            sublicense_validation = {
                'valid': True,
                'errors': [],
                'warnings': []
            }

            # Required fields validation
            required_fields = ['slug', 'description', 'data_sources', 'status']
            for field in required_fields:
                if not sublicense_config.get(field):
                    error = f"Missing required field '{field}'"
                    sublicense_validation['errors'].append(error)
                    validation_results['errors'].append(f"{name}: {error}")

            # Data sources validation
            data_sources = sublicense_config.get('data_sources', [])
            global_data_sources = self.global_config.get('data_sources', {})

            for ds in data_sources:
                base_ds = ds.replace('_yearly', '')  # Handle yearly variants
                if base_ds not in global_data_sources:
                    warning = f"Data source '{ds}' not defined in global configuration"
                    sublicense_validation['warnings'].append(warning)
                    validation_results['warnings'].append(f"{name}: {warning}")

            # Query templates validation
            query_templates = sublicense_config.get('query_templates', {})
            if not query_templates:
                warning = "No query templates defined"
                sublicense_validation['warnings'].append(warning)
                validation_results['warnings'].append(f"{name}: {warning}")

            # Output configs validation
            output_configs = sublicense_config.get('output_configs', {})
            if not output_configs:
                warning = "No output configurations defined"
                sublicense_validation['warnings'].append(warning)
                validation_results['warnings'].append(f"{name}: {warning}")

            # Status validation
            status = sublicense_config.get('status')
            if status not in ['active', 'inactive']:
                warning = f"Invalid status '{status}', should be 'active' or 'inactive'"
                sublicense_validation['warnings'].append(warning)
                validation_results['warnings'].append(f"{name}: {warning}")

            if sublicense_validation['errors']:
                sublicense_validation['valid'] = False

            validation_results['sublicense_validation'][name] = sublicense_validation

        if validation_results['errors']:
            validation_results['valid'] = False

        return validation_results

    def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics"""
        return {
            'performance_stats': self._performance_stats,
            'sublicense_count': len(self.sublicenses),
            'active_sublicense_count': len([
                s for s in self.sublicenses.values()
                if s.get('status') == 'active'
            ])
        }
