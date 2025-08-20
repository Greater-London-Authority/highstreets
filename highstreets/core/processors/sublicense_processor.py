"""
Database-Driven Sublicense Processor

This module provides an industrial-standard approach to processing sublicense data
directly from the database without loading full datasets into memory first.

Key Features:
- Database-side filtering using SQL queries
- YAML-driven configuration
- Support for multiple data sources (BT footfall, Mastercard, etc.)
- Flexible output formats and destinations
- Performance monitoring and logging
- Automatic file and datastore uploads

Usage:
    processor = SublicenseProcessor()
    processor.process_all_sublicenses()
    # or
    processor.process_sublicense('colliers-hsds')
"""

import os
import yaml
import logging
import pandas as pd
from typing import Dict, List, Optional, Union, Any
from datetime import datetime
import time

from highstreets.core.sql_manager import SQLManager
from highstreets.data_source_sink.datawriter import DataWriter
from highstreets import config


class SublicenseProcessor:
    """
    Database-driven sublicense processor that executes SQL queries
    directly against the database to filter and extract data for
    each sublicense agreement.
    """

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the sublicense processor.

        Args:
            config_path: Path to sublicense YAML configuration file
        """
        self.logger = self._setup_logging()
        self.config_path = config_path or os.path.join(
            os.path.dirname(__file__), '..', 'settings', 'sublicenses.yaml'
        )

        # Load configuration
        self.config = self._load_config()
        self.global_config = self.config.get('config', {})
        self.sublicenses = self.config.get('sublicenses', {})

        # Initialize components
        self.sql_manager = SQLManager()
        self.data_writer = DataWriter()
        self.base_dir = config.BASE_DIR

        self.logger.info(f"Initialized SublicenseProcessor with"
                         f" {len(self.sublicenses)} sublicenses")

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
                config = yaml.safe_load(file)
            self.logger.info(f"Loaded sublicense configuration from {self.config_path}")
            return config
        except Exception as e:
            self.logger.error(f"Failed to load configuration: {str(e)}")
            raise

    def _format_ids_for_sql(self, ids: List[Union[int, str]]) -> str:
        """Format a list of IDs for SQL IN clause"""
        if not ids:
            return ""

        # Convert all to strings and wrap in quotes if needed
        formatted_ids = []
        for id_val in ids:
            if isinstance(id_val, str):
                formatted_ids.append(f"'{id_val}'")
            else:
                formatted_ids.append(str(id_val))

        return ', '.join(formatted_ids)

    def _build_query(self, template: str, sublicense_config: Dict[str, Any]) -> str:
        """
        Build SQL query from template using sublicense configuration.

        Args:
            template: SQL query template with placeholders
            sublicense_config: Sublicense configuration dict

        Returns:
            Formatted SQL query string
        """
        try:
            # Get filters from sublicense config
            filters = sublicense_config.get('filters', {})

            # Format the template with schema and filter values
            query_params = {
                'schema': self.global_config.get('base_schema', 'gisapdata')
            }

            # Add formatted ID lists for each filter type
            for filter_key, filter_values in filters.items():
                if isinstance(filter_values, list):
                    query_params[filter_key] = self._format_ids_for_sql(filter_values)
                else:
                    query_params[filter_key] = filter_values

            # Handle special cases like year filtering
            if 'year' in template:
                years = filters.get('years', [])
                if years:
                    query_params['year'] = years[0]  # Use first year as default

            formatted_query = template.format(**query_params)
            return formatted_query

        except Exception as e:
            self.logger.error(f"Error building query from template: {str(e)}")
            raise

    def _execute_query(self, query: str, sublicense_name: str, data_source: str) -> Optional[pd.DataFrame]:  # noqa: E501
        """
        Execute SQL query and return DataFrame.

        Args:
            query: SQL query to execute
            sublicense_name: Name of sublicense for logging
            data_source: Data source name for logging

        Returns:
            DataFrame with query results or None if error
        """
        try:
            start_time = time.time()

            self.logger.info(f"Executing query for {sublicense_name} - {data_source}")
            self.logger.debug(f"Query: {query}")

            df = self.sql_manager.execute_query_to_dataframe(query)

            execution_time = time.time() - start_time

            if df is not None and not df.empty:
                self.logger.info(
                    f"Query successful: {len(df)} rows retrieved in"
                    f" {execution_time:.2f}s for {sublicense_name} - {data_source}"
                )
                return df
            else:
                self.logger.warning(
                    f"Query returned no data for {sublicense_name} - {data_source}"
                )
                return None

        except Exception as e:
            self.logger.error(
                f"Query execution failed for"
                f" {sublicense_name} - {data_source}: {str(e)}")
            return None

    def _save_to_file(self, df: pd.DataFrame, output_config: Dict[str, Any],
                      sublicense_name: str, data_source: str) -> Optional[str]:
        """
        Save DataFrame to CSV file.

        Args:
            df: DataFrame to save
            output_config: Output configuration
            sublicense_name: Sublicense name for logging
            data_source: Data source name for logging

        Returns:
            Full file path if successful, None otherwise
        """
        try:
            file_path = output_config.get('file_path', '')
            resource_title = output_config.get('resource_title', '')

            # Construct full file path
            full_path = os.path.join(self.base_dir, file_path, resource_title)

            # Ensure directory exists
            os.makedirs(os.path.dirname(full_path), exist_ok=True)

            # Apply data formatting based on configuration
            df_formatted = self._apply_data_formatting(df, data_source, output_config)

            # Save to CSV
            df_formatted.to_csv(full_path, index=False)

            self.logger.info(
                f"Saved {len(df_formatted)} rows to {full_path} for {sublicense_name}")
            return full_path

        except Exception as e:
            self.logger.error(
                f"Failed to save file for"
                f" {sublicense_name} - {data_source}: {str(e)}")
            return None

    def _upload_to_datastore(self, df: pd.DataFrame, full_path: str,
                             output_config: Dict[str, Any],
                             sublicense_config: Dict[str, Any],
                             sublicense_name: str, data_source: str) -> bool:
        """
        Upload data to London Datastore.

        Args:
            df: DataFrame to upload
            full_path: Full file path
            output_config: Output configuration
            sublicense_config: Sublicense configuration
            sublicense_name: Sublicense name
            data_source: Data source name

        Returns:
            True if successful, False otherwise
        """
        try:
            slug = sublicense_config.get('slug')
            resource_title = output_config.get('resource_title')
            custom_date_column = output_config.get('custom_date_column')

            if not slug:
                self.logger.warning(f"No slug configured for {sublicense_name}")
                return False

            upload_params = {
                'slug': slug,
                'resource_title': resource_title,
                'df': df,
                'file_path': full_path
            }

            if custom_date_column:
                upload_params['custom_date_column'] = custom_date_column

            self.data_writer.upload_data_to_lds(**upload_params)

            self.logger.info(
                f"Successfully uploaded to datastore: {resource_title} for"
                f" {sublicense_name}")
            return True

        except Exception as e:
            self.logger.error(
                f"Failed to upload to datastore for"
                f" {sublicense_name} - {data_source}: {str(e)}")
            return False

    def _apply_data_formatting(self, df: pd.DataFrame, data_source: str,
                               output_config: Dict[str, Any]) -> pd.DataFrame:
        """
        Apply data-specific formatting based on YAML configuration.

        Args:
            df: DataFrame to format
            data_source: Data source name
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
            self.logger.debug(f"Using output-specific formatting for {data_source}")

        # 2. Check global formatting by data_source
        elif data_source in global_formatting:
            formatting_config = global_formatting[data_source]
            self.logger.debug(f"Using global formatting for {data_source}")

        # 3. Check global formatting by data source type (bt_, mastercard_, etc.)
        else:
            for data_type, config_yaml in global_formatting.items():
                if data_type in data_source.lower():
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
                f"No formatting configuration found for {data_source},"
                f" returning data as-is")

        return df_copy

    def process_sublicense_data_source(self, sublicense_name: str, data_source: str) -> Dict[str, Any]:  # noqa: E501
        """
        Process a specific data source for a sublicense.

        Args:
            sublicense_name: Name of the sublicense
            data_source: Name of the data source

        Returns:
            Processing results dictionary
        """
        sublicense_config = self.sublicenses.get(sublicense_name)
        if not sublicense_config:
            raise ValueError(
                f"Sublicense '{sublicense_name}' not found in configuration")

        query_templates = sublicense_config.get('query_templates', {})
        output_configs = sublicense_config.get('output_configs', {})

        results = {
            'success': False,
            'rows_processed': 0,
            'files_created': [],
            'datastore_uploads': [],
            'errors': []
        }

        # Find matching query templates for this data source
        matching_templates = {k: v for k, v in query_templates.items() if data_source in k}  # noqa: E501

        if not matching_templates:
            self.logger.warning(
                f"No query templates found for {data_source} in"
                f" {sublicense_name}")
            return results

        for template_name, template_query in matching_templates.items():
            try:
                # Build and execute query
                query = self._build_query(template_query, sublicense_config)
                df = self._execute_query(query, sublicense_name, template_name)

                if df is None or df.empty:
                    continue

                results['rows_processed'] += len(df)

                # Find corresponding output configuration
                output_config = None
                for output_key, output_conf in output_configs.items():
                    if data_source in output_key or template_name.replace('_', '') in output_key.replace('_', ''):  # noqa: E501
                        output_config = output_conf
                        break

                if not output_config:
                    # Try to find a generic output config for this data source
                    output_config = output_configs.get(data_source)

                if output_config:
                    # Apply formatting
                    df = self._apply_data_formatting(df, data_source, output_config)

                    # Save to file
                    file_path = self._save_to_file(df, output_config,
                                                   sublicense_name,
                                                   template_name)
                    if file_path:
                        results['files_created'].append(file_path)

                        # Upload to datastore
                        if self._upload_to_datastore(df, file_path, output_config,
                                                     sublicense_config, sublicense_name,
                                                     template_name):
                            results['datastore_uploads'].append(
                                output_config.get('resource_title'))

                results['success'] = True

            except Exception as e:
                error_msg = f"Error processing {template_name}: {str(e)}"
                self.logger.error(error_msg)
                results['errors'].append(error_msg)

        return results

    def process_sublicense(self, sublicense_name: str) -> Dict[str, Any]:
        """
        Process all data sources for a specific sublicense.

        Args:
            sublicense_name: Name of the sublicense to process

        Returns:
            Processing results dictionary
        """
        start_time = time.time()

        self.logger.info(f"Starting processing for sublicense: {sublicense_name}")

        sublicense_config = self.sublicenses.get(sublicense_name)
        if not sublicense_config:
            raise ValueError(
                f"Sublicense '{sublicense_name}' not"
                f" found in configuration")

        # Check if sublicense is active
        if sublicense_config.get('status') != 'active':
            self.logger.warning(f"Sublicense {sublicense_name} is not active, skipping")
            return {'success': False, 'reason': 'inactive'}

        data_sources = sublicense_config.get('data_sources', [])

        overall_results = {
            'sublicense': sublicense_name,
            'start_time': datetime.now(),
            'total_rows': 0,
            'total_files': 0,
            'total_uploads': 0,
            'data_source_results': {},
            'errors': []
        }

        for data_source in data_sources:
            try:
                self.logger.info(f"Processing {data_source} for {sublicense_name}")

                results = self.process_sublicense_data_source(
                    sublicense_name, data_source)
                overall_results['data_source_results'][data_source] = results

                overall_results['total_rows'] += results.get('rows_processed', 0)
                overall_results['total_files'] += len(results.get('files_created', []))
                overall_results['total_uploads'] += len(results.get(
                    'datastore_uploads', []))
                overall_results['errors'].extend(results.get('errors', []))

            except Exception as e:
                error_msg = (f"Failed to process"
                             f" {data_source} for {sublicense_name}: {str(e)}")
                self.logger.error(error_msg)
                overall_results['errors'].append(error_msg)

        processing_time = time.time() - start_time
        overall_results['processing_time'] = processing_time
        overall_results['success'] = len(overall_results['errors']) == 0

        self.logger.info(
            f"Completed processing {sublicense_name}: "
            f"{overall_results['total_rows']} rows, "
            f"{overall_results['total_files']} files, "
            f"{overall_results['total_uploads']} uploads "
            f"in {processing_time:.2f}s"
        )

        return overall_results

    def process_all_sublicenses(self, include_inactive: bool = False) -> Dict[str, Any]:
        """
        Process all sublicenses in the configuration.

        Args:
            include_inactive: Whether to process inactive sublicenses

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
                'total_rows': 0,
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
                results = self.process_sublicense(sublicense_name)
                overall_results['sublicense_results'][sublicense_name] = results

                # Update summary
                overall_results['summary']['total_sublicenses'] += 1
                if results.get('success'):
                    overall_results['summary']['successful_sublicenses'] += 1

                overall_results['summary']['total_rows'] += results.get('total_rows', 0)
                overall_results['summary']['total_files'] += results.get(
                    'total_files', 0)
                overall_results['summary']['total_uploads'] += results.get(
                    'total_uploads', 0)
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

        summary = overall_results['summary']
        self.logger.info(
            f"Completed processing all sublicenses: "
            f"{summary['successful_sublicenses']}/{summary['total_sublicenses']} successful, "  # noqa: E501
            f"{summary['total_rows']} total rows, "
            f"{summary['total_files']} files created, "
            f"{summary['total_uploads']} datastore uploads, "
            f"{summary['total_errors']} errors "
            f"in {total_time:.2f}s"
        )

        return overall_results

    def get_sublicense_info(self, sublicense_name: Optional[str] = None) -> Dict[str, Any]:  # noqa: E501
        """
        Get information about sublicense(s).

        Args:
            sublicense_name: Specific sublicense name, or None for all

        Returns:
            Sublicense information
        """
        if sublicense_name:
            return self.sublicenses.get(sublicense_name, {})
        else:
            return {
                'total_sublicenses': len(self.sublicenses),
                'active_sublicenses': len([
                    s for s in self.sublicenses.values()
                    if s.get('status') == 'active'
                ]),
                'sublicenses': list(self.sublicenses.keys())
            }

    def validate_configuration(self) -> Dict[str, Any]:
        """
        Validate the sublicense configuration.

        Returns:
            Validation results
        """
        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': []
        }

        # Check global config
        if not self.global_config.get('base_schema'):
            validation_results['errors'].append("Missing base_schema in global config")

        # Check each sublicense
        for name, config_yaml in self.sublicenses.items():
            # Required fields
            required_fields = ['slug', 'description', 'data_sources']
            for field in required_fields:
                if not config_yaml.get(field):
                    validation_results['errors'].append(f"Missing {field} in {name}")

            # Check data sources exist in global config
            data_sources = config_yaml.get('data_sources', [])
            global_data_sources = self.global_config.get('data_sources', {})

            for ds in data_sources:
                if ds not in global_data_sources and not ds.endswith('_yearly'):
                    validation_results['warnings'].append(
                        f"Data source '{ds}' in {name} not defined in global config"
                    )

        if validation_results['errors']:
            validation_results['valid'] = False

        return validation_results


def main():
    """Main function for command-line usage"""
    import argparse

    parser = argparse.ArgumentParser(description='Process sublicense data')
    parser.add_argument('--sublicense', '-s', help='Process specific sublicense')
    parser.add_argument(
        '--all', '-a', action='store_true', help='Process all sublicenses')
    parser.add_argument(
        '--validate', '-v', action='store_true', help='Validate configuration')
    parser.add_argument(
        '--info', '-i', help='Get sublicense info')
    parser.add_argument(
        '--include-inactive', action='store_true', help='Include inactive sublicenses')

    args = parser.parse_args()

    processor = SublicenseProcessor()

    if args.validate:
        results = processor.validate_configuration()
        print("Configuration validation results:")
        print(f"Valid: {results['valid']}")
        if results['errors']:
            print("Errors:")
            for error in results['errors']:
                print(f"  - {error}")
        if results['warnings']:
            print("Warnings:")
            for warning in results['warnings']:
                print(f"  - {warning}")

    elif args.info is not None:
        info = processor.get_sublicense_info(args.info if args.info else None)
        print("Sublicense information:")
        for key, value in info.items():
            print(f"  {key}: {value}")

    elif args.sublicense:
        results = processor.process_sublicense(args.sublicense)
        print(f"Processing results for {args.sublicense}:")
        print(f"Success: {results['success']}")
        print(f"Rows processed: {results.get('total_rows', 0)}")
        print(f"Files created: {results.get('total_files', 0)}")
        print(f"Datastore uploads: {results.get('total_uploads', 0)}")
        if results.get('errors'):
            print("Errors:")
            for error in results['errors']:
                print(f"  - {error}")

    elif args.all:
        results = processor.process_all_sublicenses(
            include_inactive=args.include_inactive)
        summary = results['summary']
        print("Processing summary:")
        print(f"Sublicenses processed: {summary['successful_sublicenses']}/"
              f"{summary['total_sublicenses']}")
        print(f"Total rows: {summary['total_rows']}")
        print(f"Files created: {summary['total_files']}")
        print(f"Datastore uploads: {summary['total_uploads']}")
        print(f"Total errors: {summary['total_errors']}")
        print(f"Processing time: {results['total_processing_time']:.2f}s")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
