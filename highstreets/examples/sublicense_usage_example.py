"""
Sublicense Processing Example

This example demonstrates how to use the enhanced database-driven 
sublicense system with YAML configuration.

Run this script to process sublicenses using the new industrial-standard approach.
"""

import logging
from highstreets.core.sublicense_manager import SublicenseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def example_process_single_sublicense():
    """Example: Process a single sublicense"""
    print("=== Processing Single Sublicense Example ===")
    
    # Initialize the manager
    manager = SublicenseManager()
    
    # Process Colliers HSDS sublicense
    results = manager.process_sublicense_complete('colliers-hsds')
    
    print(f"Processing Results:")
    print(f"Success: {results['success']}")
    print(f"Queries executed: {results.get('queries_executed', 0)}")
    print(f"Files saved: {results.get('files_saved', 0)}")
    print(f"Datastore uploads: {results.get('datastore_uploads', 0)}")
    print(f"Processing time: {results.get('processing_time', 0):.2f}s")
    
    if results.get('errors'):
        print("Errors:")
        for error in results['errors']:
            print(f"  - {error}")


def example_process_all_sublicenses():
    """Example: Process all active sublicenses"""
    print("\n=== Processing All Sublicenses Example ===")
    
    manager = SublicenseManager()
    
    # Process all active sublicenses
    results = manager.process_all_sublicenses()
    
    summary = results['summary']
    print(f"Overall Results:")
    print(f"Sublicenses processed: {summary['successful_sublicenses']}/{summary['total_sublicenses']}")
    print(f"Total queries: {summary['total_queries']}")
    print(f"Files created: {summary['total_files']}")
    print(f"Datastore uploads: {summary['total_uploads']}")
    print(f"Total errors: {summary['total_errors']}")
    print(f"Total time: {results['total_processing_time']:.2f}s")


def example_execute_specific_query():
    """Example: Execute a specific query for a sublicense"""
    print("\n=== Execute Specific Query Example ===")
    
    manager = SublicenseManager()
    
    # Execute BT footfall query for Fitzrovia
    df = manager.execute_sublicense_query('fitzrovia-partnership', 'bt_footfall_bid')
    
    if df is not None:
        print(f"Retrieved {len(df)} rows for Fitzrovia BT footfall data")
        print("Columns:", list(df.columns))
        print("Date range:", df['count_date'].min(), "to", df['count_date'].max())
    else:
        print("No data retrieved")


def example_get_sublicense_info():
    """Example: Get information about sublicenses"""
    print("\n=== Sublicense Information Example ===")
    
    manager = SublicenseManager()
    
    # Get overview of all sublicenses
    overview = manager.get_sublicense_info()
    print(f"Overview:")
    print(f"Total sublicenses: {overview['total_sublicenses']}")
    print(f"Active sublicenses: {overview['active_sublicenses']}")
    print(f"Inactive sublicenses: {overview['inactive_sublicenses']}")
    
    # Get detailed info for a specific sublicense
    colliers_info = manager.get_sublicense_info('colliers-hsds')
    print(f"\nColliers HSDS Details:")
    print(f"Slug: {colliers_info['slug']}")
    print(f"Description: {colliers_info['description']}")
    print(f"Status: {colliers_info['status']}")
    print(f"Data sources: {colliers_info['data_sources']}")
    print(f"Query templates: {colliers_info['query_count']}")
    print(f"Output configs: {colliers_info['output_count']}")


def example_validate_configuration():
    """Example: Validate the YAML configuration"""
    print("\n=== Configuration Validation Example ===")
    
    manager = SublicenseManager()
    
    validation = manager.validate_configuration()
    print(f"Configuration valid: {validation['valid']}")
    
    if validation['errors']:
        print("Errors:")
        for error in validation['errors']:
            print(f"  - {error}")
    
    if validation['warnings']:
        print("Warnings:")
        for warning in validation['warnings']:
            print(f"  - {warning}")


def example_save_and_upload_separately():
    """Example: Save files and upload separately"""
    print("\n=== Separate Save and Upload Example ===")
    
    manager = SublicenseManager()
    
    # Execute query
    df = manager.execute_sublicense_query('avison-young', 'bt_footfall_towncentre')
    
    if df is not None:
        # Save to file only
        file_path = manager.save_sublicense_data(df, 'avison-young', 'bt_footfall')
        print(f"Saved to file: {file_path}")
        
        # Upload to datastore separately
        upload_success = manager.upload_to_datastore(df, 'avison-young', 'bt_footfall', file_path)
        print(f"Upload successful: {upload_success}")


def example_performance_monitoring():
    """Example: Monitor performance statistics"""
    print("\n=== Performance Monitoring Example ===")
    
    manager = SublicenseManager()
    
    # Process a few sublicenses
    manager.process_sublicense_complete('fitzrovia-partnership')
    manager.process_sublicense_complete('knightsbridge-partnership')
    
    # Get performance stats
    stats = manager.get_performance_stats()
    print("Performance Statistics:")
    print(f"Queries executed: {stats['performance_stats']['queries_executed']}")
    print(f"Rows processed: {stats['performance_stats']['total_rows_processed']}")
    print(f"Files created: {stats['performance_stats']['files_created']}")
    print(f"Uploads completed: {stats['performance_stats']['uploads_completed']}")


def example_westminster_university_yearly():
    """Example: Process Westminster University yearly data"""
    print("\n=== Westminster University Yearly Data Example ===")
    
    manager = SublicenseManager()
    
    # This will automatically process data for each year (2022, 2023, 2024, 2025)
    results = manager.process_sublicense_complete('westminster-university')
    
    print(f"Westminster University Results:")
    print(f"Success: {results['success']}")
    print(f"Queries executed: {results.get('queries_executed', 0)}")
    print(f"Files saved: {results.get('files_saved', 0)}")
    
    # Show processed items
    for item in results.get('processed_items', []):
        print(f"  - {item['query_type']}: {item['rows']} rows")


if __name__ == "__main__":
    """
    Run all examples to demonstrate the sublicense system capabilities.
    
    Uncomment specific examples to run individually:
    """
    
    try:
        # Basic examples
        example_get_sublicense_info()
        example_validate_configuration()
        
        # Processing examples  
        example_process_single_sublicense()
        example_execute_specific_query()
        
        # Advanced examples
        example_save_and_upload_separately()
        example_westminster_university_yearly()
        example_performance_monitoring()
        
        # Comprehensive processing (uncomment to run)
        # example_process_all_sublicenses()
        
        print("\n=== All Examples Completed Successfully ===")
        
    except Exception as e:
        print(f"Error running examples: {str(e)}")
        logging.error(f"Example execution failed: {str(e)}", exc_info=True) 