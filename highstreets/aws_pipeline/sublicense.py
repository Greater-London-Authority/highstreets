from highstreets.core.sublicense_manager import SublicenseManager
# sub-licensing agreement for colliers

# Initialize the manager
manager = SublicenseManager()

# Process Colliers HSDS sublicense
results = manager.process_sublicense_complete(
    'colliers-hsds',
    save_files=True,
    upload_to_datastore=True)
