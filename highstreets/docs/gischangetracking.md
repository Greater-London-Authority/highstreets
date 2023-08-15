# Data Processor Documentation
This comprehensive documentation explains the `DataProcessor` class and its methods in detail. This class is an integral part of our data processing pipeline, responsible for handling changes related to highstreets, bids, town centres, and bespoke areas. It collaborates with the DataLoader class to fetch data and interacts with our PostgreSQL database to manage change tracking. It's responsible for orchestrating the processing of incoming data changes, transforming them into actionable insights, and ensuring seamless integration into our data ecosystem.

## DataProcessor Class Overview
The `DataProcessor` class plays a pivotal role in processing changes in our data sources. By understanding its methods, the team can effectively utilize it to manage updates and inserts across different data categories.

## Initialization
Upon instantiation, the class establishes a connection to our PostgreSQL database using the SQLAlchemy library. It fetches essential database connection parameters from environment variables, ensuring that sensitive credentials are kept secure. This modular setup promotes reusability and easy configuration across different environments. The class is initialized by creating an instance of the `DataLoader` class and setting up the database connection parameters. These parameters are retrieved from environment variables, ensuring security and configurability. Lists are created to store hex IDs associated with highstreets, town centres, bids, and bespoke areas.

## process_changes Method
The heart of the class lies in the `process_changes` method. This method acts as a decision-making hub, retrieving unprocessed change records from the database. Each change record is examined to determine whether it involves an insertion or an update. The method then delegates the processing to the respective specialized methods: `process_insert` and `process_update`.
```python
def process_changes(self):
    # Retrieve change tracking data from the database
    query = """
        SELECT table_name, record_id, change_type, change_timestamp
        FROM econ_hsds_hs_bid_tc_bespoke_change_tracking
        WHERE processed = FALSE
        ORDER BY change_timestamp ASC
    """
    df = pd.read_sql_query(text(query), self.engine.connect())

    # Process each change record
    if not df.empty:
        for _index, row in df.iterrows():
            table_name, record_id, change_type, change_timestamp = row

            if change_type == "INSERT":
                self.process_insert(table_name, record_id)
            elif change_type == "UPDATE":
                self.process_update(table_name, record_id)

        # Update the processed records to prevent re-processing
        self.update_processed_records(df["record_id"].tolist())
    else:
        logging.info("No new hs/bid/tc/bespoke updated or inserted")
```
## process_insert Method
When an insert change is detected, the `process_insert` method comes into play. It collaborates with the `DataLoader` class to retrieve hex IDs associated with the inserted record. These hex IDs, representing spatial locations, are then stored for future analysis. For instance, if a new highstreet is inserted, the method fetches hex IDs relevant to that highstreet and saves them. This step provides valuable spatial context for each new record.
```python
def process_insert(self, table_name, record_id):
    hex_ids = []

    if table_name == "highstreet":
        hex_ids = self.data_loader.get_hex_lookup("highstreet")
        # Filter relevant hex IDs and store them for later use
        self.hs_hex_ids.extend(
            [
                (hex_id, record_id)
                for hex_id in hex_ids.loc[
                    hex_ids["highstreet_id"] == record_id, "hex_id"
                ].values
            ]
        )
    # ... Similar logic for other table types
    # Log the INSERT change
    logging.info(f"INSERT: Change in table {table_name} for record ID {record_id}.")
```
## process_update Method
When an update change is identified, the `process_update` method is invoked. Although it currently serves as a placeholder for custom logic, it's designed to accommodate any specialized processing required for update changes. This design allows flexibility for future enhancements, such as applying specific transformations or aggregations based on the updated data.
```python
def process_update(self, table_name, record_id):
    # Placeholder for custom logic to process UPDATE changes
    logging.info(f"UPDATE: Change in table {table_name} for record ID {record_id}.")
```
## update_processed_records Method
After successfully processing changes, the `update_processed_records` method ensures that the processed records are appropriately marked as "processed" in the change tracking table. This prevents duplicate processing and establishes an audit trail for the processed changes.

```python
def update_processed_records(self, processed_ids):
    if processed_ids:
        # Update the change tracking table to mark records as processed
        processed_query = f"""
            UPDATE econ_hsds_hs_bid_tc_bespoke_change_tracking
            SET processed = TRUE
            WHERE record_id IN ({', '.join(map(str, processed_ids))})
        """
        with self.engine.begin() as conn:
            conn.execute(text(processed_query))
        logging.info("Processed records updated.")
```
## conclusion
In summary, the `DataProcessor` class functions as a robust data change management engine. It bridges the gap between incoming changes, our PostgreSQL database, and our broader data ecosystem. By efficiently categorizing and processing insert and update changes, marking processed records, and laying the groundwork for checkpointing, the class enhances the reliability, traceability, and efficiency of our data processing pipeline. Its modular structure and clear method responsibilities empower data engineers to work seamlessly within our data management framework, ensuring accurate and up-to-date insights for downstream consumption.
