# BT hex footfall data transformations Documentation
The `HexTransform` class is a pivotal component of our data engineering framework, tailored to handle the transformation of input data containing High Street Data Service (HSDS) information. It specializes in converting raw data, such as hex IDs, dates, total volume, worker population percentages, and resident population percentages, into a more structured and meaningful output format. The transformation process is vital for downstream analysis, reporting, and decision-making.

## Initialization and Logging:
Upon instantiation, the class establishes a logger to facilitate effective tracking and communication of the transformation process. The logger is configured to provide informative messages, making it easier to monitor the execution of data transformation steps.

## Data Transformation Method (`transform_data`):
The `transform_data` method is the cornerstone of the class, responsible for performing the transformation of input data. This method is versatile and can handle input data in various formats, including Pandas DataFrames, lists, and dictionaries. The transformation is carried out in a series of well-defined steps, ensuring data integrity and quality.

1. **Input Data Validation**: The method starts by validating the input data. If the input data is in JSON format, it is converted into a Pandas DataFrame. Any inconsistencies in data format are handled gracefully with error messages.

2. **Column Validation**: The method checks whether the required columns for transformation are present in the input DataFrame. If any columns are missing, it raises a ValueError with details of the missing columns.

3. **IDE Value Removal**: The method systematically identifies and removes any "IDE" values from the dataset. The count and percentage of "IDE" values are calculated and logged. The removal is achieved by replacing "IDE" with NaN, ensuring cleaner and more accurate data.

4. **Data Transformation Operations**: Several transformation operations are performed on the input data to derive meaningful insights. These operations include:

* Conversion of the "date" column to Pandas datetime format.
* Calculation of "worker" and "resident" values based on worker and resident population percentages and total volume.
* Calculation of "visitor" values as the remaining count after deducting worker and resident counts.
* Categorization of "time_indicator" and "day" columns.
* Renaming and type conversion of columns for consistency and clarity.

5. **Specific Column Selection**: After data transformation, specific columns relevant to our analysis are selected. These columns include hex IDs, count dates, day indicators, time indicators, worker, resident, visitor counts, loyalty percentages, and dwell times.

6. **Time Indicator Mapping**: Time indicator values are mapped to a standardized format, enhancing uniformity and simplifying analysis.

7. **Logging and Return**: The method logs the completion of the data transformation process and returns the transformed data in the form of a Pandas DataFrame.

## Specific Transformation Methods:
The `HexTransform` class also provides specific transformation methods for different data categories, namely highstreets, town centres, bids, and bespoke areas. These methods share a common structure, utilizing lookup tables and reference data to enhance the transformed output.

Each specific transformation method performs the following steps:

1. **Lookup Table Integration**: The method integrates reference data lookup tables specific to the data category, such as Highstreets_quad_lookup, hex_highstreet_lookup, etc.

2. **Merging and Melting**: The transformed data is merged with the relevant lookup data based on hex IDs or other identifiers. The data is then melted, reshaping it from wide to long format, enabling more efficient aggregation.

3. **Aggregation and Reshaping**: Aggregation involves summarizing the data by grouping it based on specific attributes or identifiers. For example, when dealing with highstreets, the data may be grouped by highstreet ID, date, and time indicator. This grouping facilitates calculations such as summing up the total volume, averaging loyalty percentages, and calculating average dwell times for each unique combination of attributes.

4. **Pivot Table Transformation**: The aggregated data is transformed into a pivot table, reshaping it back to a wide format for better readability and usability.

5. **Column Type Specification**: Column data types are specified for consistency and efficiency. This includes converting columns to appropriate data types like integers, categories, and strings.

