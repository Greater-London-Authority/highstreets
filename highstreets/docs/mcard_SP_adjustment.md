# User Guide for Mastercard Spending Pulse adjustment factor

Author: Rachel Humphries

# Pipeline for Mastercard Weekly Processing

1. Load raw data into econ_busyness_mcard_raw_zoom_18

2. Transform data into econ_busyness_mcard_clean_zoom_18

3. Process data into econ_busyness_mcard_stg_zoom_18

4. Generate aggregated Inner-Outer London table (test_econ_busyness_mcard_inner_outer_txn) from stg table

5. Create adjustment factor (The output from this is then also used to adjust 3-hourly txn_amt) -> Save to Postgres

## Current steps:

6. Aggregate txn_amt, txn_cnt to all areas for DataStore CSVs

7. Aggregated spend data is then adjusted

## Alternative steps:

6. Adjust clean table at quad-level -> add new txn_amt_adj column (New table needs to be created and full backseries adjusted)

7. Aggregate txn_amt, txn_cnt, txn_amt_adj to all areas for DataStore CSVs




If you have any questions or need further assistance, please reach out to Anupam Bose - anupam.bose@london.gov.uk, or Rachel Humphries - rachel.humphries@london.gov.uk.
