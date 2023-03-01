# %%
import os

import pandas as pd
from sqlalchemy import URL, MetaData, Table, create_engine, func, inspect, select
from tqdm import tqdm

from highstreets import config
from highstreets.data import schema as bt_schema

# this is where received data should be stored
BT_INPUT_DIR = os.path.join(config.BT_DIR, "received")

# each month's data is stored in a separate folder
# and inside that folder is a 'files' folder, containing csv files with
# the actual data for that month
data_folders = [
    os.path.join(BT_INPUT_DIR, f, "files")
    for f in os.listdir(BT_INPUT_DIR)
    if os.path.isdir(os.path.join(BT_INPUT_DIR, f))
]


# make database url for connecting to Postgres
db_url = URL.create(
    "postgresql+psycopg2",
    username=config.PG11_USER,
    password=config.PG11_PASSWORD,
    host=config.PG11_HOST,
    port=config.PG11_PORT,
    database=config.PG11_DATABASE,
)

# make SQLAlchemy engine for connecting to Postgres
engine = create_engine(db_url)
metadata_obj = MetaData(engine)

# get expected file prefixes from config and assign a
# corresponding table in the database
db_prefixes_tables = {
    config.BT_LSOA_DAILY_PREFIX: (
        "bt_footfall_lsoa_daily",
        bt_schema.clean_lsoa_daily,
    ),
    config.BT_MSOA_DAILY_PREFIX: (
        "bt_footfall_msoa_daily",
        bt_schema.clean_msoa_daily,
    ),
    config.BT_LSOA_MONTHLY_PREFIX: (
        "bt_footfall_lsoa_monthly",
        bt_schema.clean_lsoa_monthly,
    ),
    config.BT_MSOA_MONTHLY_PREFIX: (
        "bt_footfall_msoa_monthly",
        bt_schema.clean_msoa_monthly,
    ),
    config.BT_TFL_HEX_DAILY_PREFIX: (
        "bt_footfall_tfl_hex_daily",
        bt_schema.clean_hex_daily,
    ),
    config.BT_TFL_HEX_MONTHLY_PREFIX: (
        "bt_footfall_tfl_hex_monthly",
        bt_schema.clean_hex_monthly,
    ),
}

# for each file prefix check if the corresponding table exists
# and store the results in a dictionary
db_tables_exist = {
    table: inspect(engine).has_table(table)
    for _, (table, _) in db_prefixes_tables.items()
}

print(db_tables_exist)

# %%

# loop over the folders for each month
# each file for each month is processed in turn
# processing involves validating the file's data against a schema for that file type
# and then adding the data to the database if it has not already been added
for dir in data_folders:
    # extract date from folder name
    date = pd.to_datetime(
        dir.split("/")[-2][-10:].replace("_", "/"),
        dayfirst=True,
    )

    # list the files in the directory
    files = [f for f in os.listdir(dir) if os.path.isfile(os.path.join(dir, f))]
    n_files = len(files)

    # looping over the files, check if the file prefix matches
    # a prefix in the config and if the date of the file is
    # greater than the date of the most recent record in the
    # corresponding table. If so, add the file's data to the database,
    # skipping otherwise
    for file_no, file in tqdm(enumerate(files)):
        # display progress
        print(f"Processing {file}, file {file_no+1} of {n_files}...")

        # loop over the file prefixes looking for a match
        # if a match is found, check if this file has already been added
        # if only some rows have been added, drop those rows and re-add the file
        # if the file has not been added, add it
        # if the file has already been added, skip it
        # if the file prefix does not match any of the prefixes in the config, skip it
        matching_table_found = False
        for prefix, (table, clean_func) in db_prefixes_tables.items():
            if file.startswith(prefix):
                matching_table_found = True

                # get the number of records in the table with the current
                # file in the file_name column
                if db_tables_exist[table]:
                    table_obj = Table(table, metadata_obj, autoload_with=engine)
                    query = (
                        select(func.count())
                        .select_from(table_obj)
                        .where(table.c.file_name == file)
                    )
                    n_records = pd.read_sql_query(
                        sql=query,
                        con=engine.connect(),
                    ).iloc[0, 0]
                    # print the number of records
                    print(f"{n_records} records in {table} for {file}")
                else:
                    n_records = 0

                # read the file into a dataframe
                df = pd.read_csv(os.path.join(dir, file), low_memory=False)

                # clean data and validate the dataframe against the schema
                df = clean_func(df, date, file)

                if n_records == 0:
                    print(f"File {file} has not been entered into {table}")
                    print(f"Reading {file} into {table} \n")

                    # write the dataframe to the database
                    df.to_sql(
                        table,
                        engine,
                        if_exists="append",
                        index=False,
                    )
                elif n_records > 0 and n_records < df.shape[0]:
                    print(f"File {file} has been incompletely entered into {table}")
                    print(f"Deleting {file} from {table} and re-entering \n")

                    # create table object for the table
                    table_obj = Table(table, metadata_obj, autoload_with=engine)

                    # drop the records that have already been entered
                    table_obj.delete().where(table_obj.c.file_name == file).execute()

                    # write the dataframe to the database
                    df.to_sql(
                        table,
                        engine,
                        if_exists="append",
                        index=False,
                    )
                elif n_records == df.shape[0]:
                    print(f"File {file} has already been entered into {table}")
                    print(f"Skipping {file} \n")
                else:
                    print(f"Too many records in {table} for {file}")
                    print(f"Removing {file} from {table} and re-entering \n")

                    # create table object for the table
                    table_obj = Table(table, metadata_obj, autoload_with=engine)

                    # drop the records that have already been entered
                    table_obj.delete().where(table_obj.c.file_name == file).execute()

                    # write the dataframe to the database
                    df.to_sql(
                        table,
                        engine,
                        if_exists="append",
                        index=False,
                    )

        if not matching_table_found:
            print(f"File {file} does not match any known prefix")
            print(f"Skipping {file} \n")
