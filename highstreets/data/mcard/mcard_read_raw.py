import os
import pandas as pd
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def create_connection(host, port, dbname, user, password):
    """
    Creates a connection to a PostgreSQL database with the specified parameters.

    Args:
        host (str): The hostname or IP address of the PostgreSQL server.
        port (str): The port number to use for the connection.
        dbname (str): The name of the database to connect to.
        user (str): The username to use for the connection.
        password (str): The password to use for the connection.

    Returns:
        A psycopg2 connection object to the specified PostgreSQL database.
    """
    conn = psycopg2.connect(host=host,
                            port=port,
                            dbname=dbname,
                            user=user,
                            password=password)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    return conn


def table_exists(conn, table_name):
    """
    Checks if a table exists in the specified PostgreSQL database connection.

    Args:
        conn (psycopg2.extensions.connection): The psycopg2 connection object.
        table_name (str): The name of the table to check for existence.

    Returns:
        A boolean indicating whether the table exists in the database.
    """
    cur = conn.cursor()
    cur.execute("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = %s)", (table_name,))
    result = cur.fetchone()[0]
    cur.close()
    return result


def drop_table(conn, table_name):
    """
    Drops a table in the specified PostgreSQL database connection.

    Args:
        conn (psycopg2.extensions.connection): The psycopg2 connection object.
        table_name (str): The name of the table to drop.

    Returns:
        None
    """
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.commit()
    cur.close()


def append_to_table(conn, table_name, data):
    """Appends a pandas DataFrame to a PostgreSQL table.

    Args:
        conn (psycopg2.extensions.connection): A PostgreSQL database connection.
        table_name (str): The name of the PostgreSQL table to append the DataFrame to.
        data (pandas.DataFrame): The DataFrame containing the data to append
        to the table.

    Returns:
        None

    Raises:
        None
    """
    cur = conn.cursor()

    # Get the max date in the existing data
    # cur.execute(f"SELECT MAX(date_column) FROM {table_name}")
    # max_date = cur.fetchone()[0]

    # Filter the new data to only include rows with dates greater than the max date in the table
    # data_filtered = data[data['date_column'] > max_date]

    # If there is no new data to append, return
    # if len(data_filtered) == 0:
    #    return

    columns = ", ".join(list(data.columns))
    values = "%s, " * len(data.columns)
    values = values[:-2]
    query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
    # data_tuples = [tuple(x) for x in data_filtered.to_records(index=False)]
    # psycopg2.extras.execute_values(cur, query, data_tuples, page_size=len(data_filtered))
    data_tuples = [tuple(x) for x in data.to_records(index=False)]
    psycopg2.extras.execute_values(cur, query, data_tuples, page_size=len(data))
    conn.commit()
    cur.close()


def process_file(file, conn):
    """
    Processes a given Mastercard file and stores the data in a PostgreSQL database table.

    Args:
        file (str): The name of the Mastercard file to be processed.
        conn (psycopg2.extensions.connection): A PostgreSQL database connection object.

    Returns:
        None
    """
    zoom_level = int(file.split("_")[4].replace("zoom", ""))
    day_end = file.split("_")[5]
    data = pd.read_csv(os.path.join("data", "mastercard", "sharefile", file), sep="|", dtype={
        "yr": float,
        "wk": float,
        "industry": str,
        "segment": str,
        "geo_type": str,
        "geo_name": str,
        "quad_id": str,
        "central_latitude": float,
        "central_longitude": float,
        "bounding_box": str,
        "txn_amt": float,
        "txn_cnt": float,
        "acct_cnt": float,
        "avg_ticket": float,
        "avg_freq": float,
        "avg_spend_amt": float,
        "yoy_txn_amt": str,
        "yoy_txn_cnt": str
    })

    data["weekday_weekend"] = day_end
    data["file_name"] = file
    table_name = f"econ_busyness_mcard_raw_{zoom_level}_zoom"
    if table_exists(conn, table_name):
        drop_table(conn, table_name)

    append_to_table(conn, table_name, data)


def main():
    host = os.environ["PG11_HOST"]
    dbname = os.environ["PG11_DATABASE"]
    user = "gisapdata"
    password = os.environ["PG11_PASSWORD"]
    port = os.environ["PG11_PORT"]

    conn = create_connection(host, port, dbname, user, password)

    for zoom_level in [15, 18]:
        table_name = f"econ_busyness_mcard_raw_{zoom_level}_zoom"
        if table_exists(conn, table_name):
            drop_table(conn, table_name)

    files = os.listdir(os.path.join("data", "mastercard", "sharefile"))
    for file in files:
        if file.endswith(".csv"):
            process_file(file, conn)

    conn.close()


if __name__ == "__main__":
    main()
