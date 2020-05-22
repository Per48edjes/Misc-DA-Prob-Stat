import os
import snowflake.connector
import pandas as pd


# Wrapper for snowflake.connector.connect
def get_connection():
    """
    Returns a connection to Snowflake provided you have credentials
    in shell config
    """
    account = "flexport"
    user = os.getenv("SNOWFLAKE_USERNAME")
    authenticator = "externalbrowser"
    conn = snowflake.connector.connect(
        user=user,
        account=account,
        warehouse="REPORTING_WH",
        role="DATA_ANALYTICS_ROLE",
        authenticator=authenticator,
    )
    cur = conn.cursor()
    return conn, cur


def validate_connection(conn, cur):
    """
    Function that validates connection by connecting and printing
    Snowflake version
    """
    try:
        cur.execute("SELECT current_version()")
        one_row = cur.fetchone()
        print(one_row[0])
    finally:
        cur.close()
    conn.close()


def get_data(input_source, input_type, conn):
    """
    Function to get data from a .sql file or SQL query string input

    Parameters
    ---
    input_source
        Either a string with a valid SQL query or a .sql file

    input_type
        'file' if input is a .sql file; 'text' if input is  a string

    conn
        Connector object returned by get_connection() function
    """

    if input_type == 'text':
        # Use the read_sql method to get the data from Snowflake into a Pandas dataframe
        df = pd.read_sql(input_source, conn)

    if input_type == 'file':
        # Open the input_source file
        with open(input_source, 'r') as q:

            # Save contents of input_source as string
            query_str = q.read()

            # Use the read_sql method to get the data from Snowflake into a Pandas dataframe
            df = pd.read_sql(query_str, conn)

    # Make all the columns lowercase
    df.columns = map(str.lower, df.columns)

    return df


if __name__ == "__main__":
    conn, cur = get_connection()
    validate_connection(conn, cur)
