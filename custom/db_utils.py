import os
import snowflake.connector


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


if __name__ == "__main__":
    conn, cur = get_connection()
    validate_connection(conn, cur)
