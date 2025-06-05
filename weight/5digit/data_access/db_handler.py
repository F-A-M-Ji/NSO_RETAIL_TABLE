import pyodbc
import pandas as pd
from config.app_config import DB_CONFIG  # แก้ไข import path


def get_sql_server_connection():
    """Establishes a connection to the SQL Server database."""
    try:
        conn_str = (
            f"DRIVER={DB_CONFIG['driver']};"
            f"SERVER={DB_CONFIG['server']},{DB_CONFIG['port']};"
            f"DATABASE={DB_CONFIG['database']};"
            f"UID={DB_CONFIG['username']};"
            f"PWD={DB_CONFIG['password']};"
            "TrustServerCertificate=yes;"  # อาจจำเป็นสำหรับ self-signed certificates
        )
        conn = pyodbc.connect(conn_str)
        return conn
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        return None


def fetch_data(conn, query, params=None):
    """Fetches data from SQL Server and returns a Pandas DataFrame."""
    try:
        if params:
            df = pd.read_sql(query, conn, params=params)
        else:
            df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        return pd.DataFrame()


def execute_query(conn, query, params=None):
    """Executes a single query (INSERT, UPDATE, DELETE) on SQL Server."""
    cursor = conn.cursor()
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        return False


def execute_many_query(conn, query, data_tuples):
    """Executes a query with multiple rows of data (executemany)."""
    if not data_tuples:
        return True  # Or False, depending on desired behavior for empty data

    cursor = conn.cursor()
    try:
        cursor.fast_executemany = True  # For pyodbc performance with SQL Server
        cursor.executemany(query, data_tuples)
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        # You might want to log data_tuples[0] to see an example of failing data
        return False


def df_to_sql_custom(conn, df, table_name):
    """
    Custom bulk insert for a DataFrame to a SQL Server table.
    Assumes table already exists.
    """
    if df.empty:
        return True

    cursor = conn.cursor()
    cursor.fast_executemany = True

    cols = ",".join([f"[{col}]" for col in df.columns])
    placeholders = ",".join(["?"] * len(df.columns))
    insert_sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"

    try:
        # Convert NaN to None for SQL compatibility, and ensure correct types
        data_tuples = [
            tuple(None if pd.isna(x) else x for x in row) for row in df.to_numpy()
        ]
        cursor.executemany(insert_sql, data_tuples)
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        return False
