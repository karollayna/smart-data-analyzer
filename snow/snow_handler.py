import utils
import snowflake.connector
import time
import pandas as pd

def connect_with_snowflake():
    secrets = utils.load_secrets("secrets.yaml")
    snowflake_account = secrets["snowflake_account"]
    snowflake_user = secrets["snowflake_user"]
    snowflake_password = secrets["snowflake_password"]
    snowflake_warehouse = secrets["snowflake_warehouse"]
    snowflake_database = secrets["snowflake_database"]
    snowflake_schema = secrets["snowflake_schema"]

    conn = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        warehouse=snowflake_warehouse,
        database=snowflake_database,
        schema=snowflake_schema,
    )

    return conn

def refresh_snowpipe(pipe_name):
    conn = connect_with_snowflake()
    cur = conn.cursor()
    cur.execute(f"ALTER PIPE {pipe_name} REFRESH;")
    time.sleep(10)
    conn.close()
    return "PIPE refreshed!"

def fetch_data(table_name):
    conn = connect_with_snowflake()
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table_name};")
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()
    return columns, pd.DataFrame(rows, columns=columns)

def fetch_full_data(view_name, user_id):
    conn = connect_with_snowflake()
    cur = conn.cursor()
    query = f"SELECT DISTINCT * FROM {view_name} WHERE USER_ID = %s"
    cur.execute(query, (user_id,))
    result = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()
    df = pd.DataFrame(result, columns=columns)
    
    return df