import snowflake.connector
import pandas as pd
from . import secrets
import time

class SnowflakeHandler:

    def __init__(self):
        """
        Initializes the SnowflakeHandler with credentials from secrets.
        """
        self.account = secrets["snowflake_account"]
        self.user = secrets["snowflake_user"]
        self.password = secrets["snowflake_password"]
        self.warehouse = secrets["snowflake_warehouse"]
        self.database = secrets["snowflake_database"]
        self.schema = secrets["snowflake_schema"]

    def connect_with_snowflake(self):
        """
        Establishes a connection to Snowflake using the provided credentials.

        Returns:
        -------
        connection : snowflake.connector.connection
            The established Snowflake connection.
        """
        return snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema,
        )
    
    def refresh_snowpipe(self, pipe_name):
        """
        Refreshes a specified Snowpipe.

        Parameters:
        ----------
        pipe_name : str
            The name of the Snowpipe to refresh.

        Returns:
        -------
        bool
            True if the refresh operation was successful, False otherwise.
        """
        try:
            with self.connect_with_snowflake() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"ALTER PIPE {pipe_name} REFRESH;")
            time.sleep(10)
            return True
        except Exception as e:
            print(f"Error during communication with snowflake: {e}")
            return False
        
    def fetch_data(self, table_name):
        """
        Fetches all data from a specified table in Snowflake.

        Parameters:
        ----------
        table_name : str
            The name of the table to fetch data from.

        Returns:
        -------
        tuple or None
            A tuple containing the column names and a pandas DataFrame with the data, or None if an error occurs.
        """
        try:
            with self.connect_with_snowflake() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT * FROM {table_name};")
                    rows = cur.fetchall()
                    columns = [desc[0] for desc in cur.description]
                    return columns, pd.DataFrame(rows, columns=columns)
        except Exception as e:
            print(f"Error fetching data from {table_name}: {e}")
            return None

    def fetch_full_data(self, view_name, user_id):
        """
        Fetches distinct data from a specified view in Snowflake for a given user ID.

        Parameters:
        ----------
        view_name : str
            The name of the view to fetch data from.
        user_id : str or int
            The ID of the user to filter data by.

        Returns:
        -------
        pandas.DataFrame or None
            A pandas DataFrame with the fetched data, or None if an error occurs.
        """
        try:
            with self.connect_with_snowflake() as conn:
                with conn.cursor() as cur:
                    query = f"SELECT DISTINCT * FROM {view_name} WHERE USER_ID = %s"
                    cur.execute(query, (user_id,))
                    result = cur.fetchall()
                    columns = [desc[0] for desc in cur.description]
                    return pd.DataFrame(result, columns=columns)
        except Exception as e:
            print(f"Error fetching data from {view_name}: {e}")
            return None