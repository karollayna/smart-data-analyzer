import snowflake.connector
import pandas as pd
from . import secrets
import time

class SnowflakeHandler:

    def __init__(self):
        """
        Initializes the SnowflakeHandler with credentials from secrets
        and establishes a persistent connection.
        """
        self.account = secrets["snowflake_account"]
        self.user = secrets["snowflake_user"]
        self.password = secrets["snowflake_password"]
        self.warehouse = secrets["snowflake_warehouse"]
        self.database = secrets["snowflake_database"]
        self.schema = secrets["snowflake_schema"]

        self.conn = snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema,
        )

    def truncate_staging_tables(self):
        """
        Truncates all staging tables before ingestion.
        This clears previous data to ensure a clean load process.
        """
        queries = [
            "TRUNCATE TABLE stg_cell_lines",
            "TRUNCATE TABLE stg_drugs",
            "TRUNCATE TABLE stg_results",
        ]
        with self.conn.cursor() as cur:
            for query in queries:
                cur.execute(query)

    def call_procedure(self, procedure_name):
        """
        Calls a stored procedure in Snowflake.

        Parameters:
        ----------
        procedure_name : str
            The name of the procedure to call (e.g., 'merge_into_dim_drugs()').
        """
        with self.conn.cursor() as cur:
            cur.execute(f"CALL {procedure_name}")

    
    def refresh_snowpipe(self, pipe_name):
        """
        Refreshes a specified Snowpipe to manually trigger file ingestion.

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
            with self.conn.cursor() as cur:
                cur.execute(f"ALTER PIPE {pipe_name} REFRESH;")
            time.sleep(10)
            return True
        except Exception as e:
            print(f"Error during communication with Snowflake: {e}")
            return False
        
    def reset_pipeline(self):
        """
        Truncates staging tables and refreshes all Snowpipes.
        Useful to prepare the environment for a fresh ETL run.
        """
        self.truncate_staging_tables()
        pipes = ["update_stg_cell_lines", "update_stg_drugs", "update_stg_results"]
        for pipe in pipes:
            success = self.refresh_snowpipe(pipe)
            if not success:
                print(f"Failed to refresh pipe: {pipe}")
        
    def fetch_data(self, table_name):
        """
        Fetches all data from a specified table in Snowflake.

        Parameters:
        ----------
        table_name : str
            The name of the table to fetch data from.

        Returns:
        -------
        tuple
            A tuple containing the list of column names and a DataFrame with the table's content.
        """      
        try:
            with self.conn.cursor() as cur:
                cur.execute(f"SELECT * FROM {table_name};")
                rows = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
                return columns, pd.DataFrame(rows, columns=columns)
        except Exception as e:
            print(f"Error fetching data from {table_name}: {e}")
            return None

    def fetch_full_data(self, view_name, user_id):
        """
        Fetches user-specific distinct data from a Snowflake view.

        Parameters:
        ----------
        view_name : str
            The name of the view to query.
        user_id : str or int
            The user ID to filter data by.

        Returns:
        -------
        pd.DataFrame or None
            A DataFrame with the filtered data, or None if the query fails.
        """
        try:
            with self.conn.cursor() as cur:
                query = f"SELECT DISTINCT * FROM {view_name} WHERE USER_ID = %s"
                cur.execute(query, (user_id,))
                result = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
                return pd.DataFrame(result, columns=columns)
        except Exception as e:
            print(f"Error fetching data from {view_name}: {e}")
            return None
        
    def close_connection(self):
        """
        Closes the Snowflake connection.
        Should be called when done using the handler.
        """
        if self.conn:
            self.conn.close()