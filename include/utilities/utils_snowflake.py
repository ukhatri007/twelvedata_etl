import os
import snowflake.connector.pandas_tools
from datetime import datetime, timezone
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import logging


from dataclasses import dataclass
# from dotenv import load_dotenv

# load_dotenv()


@dataclass
class SnowflakeConnection:
   
    def snowflake_conn(self):
        hook = SnowflakeHook(snowflake_conn_id="destination_conn")
        self.conn = hook.get_conn()
        return self.conn


@dataclass
class SnowflakeDestination:

    def __post_init__(self):
       self.conn = SnowflakeConnection().snowflake_conn()

    def wrirte_dataframe(self ,df, details:dict):
        df.columns = df.columns.str.upper()  

        try:
            success, nchunks, nrows, _ = snowflake.connector.pandas_tools.write_pandas(
                self.conn,
                df,
                table_name=details["table_name"],
                database=details["database"],
                schema=details["schema"],
                auto_create_table=True,
                overwrite=False,
                quote_identifiers=False
            )
            logging.info(
                f"Data loaded into Snowflake table: {details["table_name"]}\n"
                f"Success: {success}\nNumber of rows: {nrows}\nNumber of chunks: {nchunks}"
            )

        except Exception as e:
            logging.info(f"Failed to load data into {details['table_name']}: {e}")
            raise


    def load_into_snowflake(self, df, details):
        table_exists_response = SnowflakeOperation().check_if_table_exists(details=details)
        
        if not table_exists_response:
            load = self.wrirte_dataframe(df, details=details)
            return load
        
        timestamp = int(datetime.now(timezone.utc).timestamp())
        timestamp_suffix = str(timestamp)
        temp_table = f"{details['table_name']}_{timestamp_suffix}".upper()
        temp_details = {
            "table_name": temp_table,
            "database": details["database"].upper(),
            "schema": details["schema"].upper()
        }
       
        load = self.wrirte_dataframe(df, details=temp_details)
        schema_details = {
            "dest_table": details["table_name"].upper(),
            "database": details["database"].upper(),
            "schema": details["schema"].upper(),
            "source_table": temp_table
        }

        handle_scheam = SchemaDriftHandler()
        handle_scheam.handle_schema_drift(details=schema_details)
        load = self.wrirte_dataframe(df, details=details)
        SnowflakeOperation().delete_table(details=schema_details)
    

@dataclass
class SnowflakeOperation:
    def __post_init__(self):
      self.conn = SnowflakeConnection().snowflake_conn()
        

    def query_df(self, query_string: str):
        cursor = self.conn.cursor()
        cursor.execute(query_string)
        return cursor.fetch_pandas_all()

    def delete_table(self, details):
        database = details["database"]
        schema = details["schema"]
        table_name = details["source_table"]

        cursor = self.conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {database}.{schema}.{table_name}")
        logging.info(f"Table {database}.{schema}.{table_name} deleted successfully.")

    def check_if_table_exists(self, details) -> bool:
        table_name = details["table_name"].upper()
        database = details["database"].upper()
        schema = details["schema"].upper()

        cursor = self.conn.cursor()
        query = f"""
            SELECT 1
            FROM {database}.INFORMATION_SCHEMA.TABLES
            WHERE table_name = '{table_name}'
            AND table_schema = '{schema}'
        """
        cursor.execute(query)
        result = cursor.fetchone()
        return result is not None
        

    def add_column(self, details):
        database = details["database"]
        schema = details["schema"]
        table_name = details["table_name"]
        column_name = details["column_name"]
        data_type = details["data_type"]

        cursor = self.conn.cursor()
        cursor.execute(
            f"ALTER TABLE {database}.{schema}.{table_name} ADD COLUMN {column_name} {data_type}"
        )
        logging.info(f"column {column_name} added successfully.")
        return True

    

@dataclass
class SchemaDriftHandler:

    def __post_init__(self):
        self.conn = SnowflakeConnection().snowflake_conn()

    def column_name_data_type(self, details) -> list:
        table_name = details["table_name"]
        database = details["database"]
        schema = details["schema"]
        cursor = self.conn.cursor()
        cursor.execute(f"""
            SELECT column_name, data_type
            FROM {database}.INFORMATION_SCHEMA.COLUMNS 
            WHERE table_name = '{table_name}'
              AND table_schema = '{schema}'
            ORDER BY column_name DESC
        """)
        df = cursor.fetch_pandas_all()
        df.columns = ['column_name', 'data_type']
        columns_list = df.to_dict(orient='records')
        return columns_list

    def check_schema_drift(self, details: dict) -> list:
        source_details={
            "table_name" : details["source_table"].upper(),
            "database" : details["database"].upper(),
            "schema": details["schema"].upper()

        }
        dest_details={
            "table_name" : details["dest_table"].upper(),
            "database" : details["database"].upper(),
            "schema": details["schema"].upper()
        }
        
        dest_column = self.column_name_data_type(details=dest_details)
        logging.info(f"destination column names: {dest_column}")
        source_column = self.column_name_data_type(details= source_details)
        logging.info(f"source column names: {source_column}")

        dest_columns = [column['column_name'] for column in dest_column]

        missing_column = []
        for column in source_column:
            if column['column_name'] not in dest_columns:
                missing_column.append(column)
                
        logging.info(f"missing_column: {missing_column}")
        return missing_column

    def handle_schema_drift(self, details: dict):
        dest_name = details["dest_table"]
        snowflake_op = SnowflakeOperation()
        missing_columns = self.check_schema_drift(details)

        if missing_columns:
            for column in missing_columns:
                add_col_details = {
                    "database": details["database"],
                    "schema": details["schema"],
                    "table_name": dest_name,
                    "column_name": column["column_name"],
                    "data_type": column["data_type"],
                }
                snowflake_op.add_column(add_col_details)