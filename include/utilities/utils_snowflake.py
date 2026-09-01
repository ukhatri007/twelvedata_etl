import os
import snowflake.connector.pandas_tools

from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass
class SnowflakeConnection:
    database: str
    schema: str
    user: str = os.getenv("SNOWFLAKE_USER")
    password: str = os.getenv("SNOWFLAKE_PASSWORD")
    account: str = os.getenv("SNOWFLAKE_ACCOUNT")
    warehouse: str = os.getenv("SNOWFLAKE_WAREHOUSE")

    def create_conn(self):
        self.conn = snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema,
        )
        return self.conn


@dataclass
class SnowflakeDestination:
    database: str
    schema: str
    table_name: str
    user: str = os.getenv("SNOWFLAKE_USER")
    password: str = os.getenv("SNOWFLAKE_PASSWORD")
    account: str = os.getenv("SNOWFLAKE_ACCOUNT")
    warehouse: str = os.getenv("SNOWFLAKE_WAREHOUSE")

    def __post_init__(self):
        snowflake_conn = SnowflakeConnection(
            database=self.database,
            schema=self.schema,
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
        )
        self.conn = snowflake_conn.create_conn()

    def load_into_snowflake(self, df):
        success, nchunks, nrows, _ = snowflake.connector.pandas_tools.write_pandas(
            self.conn,
            df,
            table_name=self.table_name,
            database=self.database,
            schema=self.schema,
            auto_create_table=True,
            overwrite=False
        )
        print(
            f"Data loaded into Snowflake table: {self.table_name}\nSuccess: {success}\nNumber of rows: {nrows}\nNumber of chunks: {nchunks}"
        )


@dataclass
class SnowflakeOperation:
    database: str
    schema: str
    user: str = os.getenv("SNOWFLAKE_USER")
    password: str = os.getenv("SNOWFLAKE_PASSWORD")
    account: str = os.getenv("SNOWFLAKE_ACCOUNT")
    warehouse: str = os.getenv("SNOWFLAKE_WAREHOUSE")

    def __post_init__(self):
        snowflake_conn = SnowflakeConnection(
            database=self.database,
            schema=self.schema,
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
        )
        self.conn = snowflake_conn.create_conn()

    def query_df(self, query_string: str):
        cursor = self.conn.cursor()
        cursor.execute(query_string)
        return cursor.fetch_pandas_all()


@dataclass
class SchemaDriftHandler:
        database: str
        schema: str
        table_name: str
        user: str = os.getenv("SNOWFLAKE_USER")
        password: str = os.getenv("SNOWFLAKE_PASSWORD")
        account: str = os.getenv("SNOWFLAKE_ACCOUNT")
        warehouse: str = os.getenv("SNOWFLAKE_WAREHOUSE")
    
        def __post_init__(self):
            snowflake_conn = SnowflakeConnection(
                database=self.database,
                schema=self.schema,
                user=self.user,
                password=self.password,
                account=self.account,
                warehouse=self.warehouse,
            )
            self.conn = snowflake_conn.create_conn()
    