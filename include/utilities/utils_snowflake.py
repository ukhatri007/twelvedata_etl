import pandas as pd
import logging
from  snowflake.connector.pandas_tools  import write_pandas
import re

from contextlib import closing
from datetime import datetime, timezone
from typing import Any, Iterable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

logger = logging.getLogger(__name__)

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")

_DTYPE_TO_SNOWFLAKE = {
    "int64": "NUMBER(38,0)",
    "int32": "NUMBER(38,0)",
    "float64": "FLOAT",
    "float32": "FLOAT",
    "bool": "BOOLEAN",
    "datetime64[ns]": "TIMESTAMP_NTZ",
    "datetime64[ns, UTC]": "TIMESTAMP_TZ",
    "object": "VARCHAR",
    "string": "VARCHAR",
}

def safe_identifier(name: str) -> str:
    """Validate and normalize a SQL identifier (db/schema/table/column name).
 
    Snowflake identifiers can't be bound as query parameters, so unlike
    literal values they must be whitelisted before interpolation.
    """
    candidate = str(name).strip().upper()
    if not _IDENTIFIER_RE.match(candidate):
        raise ValueError(f"Unsafe or invalid SQL identifier: {name!r}")
    return candidate

def snowflake_type_for(dtype: Any) -> str:
    return _DTYPE_TO_SNOWFLAKE.get(str(dtype), "VARCHAR")

 
class SnowflakeConnection:
    """Context manager that owns exactly one underlying connection."""
 
    def __init__(self, conn_id: str = "destination_conn"):
        self.conn_id = conn_id
        self.conn = None
 
    def __enter__(self):
        hook = SnowflakeHook(snowflake_conn_id=self.conn_id)
        self.conn = hook.get_conn()
        return self.conn
 
    def __exit__(self, exc_type, exc, tb):
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                logger.warning("Failed to cleanly close Snowflake connection", exc_info=True)
        return False  # never suppress exceptions
 

class SnowflakeOperation:

    """Low-level DDL/introspection helpers. Takes a connection; opens none of its own."""
 
    def __init__(self, conn):
        self.conn = conn
 
    def query_df(self, query_string: str) -> pd.DataFrame:
        with closing(self.conn.cursor()) as cursor:
            cursor.execute(query_string)
            return cursor.fetch_pandas_all()
 
    def delete_table(self, details: dict) -> None:
        database = safe_identifier(details["database"])
        schema = safe_identifier(details["schema"])
        table_name = safe_identifier(details["source_table"])
 
        with closing(self.conn.cursor()) as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {database}.{schema}.{table_name}")
        logger.info("Table %s.%s.%s dropped.", database, schema, table_name)
 
    def check_if_table_exists(self, details: dict) -> bool:
        database = safe_identifier(details["database"])
        table_name = details["table_name"].upper()
        schema = details["schema"].upper()
 
        query = f"""
            SELECT 1
            FROM {database}.INFORMATION_SCHEMA.TABLES
            WHERE table_name = %s AND table_schema = %s
        """
        with closing(self.conn.cursor()) as cursor:
            cursor.execute(query, (table_name, schema))
            return cursor.fetchone() is not None
 
    def get_table_columns(self, database: str, schema: str, table_name: str) -> set[str]:
        database = safe_identifier(database)
        query = f"""
            SELECT column_name
            FROM {database}.INFORMATION_SCHEMA.COLUMNS
            WHERE table_name = %s AND table_schema = %s
        """
        with closing(self.conn.cursor()) as cursor:
            cursor.execute(query, (table_name.upper(), schema.upper()))
            return {row[0] for row in cursor.fetchall()}
 
    def add_column(self, details: dict) -> None:
        database = safe_identifier(details["database"])
        schema = safe_identifier(details["schema"])
        table_name = safe_identifier(details["table_name"])
        column_name = safe_identifier(details["column_name"])
        data_type = details["data_type"]  # not a free-form identifier; comes from our own type map
 
        with closing(self.conn.cursor()) as cursor:
            cursor.execute(
                f"ALTER TABLE {database}.{schema}.{table_name} ADD COLUMN {column_name} {data_type}"
            )
        logger.info("Column %s (%s) added to %s.%s.%s", column_name, data_type, database, schema, table_name)



class SchemaDriftHandler:
    """Detects and reconciles schema drift directly from a DataFrame's dtypes.
 
    Unlike the original approach, this never needs to write the data to a
    temporary Snowflake table just to introspect its schema.
    """
 
    def __init__(self, conn):
        self.conn = conn
        self.op = SnowflakeOperation(conn)
 
    def missing_columns(self, df: pd.DataFrame, details: dict) -> Iterable[str]:
        existing = self.op.get_table_columns(details["database"], details["schema"], details["table_name"])
        return [src_column for src_column in df.columns if src_column.upper() not in existing]
 
    def sync_missing_columns(self, df: pd.DataFrame, details: dict) -> None:
        database = safe_identifier(details["database"])
        schema = safe_identifier(details["schema"])
        table_name = safe_identifier(details["table_name"])
 
        missing = list(self.missing_columns(df, details))
        if not missing:
            return
 
        for column in missing:
            self.op.add_column({
                "database": database,
                "schema": schema,
                "table_name": table_name,
                "column_name": column,
                "data_type": snowflake_type_for(df[column].dtype),
            })
        logger.info("Synced %d missing column(s) into %s.%s.%s: %s",
                    len(missing), database, schema, table_name, missing)
 


class SnowflakeDestination:
    """High-level load operations. Takes a connection; opens none of its own."""
 
    def __init__(self, conn):
        self.conn = conn
        self.op = SnowflakeOperation(conn)
        self.drift = SchemaDriftHandler(conn)
 
    def write_dataframe(self, df: pd.DataFrame, details: dict) -> None:
        database = safe_identifier(details["database"])
        schema = safe_identifier(details["schema"])
        table_name = safe_identifier(details["table_name"])
 
        df = df.copy()
        df.columns = df.columns.str.upper()
 
        try:
            success, nchunks, nrows, _ = write_pandas(
                self.conn,
                df,
                table_name=table_name,
                database=database,
                schema=schema,
                auto_create_table=True,
                overwrite=False,
                quote_identifiers=False,
            )
            logger.info(
                "Loaded into %s.%s.%s | success=%s rows=%d chunks=%d",
                database, schema, table_name, success, nrows, nchunks,
            )
        except Exception:
            logger.error("Failed to load data into %s.%s.%s", database, schema, table_name, exc_info=True)
            raise
 
    def load_into_snowflake(self, df: pd.DataFrame, details: dict) -> None:
        """Append `df` to the destination table, evolving its schema first if needed."""
        if self.op.check_if_table_exists(details):
            self.drift.sync_missing_columns(df, details)
        self.write_dataframe(df, details=details)
 
    def merge_into_snowflake(self, df: pd.DataFrame, details: dict) -> None:
        """Upsert `df` into `details['table_name']`, keyed on `details['merge_keys']`.
 
        Stages `df` in a temporary table (required as the MERGE source),
        reconciles schema drift, runs the MERGE, then always drops the
        staging table.
        """
        merge_keys = details.get("merge_keys")
        if not merge_keys:
            raise ValueError("merge_into_snowflake requires details['merge_keys'] (list of column names)")
 
        database = safe_identifier(details["database"])
        schema = safe_identifier(details["schema"])
        dest_table = safe_identifier(details["table_name"])
 
        staging_table = safe_identifier(f"{dest_table}_STAGE_{int(datetime.now(timezone.utc).timestamp())}")
        staging_details = {"table_name": staging_table, "database": database, "schema": schema}
 
        try:
            self.write_dataframe(df, details=staging_details)
 
            if self.op.check_if_table_exists(details):
                self.drift.sync_missing_columns(df, details)
            else:
                # No destination yet: create it from the staging table's shape.
                self.write_dataframe(df.iloc[0:0], details=details)
 
            key_cols = [safe_identifier(k) for k in merge_keys]
            all_cols = [safe_identifier(c) for c in df.columns]
            update_cols = [c for c in all_cols if c not in key_cols]
 
            on_clause = " AND ".join(f"dest.{k} = src.{k}" for k in key_cols)
            set_clause = ", ".join(f"dest.{c} = src.{c}" for c in update_cols)
            insert_cols = ", ".join(all_cols)
            insert_vals = ", ".join(f"src.{c}" for c in all_cols)
 
            merge_sql = f"""
                MERGE INTO {database}.{schema}.{dest_table} AS dest
                USING {database}.{schema}.{staging_table} AS src
                ON {on_clause}
                WHEN MATCHED THEN UPDATE SET {set_clause}
                WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
            """
            with closing(self.conn.cursor()) as cursor:
                cursor.execute(merge_sql)
            logger.info("Merged %d row(s) into %s.%s.%s", len(df), database, schema, dest_table)
        finally:
            self.op.delete_table({"database": database, "schema": schema, "source_table": staging_table})
 
 
