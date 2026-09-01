import os
import time
import logging
from datetime import datetime, timedelta

import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

from airflow.sdk import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.standard.operators.python import PythonOperator

from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

API_KEY = os.getenv("API_KEY")
BASE = "https://api.twelvedata.com"
HEADERS = {"Authorization": f"apikey {API_KEY}"}

RATE_LIMIT = 7          # symbols per batch (matches TwelveData free-tier rate limit)
WINDOW_SECONDS = 60      # cooldown between batches
REQUEST_TIMEOUT = 30     # seconds, per HTTP request

SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "JPM", "V", "JNJ"]
STAGING_PATH = "/tmp/twelvedata_staging.parquet"


def extract(**kwargs):
    """
    Fetch daily time series for all symbols from TwelveData, respecting the
    API rate limit by batching requests, and stage the combined result to
    parquet. Pushes the staging file path (kept small for XCom) via ti.xcom_push.
    """
    ti = kwargs["ti"]

    def fetch_time_series(symbol: str):
        params = {
            "symbol": symbol,
            "interval": "1day",
            "order": "asc",
            "outputsize": 5000,
        }
        try:
            response = requests.get(
                f"{BASE}/time_series",
                headers=HEADERS,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
        except requests.RequestException as e:
            logging.error(f"[{symbol}] Request failed: {e}")
            return None

        data = response.json()

        # TwelveData returns HTTP 200 even for logical errors (bad symbol,
        # rate limit hit, etc.), so check the payload shape explicitly.
        if data.get("status") == "error":
            logging.error(f"[{symbol}] API error: {data.get('message')}")
            return None

        if "meta" not in data or "values" not in data:
            logging.error(f"[{symbol}] Unexpected response shape: {data}")
            return None

        meta = data["meta"]
        df = pd.DataFrame(data["values"])

        if df.empty:
            logging.warning(f"[{symbol}] No values returned")
            return None

        df = df.assign(
            symbol=meta["symbol"],
            interval=meta["interval"],
            currency=meta["currency"],
            exchange=meta["exchange"],
        )
        return df

    def chunk_list(items, size):
        return [items[i:i + size] for i in range(0, len(items), size)]

    chunks = chunk_list(SYMBOLS, RATE_LIMIT)
    result_data = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        for i, chunk in enumerate(chunks):
            logging.info(f"Processing chunk {i + 1}/{len(chunks)}: {chunk}")
            start = time.monotonic()

            results = list(executor.map(fetch_time_series, chunk))
            result_data.extend(results)

            elapsed = time.monotonic() - start
            remaining = max(0, WINDOW_SECONDS - elapsed)

            if i < len(chunks) - 1 and remaining > 0:
                logging.info(f"Sleeping {remaining:.1f}s before next chunk...")
                time.sleep(remaining)

    final_data = [df for df in result_data if df is not None]

    if not final_data:
        raise ValueError("No data extracted for any symbol — all requests failed")

    final_df = pd.concat(final_data, ignore_index=True)
    logging.info(f"Extracted {len(final_df)} rows across {final_df['symbol'].nunique()} symbols")

    final_df.to_parquet(STAGING_PATH, index=False)
    ti.xcom_push(key="staging_path", value=STAGING_PATH)


# def handle_schema(**kwargs):
#     ti = kwargs["ti"]
#     staging_path = ti.xcom_pull(task_ids="extract", key="staging_path")
#     df= pd.read_parquet(staging_path)

def load(**kwargs):
    ti = kwargs["ti"]
    staging_path = ti.xcom_pull(task_ids="extract", key="staging_path")
 
    df = pd.read_parquet(staging_path)
    logging.info(f"Loading {len(df)} rows into Snowflake from {staging_path}")
 
    hook = SnowflakeHook(snowflake_conn_id='destination_conn')
    logging.info("Creating a connection using Hook")
    conn = hook.get_conn()
    logging.info("Connection successful")
 
    success, num_chunks, num_rows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name="TWELVE_DATA",  
        auto_create_table=True,
        overwrite=False,          
    )
 
    logging.info(f"Loaded {num_rows} rows into TWELVE_DATA. Success: {success}")
    conn.close()
 
    # Clean up the staging file now that it's loaded
    if os.path.exists(staging_path):
        os.remove(staging_path)


with DAG(
    dag_id="twelvedata_extract_load",
    description="Extract daily stock time series from TwelveData and load into Snowflake",
    schedule=None,
    start_date=datetime(2026, 7, 24),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "owner": "Ujjwol kc",
    },
    tags=["finance", "twelvedata", "practice"],
) as dag:
    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    extract_task >> load_task