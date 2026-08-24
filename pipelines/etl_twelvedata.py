import requests, time
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor
from utilities.utils_snowflake import SnowflakeDestination
from dotenv import load_dotenv

load_dotenv()


API_KEY = os.getenv("API_KEY")
BASE = "https://api.twelvedata.com"
HEADERS = {"Authorization": f"apikey {API_KEY}"}

RATE_LIMIT = 7
WINDOW_SECONDS = 60

def fatch_time_series(symbol):
    params = {"symbol":symbol, "interval":"1day", "order":"asc", "outputsize":5000}
    request= requests.get(f"{BASE}/time_series",headers= HEADERS,params=params)
    data = request.json()


    meta = data["meta"]
    df = pd.DataFrame(data["values"])
    df= df.assign(
        symbol= meta["symbol"],
        interval= meta["interval"],
        currency= meta["currency"],
        exchange= meta["exchange"]
    )

    return df

def chunk_list(symbol,size):
    chunks = []
    for i in range(0,len(symbol),size):
        chunks.append(symbol[i:i+ size])
    return(chunks)

  

if __name__ == "__main__":

    with ThreadPoolExecutor(max_workers=5) as executor:
        symbol = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "JPM", "V", "JNJ"]
        chunks = chunk_list(symbol,RATE_LIMIT)


        result_data = []
        for i, chunk in enumerate(chunks):
            print(f"processing chunk:{i}/ symbols in list:{chunk}")
            results = list(executor.map(fatch_time_series,chunk))
            result_data.extend(results)
            if i < len(chunks) - 1:          # only sleep if this ISN'T the last chunk
                print(f"Sleeping {WINDOW_SECONDS}s before next chunk...")
                time.sleep(WINDOW_SECONDS)

        final_data = [df for df in result_data if df is not None]
        final_df = pd.concat(final_data,ignore_index= True)


        snow_conn = SnowflakeDestination(database="TWELVEDATA_DB",schema="TWELVEDATA",table_name="TWELVE_DATA")
        snow_conn.load_into_snowflake(df=final_df)
