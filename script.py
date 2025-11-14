import requests
import os
import snowflake.connector
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

# Snowflake connection settings
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "arron")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_TABLE")

LIMIT = 10
MAX_PAGES = 2
DS = '2025-11-13'

def run_stock_job():
    DS = datetime.now().strftime('%Y-%m-%d')
    url = f'https://api.massive.com/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}'

    response = requests.get(url)
    if response.status_code != 200:
        raise RuntimeError(f"Failed to fetch initial data: {response.status_code} {response.text}")
    
    tickers = []

    data = response.json()
    if 'results' not in data:
        raise RuntimeError(f"Unexpected API response structure: {data}")
    
    for ticker in data['results']:
        tickers.append(ticker)

    page_count = 0
    while 'next_url' in data and page_count < MAX_PAGES:
        print('requesting next page', data['next_url'])
        response = requests.get(data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
        if response.status_code != 200:
            raise RuntimeError(f"Failed to fetch next page {data['next_url']}: {response.status_code} {response.text}")
        data = response.json()
        
        if 'results' in data:
            for ticker in data['results']:
                ticker['ds'] = DS
                tickers.append(ticker)
        
        page_count += 1

    example_ticker = {'ticker': 'HOUR', 
        'name': 'Hour Loop, Inc. Common Stock', 
        'market': 'stocks', 
        'locale': 'us', 
        'primary_exchange': 'XNAS', 
        'type': 'CS', 
        'active': True, 
        'currency_name': 'usd', 
        'cik': '0001874875', 
        'composite_figi': 'BBG0137W8PC7', 
        'share_class_figi': 'BBG0137W8QC5', 
        'last_updated_utc': '2025-11-06T07:05:49.709353551Z',
        'ds' : '2025-11-13'}

    # Get fieldnames from example_ticker schema
    fieldnames = list(example_ticker.keys())

    # Connect to Snowflake
    if not all([SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA]):
        raise RuntimeError("Missing Snowflake environment variables: SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA")

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE
    )

    try:
        with conn.cursor() as cur:
            # Create table if it doesn't exist with the same schema as example_ticker
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE} (
                    ticker VARCHAR,
                    name VARCHAR,
                    market VARCHAR,
                    locale VARCHAR,
                    primary_exchange VARCHAR,
                    type VARCHAR,
                    active BOOLEAN,
                    currency_name VARCHAR,
                    cik VARCHAR,
                    composite_figi VARCHAR,
                    share_class_figi VARCHAR,
                    last_updated_utc TIMESTAMP,
                    DS DATE
                )
            """
            cur.execute(create_table_sql)

            # Insert data
            if tickers:
                insert_sql = f"""
                    INSERT INTO {SNOWFLAKE_TABLE} (
                        ticker, name, market, locale, primary_exchange, type,
                        active, currency_name, cik, composite_figi, share_class_figi, last_updated_utc, ds
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s
                    )
                """
                
                rows_to_insert = []
                for t in tickers:
                    row = (
                        t.get('ticker', ''),
                        t.get('name', ''),
                        t.get('market', ''),
                        t.get('locale', ''),
                        t.get('primary_exchange', ''),
                        t.get('type', ''),
                        t.get('active', False),
                        t.get('currency_name', ''),
                        t.get('cik', ''),
                        t.get('composite_figi', ''),
                        t.get('share_class_figi', ''),
                        t.get('last_updated_utc', ''),
                        t.get('ds', DS)
                    )
                    rows_to_insert.append(row)
                
                cur.executemany(insert_sql, rows_to_insert)
                print(f'Inserted {len(rows_to_insert)} rows into {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}')
            else:
                print('No tickers to insert')
    finally:
        conn.close()

if __name__ == '__main__':
    run_stock_job()
        