import requests
import os
from datetime import datetime, timedelta, UTC
from dotenv import load_dotenv
from twelvedata import TDClient
import snowflake.connector

load_dotenv()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
TWELVE_API_KEY = os.getenv("TWELVE_API_KEY")
TIINGO_API_KEY = os.getenv("TIINGO_API_KEY")



LIMIT = 1000
def run_stock_job():

    yesterday_date = (datetime.now(UTC) - timedelta(days=1)).strftime('%Y-%m-%d')

    url = f'https://api.massive.com/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}'
    twelve_url = f'https://api.twelvedata.com/time_series?symbol=?&interval=1day&apikey={TWELVE_API_KEY}'

    response = requests.get(url)

    headers = {
            'Content-Type': 'application/json'
            }

    requestResponse = requests.get(f'https://api.tiingo.com/tiingo/crypto/prices?tickers=btcusd&startDate={yesterday_date}&resampleFreq=5min&token={TIINGO_API_KEY}', headers=headers)


    data = requestResponse.json()

    example_ticker ={
            "ticker":"btcusd",
            "baseCurrency":"btc",
            "quoteCurrency":"usd",
            "priceData":[
                {
                    "open":3914.749407813885,
                    "high":3942.374263716895,
                    "low":3846.1755315352952,
                    "close":3849.1217299601617,
                    "date":"2019-01-02T00:00:00+00:00",
                    "tradesDone":756.0,
                    "volume":339.68131616889997,
                    "volumeNotional":1307474.735327181
                }
            ]
        }

    # Flatten the schema: ticker, baseCurrency, quoteCurrency + all priceData fields
    price_data_fields = ['open', 'high', 'low', 'close', 'date', 'tradesDone', 'volume', 'volumeNotional']
    fieldnames = ['ticker', 'baseCurrency', 'quoteCurrency'] + price_data_fields
    table_name = os.getenv("SNOWFLAKE_TABLE", "CRYPTO_PRICES")

    # Handle response - API returns a list of ticker objects
    ticker_data = data if isinstance(data, list) else [data]

    # Build rows for Snowflake insert
    rows_to_insert = []
    for ticker_obj in ticker_data:
        ticker = ticker_obj.get('ticker', '')
        base_currency = ticker_obj.get('baseCurrency', '')
        quote_currency = ticker_obj.get('quoteCurrency', '')
        price_data = ticker_obj.get('priceData', [])

        for price_entry in price_data:
            rows_to_insert.append((
                ticker,
                base_currency,
                quote_currency,
                price_entry.get('open', None),
                price_entry.get('high', None),
                price_entry.get('low', None),
                price_entry.get('close', None),
                price_entry.get('date', None),
                price_entry.get('tradesDone', None),
                price_entry.get('volume', None),
                price_entry.get('volumeNotional', None),
            ))

    # Snowflake connection settings from environment variables
    sf_user = os.getenv("SNOWFLAKE_USER")
    sf_password = os.getenv("SNOWFLAKE_PASSWORD")
    sf_account = os.getenv("SNOWFLAKE_ACCOUNT")
    sf_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    sf_database = os.getenv("SNOWFLAKE_DATABASE")
    sf_schema = os.getenv("SNOWFLAKE_SCHEMA")
    sf_role = os.getenv("SNOWFLAKE_ROLE")

    if not all([sf_user, sf_password, sf_account, sf_warehouse, sf_database, sf_schema]):
        raise RuntimeError("Missing one or more Snowflake env vars: SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA")

    conn = snowflake.connector.connect(
        user=sf_user,
        password=sf_password,
        account=sf_account,
        warehouse=sf_warehouse,
        database=sf_database,
        schema=sf_schema,
        role=sf_role
    )

    try:
        with conn.cursor() as cur:
            # Create table if it doesn't exist
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    ticker VARCHAR,
                    baseCurrency VARCHAR,
                    quoteCurrency VARCHAR,
                    open FLOAT,
                    high FLOAT,
                    low FLOAT,
                    close FLOAT,
                    date VARCHAR,
                    tradesDone FLOAT,
                    volume FLOAT,
                    volumeNotional FLOAT
                )
            """)

            # Insert data
            insert_sql = f"""
                INSERT INTO {table_name} (
                    ticker, baseCurrency, quoteCurrency,
                    open, high, low, close, date,
                    tradesDone, volume, volumeNotional
                )
                VALUES (
                    %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s
                )
            """
            if rows_to_insert:
                cur.executemany(insert_sql, rows_to_insert)
                conn.commit()
    finally:
        conn.close()

if __name__ == '__main__':
    run_stock_job()

    '''
    # Initialize client with your API key
    td = TDClient(apikey= TWELVE_API_KEY)

    # Get latest price for Apple
    price = td.price(symbol="AAPL").as_json()

    print(price)
    '''

    
    tickers = []

    data = response.json()
    for ticker in data['results']:
        tickers.append(ticker)

        while 'next_url' in data:
            print('requesting next page',data['next_url'])
            response = requests.get(data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
            data = response.json()
            
            for ticker in data['results']:
                tickers.append(ticker)
    print(data)

    example_ticker2 = {'ticker': 'HOUR', 
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
        'last_updated_utc': '2025-11-06T07:05:49.709353551Z'}

    # Write results to CSV with the same schema as example_ticker
    fieldnames = list(example_ticker.keys())
    output_csv = 'tickers.csv'

    with open(output_csv, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for t in tickers:
            row = {key: t.get(key, '') for key in fieldnames}
            writer.writerow(row)
            print(f'Wrote {len(tickers)} rows to {output_csv}')
            