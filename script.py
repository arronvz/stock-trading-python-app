import os
import logging
from datetime import datetime
from urllib.parse import quote

import requests
import snowflake.connector
from dotenv import load_dotenv


# -----------------------------------------------------------------------------
# Logging setup
# -----------------------------------------------------------------------------
# This sets up simple structured logging so the script tells us what it is doing.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Load environment variables
# -----------------------------------------------------------------------------
# This reads values from a local .env file so we do not hardcode secrets such as
# API keys and database credentials directly into the script.
load_dotenv()


def get_required_env(name: str, default: str | None = None) -> str:
    """
    Read an environment variable and fail fast if it is missing or still
    contains a placeholder value like 'your_account'.

    Why this matters:
    - It gives a clear error at the start of the script
    - It avoids confusing failures later in the pipeline
    """
    value = os.getenv(name, default)

    if not value or value.strip() == "":
        raise RuntimeError(f"Missing required environment variable: {name}")

    # Catch common placeholder values left in .env files
    lowered = value.lower().strip()
    if lowered.startswith("your_") or lowered == "your_role":
        raise RuntimeError(f"Invalid placeholder value for environment variable: {name}")

    return value


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
# These are the settings the pipeline needs to run.
# They come from .env file.
POLYGON_API_KEY = get_required_env("POLYGON_API_KEY")

SNOWFLAKE_USER = get_required_env("SNOWFLAKE_USER", "arron")
SNOWFLAKE_PASSWORD = get_required_env("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = get_required_env("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = get_required_env("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = get_required_env("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = get_required_env("SNOWFLAKE_SCHEMA")
SNOWFLAKE_ROLE = get_required_env("SNOWFLAKE_ROLE")
SNOWFLAKE_TABLE = get_required_env("SNOWFLAKE_TABLE")

# API paging settings
LIMIT = 10
MAX_PAGES = 2

# Fully qualified table name so Snowflake always knows exactly where to write
FULL_TABLE_NAME = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}"


def get_last_processed_ticker(cur) -> str | None:
    """
    Get the highest ticker currently stored in Snowflake.

    Steps:
    - We use this as our checkpoint
    - The next API run starts after this ticker
    - If the table is empty, return None so we start from the beginning
    """
    query = f"SELECT MAX(ticker) FROM {FULL_TABLE_NAME}"
    cur.execute(query)
    result = cur.fetchone()

    if result and result[0]:
        return result[0]

    return None


def fetch_tickers(ds: str, last_ticker: str | None = None) -> list[dict]:
    """
    Pull stock ticker reference data from the Massive/Polygon API.

    Steps:
    - Build the API URL
    - If we already loaded some records before, start after the last ticker
    - Keep following next pages until we hit MAX_PAGES
    - Add the processing date (ds) to every record
    - Return a list of ticker dictionaries
    """
    base_url = (
        "https://api.massive.com/v3/reference/tickers"
        f"?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker"
    )

    # If we have already processed tickers, continue from the next one
    if last_ticker:
        # quote() safely encodes any special characters if needed
        base_url += f"&ticker.gt={quote(last_ticker)}"
        logger.info("Continuing from last processed ticker: %s", last_ticker)
    else:
        logger.info("No existing ticker found in Snowflake, starting from the beginning")

    base_url += f"&apiKey={POLYGON_API_KEY}"

    logger.info("Requesting first page of ticker data")
    response = requests.get(base_url, timeout=30)

    if response.status_code != 200:
        raise RuntimeError(
            f"Failed to fetch initial data: {response.status_code} {response.text}"
        )

    data = response.json()

    if "results" not in data:
        raise RuntimeError(f"Unexpected API response structure: {data}")

    tickers = []

    # Add ds to the first page records
    for ticker in data["results"]:
        ticker["ds"] = ds
        tickers.append(ticker)

    page_count = 0

    # Continue paging while the API gives us a next_url and we are under the max
    while "next_url" in data and data["next_url"] and page_count < MAX_PAGES:
        next_url = f"{data['next_url']}&apiKey={POLYGON_API_KEY}"
        logger.info("Requesting next page: %s", next_url)

        response = requests.get(next_url, timeout=30)

        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to fetch next page {data['next_url']}: "
                f"{response.status_code} {response.text}"
            )

        data = response.json()

        if "results" in data:
            for ticker in data["results"]:
                ticker["ds"] = ds
                tickers.append(ticker)

        page_count += 1

    logger.info("Fetched %s ticker records from API", len(tickers))
    return tickers


def get_snowflake_connection():
    """
    Create a Snowflake connection using the environment variables.

    Steps:
    - Connect to Snowflake with the credentials from .env
    - Pass warehouse, database, schema, and role into the connection
    """
    logger.info("Connecting to Snowflake account %s", SNOWFLAKE_ACCOUNT)

    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE
    )


def create_table_if_not_exists(cur):
    """
    Create the target Snowflake table if it does not already exist.

    Steps:
    - This makes the pipeline idempotent
    - If the table already exists, Snowflake leaves it alone
    - If the table does not exist, Snowflake creates it
    """
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {FULL_TABLE_NAME} (
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
            ds DATE
        )
    """

    logger.info("Ensuring table exists: %s", FULL_TABLE_NAME)
    cur.execute(create_table_sql)


def insert_tickers(cur, tickers: list[dict], ds: str):
    """
    Insert ticker records into Snowflake.

    Steps:
    - Convert the API JSON into rows Snowflake can insert
    - Insert all rows in one batch using executemany
    - This is more efficient than inserting one row at a time
    """
    if not tickers:
        logger.info("No ticker records returned from API, nothing to insert")
        return

    insert_sql = f"""
        INSERT INTO {FULL_TABLE_NAME} (
            ticker,
            name,
            market,
            locale,
            primary_exchange,
            type,
            active,
            currency_name,
            cik,
            composite_figi,
            share_class_figi,
            last_updated_utc,
            ds
        )
        VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s
        )
    """

    rows_to_insert = []
    for t in tickers:
        row = (
            t.get("ticker"),
            t.get("name"),
            t.get("market"),
            t.get("locale"),
            t.get("primary_exchange"),
            t.get("type"),
            t.get("active", False),
            t.get("currency_name"),
            t.get("cik"),
            t.get("composite_figi"),
            t.get("share_class_figi"),
            t.get("last_updated_utc"),
            t.get("ds", ds),
        )
        rows_to_insert.append(row)

    logger.info("Inserting %s rows into %s", len(rows_to_insert), FULL_TABLE_NAME)
    cur.executemany(insert_sql, rows_to_insert)


def run_stock_job():
    """
    Main pipeline function.

    Flow:
    1. Work out today's processing date
    2. Connect to Snowflake
    3. Explicitly set the Snowflake session context
    4. Create the destination table if needed
    5. Read the last processed ticker from Snowflake table
    6. Fetch the next batch from the API starting after that ticker
    7. Insert the data
    8. Close the connection cleanly
    """
    ds = datetime.now().strftime("%Y-%m-%d")
    logger.info("Starting stock ticker pipeline for ds=%s", ds)

    conn = get_snowflake_connection()

    try:
        with conn.cursor() as cur:
            # Explicitly set session context.
            # This makes the script more reliable and easier to debug.
            cur.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
            cur.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
            cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")

            create_table_if_not_exists(cur)

            last_ticker = get_last_processed_ticker(cur)
            tickers = fetch_tickers(ds, last_ticker)

            insert_tickers(cur, tickers, ds)

        conn.commit()
        logger.info("Pipeline completed successfully")

    finally:
        conn.close()
        logger.info("Snowflake connection closed")


if __name__ == "__main__":
    run_stock_job()