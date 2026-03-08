Stock Trading Data Pipeline (Python + Snowflake)

A Python-based data pipeline that extracts stock ticker reference data from the Massive.com API (formerly Polygon.io), processes it incrementally, and loads it into Snowflake for analytics.

The pipeline supports automated scheduling and incremental ingestion so that each run continues from the last processed record.

Overview

This project demonstrates a simple data engineering pipeline built with Python and Snowflake.

The pipeline:

Retrieves stock ticker reference data from the Massive API

Handles API pagination

Performs incremental ingestion using Snowflake as the checkpoint source

Loads the data into a Snowflake table

Can be executed manually or via a scheduler

The goal of the project is to simulate a production-style ingestion pipeline that is automated, observable, and idempotent.

Architecture
Massive.com API
        │
        ▼
Python Ingestion Script
(script.py)
        │
        ▼
Incremental Processing
(last processed ticker checkpoint)
        │
        ▼
Snowflake Data Warehouse
(STOCK_DB.STOCK_SCHEMA.STOCK_TICKERS)
        │
        ▼
Scheduler
(scheduler.py)
Features
Stock Data Extraction

Fetch stock ticker reference data from the Massive.com API.

Incremental Data Loading

Each run continues from the last processed ticker, preventing duplicate processing.

Pagination Handling

Automatically retrieves multiple API pages using next_url.

Snowflake Data Warehouse Integration

Loads structured ticker data directly into a Snowflake table.

Automated Scheduling

Includes a lightweight Python scheduler for automated runs.

Structured Logging

Uses Python logging to provide clear visibility into pipeline execution.

Project Structure
stock-trading-python-app/
│
├── script.py
│   Main ingestion pipeline:
│   - Calls Massive API
│   - Handles pagination
│   - Performs incremental ingestion
│   - Loads data into Snowflake
│
├── scheduler.py
│   Runs the pipeline automatically on a schedule.
│
├── requirements.txt
│   Python dependencies required to run the project.
│
├── .env
│   Environment variables for API keys and Snowflake credentials.
│
└── README.md
Prerequisites

Before running the project you will need:

Python 3.9+

A Massive.com API key

A Snowflake account

A Snowflake warehouse

Installation
1. Clone the repository
git clone https://github.com/arronvz/stock-trading-python-app.git
cd stock-trading-python-app
2. Create a virtual environment
python -m venv pythonenv

Activate it:

Mac / Linux

source pythonenv/bin/activate

Windows

pythonenv\Scripts\activate
3. Install dependencies
pip install -r requirements.txt
Environment Configuration

Create a .env file in the project root.

Example:

POLYGON_API_KEY=your_massive_api_key

SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=STOCK_DB
SNOWFLAKE_SCHEMA=STOCK_SCHEMA
SNOWFLAKE_ROLE=PUBLIC
SNOWFLAKE_TABLE=STOCK_TICKERS
Snowflake Setup

Create the database and schema in Snowflake:

CREATE DATABASE STOCK_DB;

CREATE SCHEMA STOCK_DB.STOCK_SCHEMA;

The pipeline will automatically create the target table if it does not exist.

Running the Pipeline
Run the ingestion job manually
python script.py

This will:

Connect to the Massive API

Fetch ticker reference data

Continue from the last stored ticker

Insert new records into Snowflake

Scheduling the Pipeline

You can run the scheduler to execute the pipeline automatically.

python scheduler.py

Example schedule:

Runs every minute (test job)
Runs the stock pipeline once per day

For testing purposes you can also schedule the pipeline to run every minute.

Incremental Ingestion

To avoid reprocessing the same data each run, the pipeline implements incremental loading.

Steps:

Query Snowflake for the highest ticker already stored

SELECT MAX(ticker)
FROM STOCK_DB.STOCK_SCHEMA.STOCK_TICKERS

Use that value to filter the API request

ticker.gt=LAST_TICKER

Load only new records.

This makes the pipeline idempotent and prevents duplicate data ingestion.

Logging

The pipeline uses structured logging for visibility.

Example output:

Starting stock ticker pipeline
Requesting first page of ticker data
Fetched 30 ticker records from API
Inserting 30 rows into STOCK_DB.STOCK_SCHEMA.STOCK_TICKERS
Pipeline completed successfully
Future Improvements

Possible enhancements to make this a production-grade pipeline:

Use MERGE instead of INSERT to prevent duplicates

Store pipeline checkpoints in a state table

Replace the scheduler with Airflow / Prefect / Dagster

Add data validation tests

Implement Bronze / Silver / Gold data layers

License

This project is open source and available under the MIT License.

Acknowledgments

Massive.com API for stock market reference data

Snowflake for the cloud data warehouse platform