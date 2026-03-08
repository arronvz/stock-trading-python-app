# Stock Trading Python Data Pipeline

A Python application that uses the [Massive.com](https://massive.com/) API (formerly Polygon.io) to extract stock ticker reference data and load it into a Snowflake data warehouse.

### Overview

This project demonstrates a simple **data engineering pipeline** that retrieves stock ticker reference data from the Massive API, processes it incrementally, and loads it into Snowflake.

The pipeline supports scheduled execution and incremental ingestion so that each run continues from the last processed record.

### Architecture

```
Massive.com API
        │
        ▼
Python Ingestion Script (script.py)
        │
        ├── Pagination Handling (next_url)
        │
        ├── Incremental Processing
        │      └── Continue after last processed ticker
        │
        ▼
Snowflake Data Warehouse
(STOCK_DB.STOCK_SCHEMA.STOCK_TICKERS)
        │
        ▼
Analytics / Reporting
```

A lightweight scheduler triggers the ingestion pipeline automatically.

```
Scheduler (scheduler.py)
        │
        ▼
Daily Pipeline Execution
```

### Features

- **Stock Data Extraction** – Retrieve stock ticker reference data from the Massive.com API  
- **Incremental Ingestion** – Each run continues from the last processed ticker  
- **Pagination Handling** – Automatically processes multiple API pages using `next_url`  
- **Snowflake Integration** – Loads structured data directly into a Snowflake warehouse  
- **Scheduled Execution** – Automate data collection with the built-in scheduler  
- **Structured Logging** – Clear pipeline logs for monitoring and debugging  

### Project Structure

```
stock-trading-python-app/
├── script.py          # Main ingestion pipeline (API → Snowflake)
├── scheduler.py       # Scheduled task runner for automated data collection
├── requirements.txt   # Python dependencies
├── .env               # Environment variables (API + Snowflake credentials)
└── README.md
```

### Prerequisites

- Python 3.x
- A Massive.com API key ([Sign up here](https://massive.com/))
- A Snowflake account
- A Snowflake warehouse

### Installation

1. **Clone the repository**

```bash
git clone https://github.com/arronvz/stock-trading-python-app.git
cd stock-trading-python-app
```

2. **Create and activate a virtual environment**

```bash
python -m venv pythonenv

# On Windows
pythonenv\Scripts\activate

# On macOS/Linux
source pythonenv/bin/activate
```

3. **Install dependencies**

```bash
pip install -r requirements.txt
```

4. **Configure environment variables**

Create a `.env` file in the project root.

Example configuration:

```
POLYGON_API_KEY=your_massive_api_key

SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=STOCK_DB
SNOWFLAKE_SCHEMA=STOCK_SCHEMA
SNOWFLAKE_ROLE=PUBLIC
SNOWFLAKE_TABLE=STOCK_TICKERS
```

### Usage

### Run the Data Pipeline

Run the ingestion script to fetch stock ticker data and load it into Snowflake.

```bash
python script.py
```

The pipeline will:

1. Request ticker reference data from the Massive API  
2. Handle pagination using `next_url`  
3. Determine the last processed ticker from Snowflake  
4. Fetch only new records  
5. Insert them into the Snowflake table  

### Scheduled Data Collection

To run automated data collection on a schedule:

```bash
python scheduler.py
```

The scheduler can run the pipeline:

- every minute (for testing)
- once per day (for production-style execution)

### Incremental Processing

To avoid reprocessing the same records each run, the pipeline performs **incremental ingestion**.

Steps:

1. Query Snowflake for the last stored ticker

```
SELECT MAX(ticker)
FROM STOCK_DB.STOCK_SCHEMA.STOCK_TICKERS
```

2. Use this value in the API request

```
ticker.gt=LAST_TICKER
```

3. Fetch only new records.

This allows the pipeline to **continue exactly where the previous run stopped**.

### API Reference

This project uses the [Massive.com API](https://massive.com/docs/) for stock market data.

Key endpoint used:

- **Ticker Reference Endpoint** – Provides metadata for all active stock tickers

### License

This project is open source. See the repository for license details.

### Contributing

Contributions are welcome! Feel free to submit issues or pull requests.

### Acknowledgments

- [Massive.com](https://massive.com/) for providing the stock market data API  
- [Snowflake](https://www.snowflake.com/) for the cloud data warehouse platform  

