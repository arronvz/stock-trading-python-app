# Stock Trading Python App

A Python application that uses the [Polygon.io](https://polygon.io/) API to extract and analyze stock market data.

## Overview

This project provides tools for fetching stock data from the Polygon.io API, with support for scheduled data collection and batch processing of multiple tickers.

## Features

- **Stock Data Extraction** – Retrieve real-time and historical stock data via the Polygon.io API
- **Batch Processing** – Process multiple stock tickers from a CSV file
- **Scheduled Execution** – Automate data collection with the built-in scheduler

## Project Structure

```
stock-trading-phyton-app/
├── script.py          # Main script for fetching stock data
├── scheduler.py       # Scheduled task runner for automated data collection
├── tickers.csv        # List of stock tickers to track
├── requirements.txt   # Python dependencies
└── pythonenv/         # Python virtual environment
```

## Prerequisites

- Python 3.x
- A Polygon.io API key ([Sign up here](https://polygon.io/))

## Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/arronvz/stock-trading-phyton-app.git
   cd stock-trading-phyton-app
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

4. **Configure your API key**
   
   Set your Polygon.io API key as an environment variable:
   ```bash
   # On Windows
   set POLYGON_API_KEY=your_api_key_here
   
   # On macOS/Linux
   export POLYGON_API_KEY=your_api_key_here
   ```

## Usage

### Fetch Stock Data

Run the main script to fetch stock data:

```bash
python script.py
```

### Scheduled Data Collection

To run automated data collection on a schedule:

```bash
python scheduler.py
```

### Configuring Tickers

Edit the `tickers.csv` file to add or remove stock symbols you want to track.

## API Reference

This project uses the [Polygon.io API](https://polygon.io/docs/) for market data. Key endpoints include:

- **Aggregates (Bars)** – Historical OHLCV data
- **Ticker Details** – Company information
- **Market Status** – Exchange open/close times

## License

This project is open source. See the repository for license details.

## Contributing

Contributions are welcome! Feel free to submit issues or pull requests.

## Acknowledgments

- [Polygon.io](https://polygon.io/) for providing the market data API
