# About This Project

This **Stock Market Backtesting Tool** is designed to help traders and data enthusiasts analyze stock performance and optimize trading strategies. Powered by **Streamlit** and **Apache Spark**, the app processes historical stock data to generate key insights and simulate potential trade outcomes based on technical indicators like **SMA (Simple Moving Average)**, **ATR (Average True Range)**, and **RSI (Relative Strength Index)**.

## Key Features:

- **Backtesting Strategies**: Analyze buy/sell signals using a combination of indicators such as SMA, ATR, and RSI to evaluate potential trade outcomes.
- **Updated Data**: Fetch and process stock data from the **Alpha Vantage API**, ensuring up-to-date analysis.
- **Customizable Parameters**: Adjust target and stop-loss multipliers dynamically to test different risk-reward scenarios.
- **Performance Insights**: Get detailed metrics like win rate, total trades, and cumulative return over a specified period.
- **Data Visualization**: Interactive and intuitive charts to visualize stock trends and trading signals.

This tool is perfect for those looking to refine their stock market strategies with **data-driven insights**. Whether you're a seasoned trader or just getting started, this app provides the flexibility and power to explore various setups and improve decision-making.

## ❗ Disclaimer ❗

This project is intended for **educational purposes only**. None of the information, strategies, or analyses provided by this tool should be interpreted as a **recommendation** for any specific trading action or financial decision. Always conduct your own research and consult with financial professionals before making investment choices.

## Requirements

- Python 3.x
- Docker

## Installation

To install the required libraries, run:

```bash
pip install -r requirements_app.txt
```

## Prep

Get an API key at https://www.alphavantage.co/support/#api-key 

Set **.env** file with the API key and set a secret_key of your choice.

```env 
ALPHA_VANTAGE_API_KEY=paste_key_here
AIRFLOW_SECRET_KEY=lalala123
```

## Usage

### Step 1: Run the Airflow DAG

Run the following command to start Airflow in detached mode:

```bash
docker-compose up --build -d
```

This will start the Airflow environment so that the DAG can be triggered through the UI.

### Step 2: Run the Streamlit App

Once the DAG has completed, run the Streamlit app using:

```bash
streamlit run app.py
```

### Step 3: View the App

Open a web browser and navigate to http://localhost:8501 to view the app.

## Data

The app uses data from the `data/dave_landry_analysis.parquet` file.
