# NASDAQ Analysis Results

A Streamlit app for visualizing and analyzing NASDAQ stock data.

## Requirements

- Python 3.x
- Streamlit
- Pandas
- PyArrow
- Plotly
- Docker

## Installation

To install the required libraries, run:

```bash
pip install streamlit plotly pandas
```

## Usage

### Step 1: Run the Airflow DAG

Run the following command to start the Airflow DAG in detached mode:

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