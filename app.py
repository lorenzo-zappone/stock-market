import streamlit as st
import pandas as pd
import pyarrow.parquet as pq
import plotly.express as px
from datetime import datetime

# Load the Parquet directory
parquet_dir = 'data/dave_landry_analysis.parquet'
table = pq.ParquetDataset(parquet_dir).read()

# Convert to pandas DataFrame
df = table.to_pandas()

# Convert columns to numeric, errors='coerce' will handle any non-numeric values
df['Close'] = pd.to_numeric(df['Close'], errors='coerce')
df['SMA_21'] = pd.to_numeric(df['SMA_21'], errors='coerce')
df['SMA_50'] = pd.to_numeric(df['SMA_50'], errors='coerce')

# Ensure Date is a datetime type
df['Date'] = pd.to_datetime(df['Date'])

# Streamlit app
st.title("NASDAQ Analysis Results")

# Filter by stock symbol
symbols = df['Symbol'].unique()
selected_symbol = st.selectbox("Select a stock symbol:", symbols)

# Filter data based on selected symbol
filtered_df = df[df['Symbol'] == selected_symbol]

# Date range filter
min_date = filtered_df['Date'].min()
max_date = filtered_df['Date'].max()

# Slider for selecting date range
start_date, end_date = st.slider(
    "Select date range:",
    min_value=min_date.date(),  # Convert to date type for the slider
    max_value=max_date.date(),  # Convert to date type for the slider
    value=(min_date.date(), max_date.date())
)

# Filter the data by date range
filtered_df = filtered_df[(filtered_df['Date'] >= pd.to_datetime(start_date)) & (filtered_df['Date'] <= pd.to_datetime(end_date))]
filtered_df = filtered_df.sort_values(by='Date', ascending=False)

# Timeframe selection
timeframe = st.selectbox("Select timeframe:", ['Daily', 'Weekly', 'Monthly'])

# Resample data according to selected timeframe
if timeframe == 'Weekly':
    filtered_df = filtered_df.set_index('Date').resample('W-Mon').agg({
        'Close': 'last',
        'SMA_21': 'last',
        'SMA_50': 'last',
        'Signal': 'last'
    }).reset_index().sort_values(by='Date', ascending=False)
elif timeframe == 'Monthly':
    filtered_df = filtered_df.set_index('Date').resample('M').agg({
        'Close': 'last',
        'SMA_21': 'last',
        'SMA_50': 'last',
        'Signal': 'last'
    }).reset_index().sort_values(by='Date', ascending=False)

# Display the filtered dataframe
st.write(f"Data for {selected_symbol} ({timeframe})")
st.write(filtered_df)

# Plot the stock's Close price with SMA_21 and SMA_50
fig = px.line(filtered_df, x='Date', y=['Close', 'SMA_21', 'SMA_50'], 
              labels={'value': 'Price', 'variable': 'Indicator'},
              title=f"Price and SMAs for {selected_symbol} ({timeframe})")
st.plotly_chart(fig)

# Show Buy/Hold/Sell recommendation
latest_signal = filtered_df.iloc[-1]['Signal']
if latest_signal == 'Buy':
    st.success(f"Recommendation for {selected_symbol}: **{latest_signal}**")
elif latest_signal == 'Sell':
    st.error(f"Recommendation for {selected_symbol}: **{latest_signal}**")
else:
    st.warning(f"Recommendation for {selected_symbol}: **{latest_signal}**")
