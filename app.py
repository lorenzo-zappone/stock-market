import streamlit as st
import pandas as pd
import pyarrow.parquet as pq
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os

# Configurar o layout da página
st.set_page_config(page_title="NASDAQ Analysis", layout="wide")

# Carregar o diretório Parquet
parquet_dir = 'data/dave_landry_analysis.parquet'
table = pq.ParquetDataset(parquet_dir).read()

# Converter para DataFrame do pandas
df = table.to_pandas()

# Contagem total de registros
total_records = df.shape[0]

# Calcular o tamanho dos arquivos Parquet em MB
file_size_mb = sum(os.path.getsize(os.path.join(parquet_dir, f)) for f in os.listdir(parquet_dir)) / (1024 * 1024)

# Converter colunas para tipos numéricos, erros='coerce' irá lidar com valores não numéricos
df['Close'] = pd.to_numeric(df['Close'], errors='coerce')
df['SMA_21'] = pd.to_numeric(df['SMA_21'], errors='coerce')
df['SMA_50'] = pd.to_numeric(df['SMA_50'], errors='coerce')

# Garantir que a coluna Date é do tipo datetime
df['Date'] = pd.to_datetime(df['Date'])

# Calcular os retornos diários
df['Return'] = df.groupby('Symbol')['Close'].pct_change()

# Sidebar para navegação
st.sidebar.title("Menu")
section = st.sidebar.radio("Ir para", ["Visão Geral", "Análise de Ações", "Análise de Potencial"])

# Seção: Visão Geral
if section == "Visão Geral":
    st.title("NASDAQ Analysis Results")
    st.write("Nesta seção, você encontra uma visão geral dos dados de análise.")

    # Exibir contagem total de registros e tamanho do arquivo com gauges
    st.header("Resumo dos Dados")
    col1, col2 = st.columns(2)
    col1.metric("Total de Registros", total_records)
    col2.metric("Tamanho Total do Arquivo (MB)", f"{file_size_mb:.2f}")

    # Insights Adicionais com Representações Visuais
    st.header("Insights Adicionais")

    # Criação das colunas para os gauges
    col1, col2, col3 = st.columns(3)

    # 1. Ação com Melhor Desempenho
    best_performance = df.groupby('Symbol')['Return'].sum().reset_index()
    best_performance['Retorno Acumulado (%)'] = best_performance['Return'] * 100
    best_stock = best_performance.loc[best_performance['Retorno Acumulado (%)'].idxmax()]

    fig_best = go.Figure(go.Indicator(
        mode="gauge+number",
        value=best_stock['Retorno Acumulado (%)'],
        title={'text': f"Melhor Desempenho: {best_stock['Symbol']}"},
        gauge={'axis': {'range': [None, max(best_performance['Retorno Acumulado (%)'] * 1.1)]},
               'bar': {'color': "green"}}
    ))
    col1.plotly_chart(fig_best, use_container_width=True)

    # 2. Ação com Pior Desempenho
    worst_stock = best_performance.loc[best_performance['Retorno Acumulado (%)'].idxmin()]

    fig_worst = go.Figure(go.Indicator(
        mode="gauge+number",
        value=worst_stock['Retorno Acumulado (%)'],
        title={'text': f"Pior Desempenho: {worst_stock['Symbol']}"},
        gauge={'axis': {'range': [min(best_performance['Retorno Acumulado (%)']) * 1.1, 0]},
               'bar': {'color': "red"}}
    ))
    col2.plotly_chart(fig_worst, use_container_width=True)

    # 3. Volatilidade Média das Ações
    volatility = df.groupby('Symbol')['Return'].std().reset_index()
    volatility['Volatilidade (%)'] = volatility['Return'] * 100
    avg_volatility = volatility['Volatilidade (%)'].mean()

    fig_volatility = go.Figure(go.Indicator(
        mode="gauge+number",
        value=avg_volatility,
        title={'text': "Volatilidade Média (%)"},
        gauge={'axis': {'range': [0, max(volatility['Volatilidade (%)']) * 1.1]},
               'bar': {'color': "orange"}}
    ))
    col3.plotly_chart(fig_volatility, use_container_width=True)

    # 4. Ação com Maior Frequência de Sinais de Compra
    buy_signals = df[df['Signal'] == 'Buy']
    most_buy_signals = buy_signals['Symbol'].value_counts().idxmax()

    # Gauge para Frequência de Sinais de Compra
    fig_most_buys = go.Figure(go.Indicator(
        mode="gauge+number",
        value=buy_signals['Symbol'].value_counts().max(),
        title={'text': f"Maior Frequência de Compra: {most_buy_signals}"},
        gauge={'axis': {'range': [0, buy_signals['Symbol'].value_counts().max() * 1.1]},
               'bar': {'color': "blue"}}
    ))
    st.plotly_chart(fig_most_buys, use_container_width=True)

    # 5. Distribuição dos Sinais com Bar Chart
    signal_distribution = df['Signal'].value_counts()
    fig_signals = px.bar(
        x=signal_distribution.index,
        y=signal_distribution.values,
        labels={'x': 'Sinal', 'y': 'Quantidade'},
        title='Distribuição dos Sinais'
    )
    st.plotly_chart(fig_signals, use_container_width=True)

# Seção: Análise de Ações
elif section == "Análise de Ações":
    st.title("Análise de Ações")
    st.write("Selecione um símbolo de ação para ver detalhes de desempenho.")

    # Filtro por símbolo de ação
    symbols = df['Symbol'].unique()
    selected_symbol = st.selectbox("Selecione um símbolo de ação:", symbols)

    # Filtrar dados com base no símbolo selecionado
    filtered_df = df[df['Symbol'] == selected_symbol]

    # Filtro por intervalo de datas
    min_date = filtered_df['Date'].min()
    max_date = filtered_df['Date'].max()

    # Input para selecionar intervalo de datas
    start_date = st.date_input("Data de Início:", min_value=min_date.date(), max_value=max_date.date(), value=min_date.date())
    end_date = st.date_input("Data de Fim:", min_value=min_date.date(), max_value=max_date.date(), value=max_date.date())

    # Filtrar os dados pelo intervalo de datas selecionado
    filtered_df = filtered_df[(filtered_df['Date'] >= pd.to_datetime(start_date)) & (filtered_df['Date'] <= pd.to_datetime(end_date))]
    filtered_df = filtered_df.sort_values(by='Date', ascending=False)

    # Seleção de timeframe
    timeframe = st.selectbox("Selecione o Timeframe:", ['Diário', 'Semanal', 'Mensal'])

    # Resample dos dados de acordo com o timeframe selecionado
    if timeframe == 'Semanal':
        filtered_df = filtered_df.set_index('Date').resample('W-Mon').agg({
            'Close': 'last',
            'SMA_21': 'last',
            'SMA_50': 'last',
            'Return': 'sum',
            'Signal': 'last'
        }).reset_index().sort_values(by='Date', ascending=False)
    elif timeframe == 'Mensal':
        filtered_df = filtered_df.set_index('Date').resample('M').agg({
            'Close': 'last',
            'SMA_21': 'last',
            'SMA_50': 'last',
            'Return': 'sum',
            'Signal': 'last'
        }).reset_index().sort_values(by='Date', ascending=False)

    # Exibir o DataFrame filtrado
    st.write(f"Dados para {selected_symbol} ({timeframe})")
    st.write(filtered_df)

    # Plotar o preço de fechamento com SMA_21 e SMA_50
    fig = px.line(filtered_df, x='Date', y=['Close', 'SMA_21', 'SMA_50'], 
                  labels={'value': 'Preço', 'variable': 'Indicador'},
                  title=f"Preço e Médias Móveis para {selected_symbol} ({timeframe})")
    st.plotly_chart(fig)

    # Mostrar recomendação de Compra/Manter/Venda
    latest_signal = filtered_df.iloc[-1]['Signal']
    if latest_signal == 'Buy':
        st.success(f"Recomendação para {selected_symbol}: **{latest_signal}**")
    elif latest_signal == 'Sell':
        st.error(f"Recomendação para {selected_symbol}: **{latest_signal}**")
    else:
        st.warning(f"Recomendação para {selected_symbol}: **{latest_signal}**")

# Seção: Análise de Potencial
elif section == "Análise de Potencial":
    st.title("Análise de Potencial Projetado")
    st.write("Identifique as ações com o maior potencial de compra com base nos sinais mais recentes.")

    # Filtro por intervalo de datas
    min_date = df['Date'].min()
    max_date = df['Date'].max()

    # Input para selecionar intervalo de datas
    start_date = st.date_input("Data de Início:", min_value=min_date.date(), max_value=max_date.date(), value=min_date.date())
    end_date = st.date_input("Data de Fim:", min_value=min_date.date(), max_value=max_date.date(), value=max_date.date())

    # Filtrar dados com base nas datas selecionadas
    performance_df = df[(df['Date'] >= pd.to_datetime(start_date)) & (df['Date'] <= pd.to_datetime(end_date))]

    # Análise das Melhores Ações com base em Retorno Acumulado
    performance_summary = performance_df.groupby('Symbol')['Return'].sum().reset_index()
    performance_summary['Retorno Acumulado (%)'] = performance_summary['Return'] * 100
    performance_summary = performance_summary.sort_values(by='Retorno Acumulado (%)', ascending=False)

    # Exibir as melhores ações
    st.subheader("Ações com Melhor Desempenho:")
    st.write(performance_summary.head(5))

    # Análise de Potencial com base nos sinais de 'Buy'
    recent_signals = performance_df[performance_df['Date'] == performance_df['Date'].max()]

    # Exibir as ações com sinal de 'Buy'
    potential_stocks = recent_signals[recent_signals['Signal'] == 'Buy']
    st.subheader("Ações com Potencial de Compra:") 
    st.write(potential_stocks[['Symbol', 'Close', 'SMA_21', 'SMA_50', 'Signal']])
