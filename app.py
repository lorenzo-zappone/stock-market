import streamlit as st
import pandas as pd
import pyarrow.parquet as pq
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os

# Configurar o layout da página
st.set_page_config(page_title="NASDAQ Analysis", layout="wide")

# Carregar e ler dados Parquet
@st.cache_data
def load_parquet(parquet_dir):
    table = pq.ParquetDataset(parquet_dir).read()
    return table.to_pandas()

parquet_dir = 'data/dave_landry_analysis.parquet'
df = load_parquet(parquet_dir)

# Contagem total de registros
total_records = df.shape[0]

# Calcular o tamanho dos arquivos Parquet em MB
@st.cache_data
def calculate_file_size(parquet_dir):
    return sum(os.path.getsize(os.path.join(parquet_dir, f)) for f in os.listdir(parquet_dir)) / (1024 * 1024)

file_size_mb = calculate_file_size(parquet_dir)

# Converter colunas para tipos numéricos e garantir que a coluna Date é datetime
df['Close'] = pd.to_numeric(df['Close'], errors='coerce')
df['SMA_21'] = pd.to_numeric(df['SMA_21'], errors='coerce')
df['SMA_80'] = pd.to_numeric(df['SMA_80'], errors='coerce')
df['Date'] = pd.to_datetime(df['Date'])

# Calcular retornos diários
df['Return'] = df.groupby('Symbol')['Close'].pct_change()

# Sidebar para navegação
st.sidebar.title("Menu")
section = st.sidebar.radio("Ir para", ["Visão Geral", "Análise de Ações", "Análise de Potencial", "Entradas Recentes"])

# Seção: Visão Geral
if section == "Visão Geral":
    st.title("NASDAQ Analysis Results")
    st.write("Nesta seção, você encontra uma visão geral dos dados de análise.")

    # Exibir contagem total de registros e tamanho do arquivo
    st.header("Resumo dos Dados")
    col1, col2 = st.columns(2)
    col1.metric("Total de Registros", total_records)
    col2.metric("Tamanho Total do Arquivo (MB)", f"{file_size_mb:.2f}")

    # Insights Adicionais com Representações Visuais
    st.header("Insights Adicionais")
    col1, col2, col3 = st.columns(3)

    # 1. Melhor Desempenho
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

    # 2. Pior Desempenho
    worst_stock = best_performance.loc[best_performance['Retorno Acumulado (%)'].idxmin()]

    fig_worst = go.Figure(go.Indicator(
        mode="gauge+number",
        value=worst_stock['Retorno Acumulado (%)'],
        title={'text': f"Pior Desempenho: {worst_stock['Symbol']}"},
        gauge={'axis': {'range': [min(best_performance['Retorno Acumulado (%)']) * 1.1, 0]},
               'bar': {'color': "red"}}
    ))
    col2.plotly_chart(fig_worst, use_container_width=True)

    # 3. Volatilidade Média
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

    fig_most_buys = go.Figure(go.Indicator(
        mode="gauge+number",
        value=buy_signals['Symbol'].value_counts().max(),
        title={'text': f"Maior Frequência de Compra: {most_buy_signals}"},
        gauge={'axis': {'range': [0, buy_signals['Symbol'].value_counts().max() * 1.1]},
               'bar': {'color': "blue"}}
    ))
    st.plotly_chart(fig_most_buys, use_container_width=True)

    # 5. Distribuição dos Sinais
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

    symbols = df['Symbol'].unique()
    selected_symbol = st.selectbox("Selecione um símbolo de ação:", symbols)

    # Filtrar dados com base no símbolo selecionado
    filtered_df = df[df['Symbol'] == selected_symbol]

    # Filtro por intervalo de datas
    min_date = filtered_df['Date'].min()
    max_date = filtered_df['Date'].max()

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
            'SMA_80': 'last',
            'Return': 'sum',
            'Signal': 'last'
        }).reset_index().sort_values(by='Date', ascending=False)
    elif timeframe == 'Mensal':
        filtered_df = filtered_df.set_index('Date').resample('M').agg({
            'Close': 'last',
            'SMA_21': 'last',
            'SMA_80': 'last',
            'Return': 'sum',
            'Signal': 'last'
        }).reset_index().sort_values(by='Date', ascending=False)

    # Exibir o DataFrame filtrado
    st.write(f"Dados para {selected_symbol} ({timeframe})")
    st.dataframe(filtered_df, use_container_width=True, hide_index=True)

    # Plotar o preço de fechamento com SMA_21 e SMA_80
    fig = px.line(filtered_df, x='Date', y=['Close', 'SMA_21', 'SMA_80'], 
                  labels={'value': 'Preço', 'variable': 'Indicador'},
                  title=f"Preço e Médias Móveis para {selected_symbol} ({timeframe})")
    st.plotly_chart(fig)

# Seção: Backtest e Simulação de Retorno
elif section == "Análise de Potencial":
    st.title("Análise de Potencial")
    st.write("Simule o percentual de acerto e o retorno financeiro.")

    symbols = df['Symbol'].unique()
    selected_symbol = st.selectbox("Selecione um símbolo de ação para o backtest:", symbols)

    # Filtrar dados com base no símbolo selecionado
    filtered_df = df[df['Symbol'] == selected_symbol].sort_values('Date')

    # Filtro de período
    min_date = filtered_df['Date'].min()
    max_date = filtered_df['Date'].max()

    start_date = st.date_input("Data de Início:", min_value=min_date.date(), max_value=max_date.date(), value=min_date.date())
    end_date = st.date_input("Data de Fim:", min_value=min_date.date(), max_value=max_date.date(), value=max_date.date())

    # Filtrar os dados pelo intervalo de datas selecionado
    filtered_df = filtered_df[(filtered_df['Date'] >= pd.to_datetime(start_date)) & (filtered_df['Date'] <= pd.to_datetime(end_date))]

    # Simulação de acerto
    st.subheader(f"Simulação de Acertos - {selected_symbol}")
    acertos = filtered_df['Signal'].value_counts(normalize=True) * 100
    st.write(acertos)

    # Plotar sinais 
    st.subheader("Gráfico de Sinais")
    signal_color_map = {'Buy': 'green', 'Sell': 'red'}

    fig = px.scatter(
        filtered_df, 
        x='Date', 
        y='Close', 
        color='Signal',
        color_discrete_map=signal_color_map,  # Mapeamento de cores personalizado
        labels={'Close': 'Preço de Fechamento'}, 
        title=f"Sinais de Compra e Venda - {selected_symbol}"
    )
    st.plotly_chart(fig, use_container_width=True)

# Seção: Entradas Recentes
elif section == "Entradas Recentes":
    st.title("Entradas Recentes")
    st.write("Esta seção mostra os sinais de compra mais recentes nos últimos 15 dias.")

    # Definir a data limite para os últimos 15 dias
    end_date = datetime.now()
    start_date = end_date - timedelta(days=15)

    # Filtrar sinais de compra nos últimos 15 dias
    buy_signals = df[(df['Signal'] == 'Buy') & (df['Date'] >= start_date)].sort_values(by='Date', ascending=False)

    # Exibir tabela com os sinais de compra recentes
    st.write("Sinais de Compra Recentes")
    st.dataframe(buy_signals[['Date', 'Symbol', 'Close']], use_container_width=True, hide_index=True)

    # Criar gráfico para sinais de compra
    if not buy_signals.empty:
        st.subheader("Gráfico de Sinais de Compra Recentes")
        
        # Plotar um gráfico de barras para mostrar os preços de fechamento das entradas
        fig_buy = px.bar(
            buy_signals, 
            x='Date', 
            y='Close', 
            color='Symbol', 
            title="Preços de Fechamento para Sinais de Compra Recentes",
            labels={'Close': 'Preço de Fechamento'},
            color_discrete_sequence=px.colors.sequential.Blues
        )
        
        # Exibir o gráfico
        st.plotly_chart(fig_buy, use_container_width=True)
    else:
        st.write("Nenhum sinal de compra recente encontrado nos últimos 15 dias.")
