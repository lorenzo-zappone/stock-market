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

# Defina uma lista com os nomes das colunas que precisam ser convertidas para tipos numéricos
numeric_columns = ['Close', 'SMA_21', 'SMA_80', 'High', 'Low', 'ATR']

# Utilize um loop para converter as colunas para numérico, lidando com erros
for column in numeric_columns:
    df[column] = pd.to_numeric(df[column], errors='coerce')

# Converta a coluna 'Date' para datetime
df['Date'] = pd.to_datetime(df['Date'], errors='coerce')  # Adicione errors='coerce' para evitar falhas

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

    # Seleção do símbolo de ação
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

    # Simulação de acertos
    st.subheader(f"Simulação de Acertos - {selected_symbol}")
    acertos = filtered_df['Signal'].value_counts(normalize=True) * 100
    st.write(acertos)

    # Gráfico de Sinais
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

    # Parâmetros ajustáveis pelo usuário para Backtest
    st.subheader("Parâmetros de Backtest")
    col1, col2 = st.columns(2)
    
    with col1:
        target_multiplier = st.slider("Multiplicador do Alvo (ATR)", 1.0, 10.0, 10.0, 0.1)
    with col2:
        stop_loss_multiplier = st.slider("Multiplicador do Stop Loss (ATR)", 0.5, 5.0, 5.0, 0.1)

    # Cálculo do Backtest
    filtered_df['Signal_Change'] = filtered_df['Signal'] != filtered_df['Signal'].shift(1)
    filtered_df['Block_Num'] = filtered_df['Signal_Change'].cumsum()
    filtered_df['Buy_Price'] = filtered_df.apply(lambda row: row['Close'] if row['Signal'] == 'Buy' else None, axis=1)
    filtered_df['Sell_Price'] = filtered_df.apply(lambda row: row['Close'] if row['Signal'] == 'Sell' else None, axis=1)

    # Manter o primeiro sinal de compra e o último sinal de venda em cada bloco
    filtered_df['Buy_Price'] = filtered_df.groupby('Block_Num')['Buy_Price'].transform('first')
    filtered_df['Sell_Price'] = filtered_df.groupby('Block_Num')['Sell_Price'].transform('last')

    # Calcular o retorno baseado no bloco
    filtered_df['Resultado'] = 0
    filtered_df['Retorno Setup'] = 0.0
    filtered_df['Preço Entrada'] = 0.0

    for i in range(len(filtered_df) - 1):  # Não considerar o último dia
        if filtered_df.iloc[i]['Signal'] == 'Buy':
            entry_price = filtered_df.iloc[i]['Close']
            filtered_df.at[filtered_df.index[i], 'Preço Entrada'] = entry_price
            target = entry_price * (1 + target_multiplier * filtered_df.iloc[i]['ATR'] / entry_price)
            stop_loss = entry_price * (1 - stop_loss_multiplier * filtered_df.iloc[i]['ATR'] / entry_price)

            for j in range(i+1, len(filtered_df)):
                if filtered_df.iloc[j]['High'] >= target:
                    filtered_df.at[filtered_df.index[j], 'Resultado'] = 1  # Acerto
                    filtered_df.at[filtered_df.index[j], 'Retorno Setup'] = (target - entry_price) / entry_price
                    break
                elif filtered_df.iloc[j]['Low'] <= stop_loss:
                    filtered_df.at[filtered_df.index[j], 'Resultado'] = -1  # Erro
                    filtered_df.at[filtered_df.index[j], 'Retorno Setup'] = (stop_loss - entry_price) / entry_price
                    break
                elif j == len(filtered_df) - 1:
                    # Se chegou ao final sem atingir target ou stop, considera o último preço
                    filtered_df.at[filtered_df.index[j], 'Resultado'] = 0  # Trade aberto
                    filtered_df.at[filtered_df.index[j], 'Retorno Setup'] = (filtered_df.iloc[j]['Close'] - entry_price) / entry_price

    # Cálculo de métricas para o setup ajustado
    total_trades = filtered_df['Resultado'].abs().sum()
    win_trades = filtered_df[filtered_df['Resultado'] == 1].shape[0]
    loss_trades = filtered_df[filtered_df['Resultado'] == -1].shape[0]
    open_trades = filtered_df[filtered_df['Resultado'] == 0].shape[0]
    win_rate = (win_trades / total_trades) * 100 if total_trades > 0 else 0
    avg_win = filtered_df[filtered_df['Resultado'] == 1]['Retorno Setup'].mean()
    avg_loss = filtered_df[filtered_df['Resultado'] == -1]['Retorno Setup'].mean()
    profit_factor = abs(avg_win * win_trades / (avg_loss * loss_trades)) if loss_trades > 0 else float('inf')
    total_return = filtered_df['Retorno Setup'].sum()  # Retorno total do setup

    # Exibição dos resultados
    st.subheader(f"Resultados do Backtest para {selected_symbol}")
    col1, col2, col3 = st.columns(3)
    col1.metric("Total de Trades", total_trades)
    col2.metric("Taxa de Acerto", f"{win_rate:.2f}%")
    col3.metric("Retorno Médio por Trade", f"{(total_return / total_trades):.2%}" if total_trades > 0 else "N/A")

    col1, col2, col3 = st.columns(3)
    col1.metric("Ganho Médio", f"{avg_win:.2%}")
    col2.metric("Perda Média", f"{avg_loss:.2%}")
    col3.metric("Retorno Total do Setup", f"{total_return:.2%}")

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
    st.dataframe(buy_signals[['Date', 'Symbol', 'Close']].head(10), use_container_width=True, hide_index=True)

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
