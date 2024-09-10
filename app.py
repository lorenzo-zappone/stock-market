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

# Seção: Backtest e Simulação de Retorno
elif section == "Análise de Potencial":
    
    st.title("Análise de Potencial")
    st.write("Simule o percentual de acerto e o retorno financeiro.")

    # Filtro por símbolo de ação
    symbols = df['Symbol'].unique()
    selected_symbol = st.selectbox("Selecione um símbolo de ação para o backtest:", symbols)

    # Filtrar dados com base no símbolo selecionado
    filtered_df = df[df['Symbol'] == selected_symbol].sort_values('Date')

    # Filtro de período
    st.subheader("Selecione o Período para Análise")
    
    # Filtro por intervalo de datas
    min_date = filtered_df['Date'].min()
    max_date = filtered_df['Date'].max()

    # Input para selecionar intervalo de datas
    start_date = st.date_input("Data de Início:", min_value=min_date.date(), max_value=max_date.date(), value=min_date.date())
    end_date = st.date_input("Data de Fim:", min_value=min_date.date(), max_value=max_date.date(), value=max_date.date())

    # Aplicar filtro de período ao DataFrame filtrado por símbolo
    filtered_df = filtered_df[(filtered_df['Date'] >= pd.to_datetime(start_date)) & 
                              (filtered_df['Date'] <= pd.to_datetime(end_date))]

    # Converter colunas para numérico
    filtered_df['High'] = pd.to_numeric(filtered_df['High'], errors='coerce')
    filtered_df['Low'] = pd.to_numeric(filtered_df['Low'], errors='coerce')
    filtered_df['Close'] = pd.to_numeric(filtered_df['Close'], errors='coerce')
    filtered_df['ATR'] = pd.to_numeric(filtered_df['ATR'], errors='coerce')

    # Carregar o diretório Parquet
    # Carregar os multiplicadores otimizados
    # Carregar o diretório Parquet
    df_opt = pd.read_parquet('data/best_atr_multipliers.parquet')

    # Obter os multiplicadores otimizados para o símbolo selecionado
    opt_multipliers = df_opt[df_opt['Symbol'] == selected_symbol].iloc[0]
    default_target_mult = opt_multipliers['Target_Mult']
    default_stop_loss_mult = opt_multipliers['Stop_Loss_Mult']

    # Parâmetros ajustáveis pelo usuário
    st.subheader("Parâmetros de Backtest")
    col1, col2 = st.columns([1, 1])
    with col1:
#        st.subheader("Selecionar Parâmetros")
        target_multiplier = st.slider("Multiplicador do Alvo (ATR)", 1.0, 10.0, float(default_target_mult), 0.1)
        stop_loss_multiplier = st.slider("Multiplicador do Stop Loss (ATR)", 0.5, 5.0, float(default_stop_loss_mult), 0.1)

#    with col2:
#        st.subheader("Multiplicadores Otimizados")
#        st.write(df_opt)

        # Identificar blocos contínuos de sinais de compra e venda
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
    col3.metric("Fator de Lucro", f"{profit_factor:.2f}")

    col1, col2, col3 = st.columns(3)
    col1.metric("Ganho Médio", f"{avg_win:.2%}")
    col2.metric("Perda Média", f"{avg_loss:.2%}")
    col3.metric("Retorno Total do Setup", f"{total_return:.2%}")

    col1, col2 = st.columns(2)
    col1.metric("Retorno Médio por Trade", f"{(total_return / total_trades):.2%}" if total_trades > 0 else "N/A")
    
    # Gráfico Interativo dos Trades
    st.subheader("Visualização Gráfica dos Trades")

    fig = go.Figure()

    # Adiciona a linha de preços
    fig.add_trace(go.Scatter(
        x=filtered_df['Date'],
        y=filtered_df['Close'],
        mode='lines',
        name='Preço Fechamento'
    ))

    # Adiciona as Médias Móveis
    fig.add_trace(go.Scatter(
        x=filtered_df['Date'],
        y=filtered_df['SMA_21'],
        mode='lines',
        name='SMA 21'
    ))

    fig.add_trace(go.Scatter(
        x=filtered_df['Date'],
        y=filtered_df['SMA_50'],
        mode='lines',
        name='SMA 50'
    ))

    # Pontos de Entrada (Sinal de Compra)
    entry_points = filtered_df[filtered_df['Signal'] == 'Buy']

    # Marcar os pontos de entrada (sinal de compra)
    fig.add_trace(go.Scatter(
        x=entry_points['Date'],
        y=entry_points['Close'],
        mode='markers',
        marker=dict(color='blue', size=8, symbol='circle'),
        name='Entrada (Buy)'
    ))

    # Pontos de Saída de Sucesso (Acertos)
    exits_success = filtered_df[filtered_df['Resultado'] == 1]

    # Marcar os pontos de saída de sucesso
    fig.add_trace(go.Scatter(
        x=exits_success['Date'],
        y=exits_success['Close'],
        mode='markers',
        marker=dict(color='green', size=8, symbol='triangle-up'),
        name='Saída (Sucesso)'
    ))

    # Pontos de Saída de Falha (Erros)
    exits_failure = filtered_df[filtered_df['Resultado'] == -1]

    # Marcar os pontos de saída de falha
    fig.add_trace(go.Scatter(
        x=exits_failure['Date'],
        y=exits_failure['Close'],
        mode='markers',
        marker=dict(color='red', size=8, symbol='triangle-down'),
        name='Saída (Falha)'
    ))

    fig.update_layout(
        title=f"Análise de {selected_symbol} com Configuração de Setup",
        xaxis_title="Data",
        yaxis_title="Preço",
        legend_title="Elementos"
    )

    # Exibir gráfico interativo
    st.plotly_chart(fig, use_container_width=True)
    
    # Adicionar gráficos de ATR e RSI
    st.subheader("Indicadores Técnicos")
    
    # Gráfico do ATR
    fig_atr = go.Figure()
    fig_atr.add_trace(go.Scatter(x=filtered_df['Date'], y=filtered_df['ATR'], mode='lines', name='ATR'))
    fig_atr.update_layout(title='Average True Range (ATR)', xaxis_title='Data', yaxis_title='ATR')
    st.plotly_chart(fig_atr)

    # Gráfico do RSI
    fig_rsi = go.Figure()
    fig_rsi.add_trace(go.Scatter(x=filtered_df['Date'], y=filtered_df['RSI'], mode='lines', name='RSI'))
    fig_rsi.add_hline(y=70, line_dash="dash", line_color="red", annotation_text="Sobrecomprado")
    fig_rsi.add_hline(y=30, line_dash="dash", line_color="green", annotation_text="Sobrevendido")
    fig_rsi.update_layout(title='Relative Strength Index (RSI)', xaxis_title='Data', yaxis_title='RSI')
    st.plotly_chart(fig_rsi)