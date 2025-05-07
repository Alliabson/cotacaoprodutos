import streamlit as st
import pandas as pd
import plotly.express as px
from utils.api_client import fetch_agro_data
from utils.data_processing import process_quotes_data
from datetime import datetime, timedelta

# Configuração da página
st.set_page_config(
    page_title="Cotações Agropecuárias",
    page_icon="🌱",
    layout="wide"
)

# Título do aplicativo
st.title("🌱 Cotações Agropecuárias")
st.markdown("Acompanhe as cotações de produtos agropecuários em tempo real.")

# Sidebar com filtros
with st.sidebar:
    st.header("Filtros")
    produto = st.selectbox(
        "Selecione o produto",
        ["Boi Gordo", "Bezerro", "Milho", "Soja", "Café", "Feijão"],
        index=0
    )
    
    dias_historico = st.slider(
        "Histórico (dias)",
        min_value=1,
        max_value=90,
        value=30
    )

# Mapeamento de produtos para códigos das APIs
PRODUTOS_API = {
    "Boi Gordo": "boi-gordo",
    "Bezerro": "bezerro",
    "Milho": "milho",
    "Soja": "soja",
    "Café": "cafe",
    "Feijão": "feijao"
}

# Função principal
def main():
    # Exibe spinner enquanto carrega os dados
    with st.spinner(f"Carregando cotações para {produto}..."):
        try:
            # Obtém dados da API
            data = fetch_agro_data(
                produto=PRODUTOS_API[produto],
                dias_historico=dias_historico
            )
            
            # Processa os dados
            df = process_quotes_data(data)
            
            if df.empty:
                st.warning("Não foram encontrados dados para o período selecionado.")
                return
            
            # Exibe métricas principais
            ultima_cotacao = df.iloc[-1]
            col1, col2, col3 = st.columns(3)
            col1.metric("Último Preço", f"R$ {ultima_cotacao['preco']:.2f}")
            col2.metric("Variação Dia", f"{ultima_cotacao['variacao_dia']:.2f}%",
                       delta=f"{ultima_cotacao['variacao_dia']:.2f}%")
            col3.metric("Data", ultima_cotacao['data'].strftime("%d/%m/%Y"))
            
            # Gráfico de linha com histórico
            fig = px.line(
                df,
                x="data",
                y="preco",
                title=f"Histórico de Cotações - {produto}",
                labels={"preco": "Preço (R$)", "data": "Data"},
                markers=True
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Tabela com dados detalhados
            st.subheader("Dados Detalhados")
            st.dataframe(
                df.sort_values("data", ascending=False),
                use_container_width=True,
                hide_index=True
            )
            
        except Exception as e:
            st.error(f"Erro ao carregar dados: {str(e)}")

if __name__ == "__main__":
    main()
