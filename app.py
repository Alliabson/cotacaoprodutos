import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import time

# Configuração da página
st.set_page_config(
    page_title="Cotações Agropecuárias",
    page_icon="🌱",
    layout="wide"
)

# Título do aplicativo
st.title("🌱 Cotações Agropecuárias")
st.markdown("Acompanhe as cotações de produtos agropecuários")

# APIs disponíveis
API_OPTIONS = {
    "CEPEA - Boi Gordo": {
        "url": "https://www.cepea.esalq.usp.br/br/indicador/series/boi-gordo.aspx",
        "codigo": "boi-gordo"
    },
    "IPEAData - Soja": {
        "url": "http://www.ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO='PPM12_SOJA12')",
        "codigo": "PPM12_SOJA12"
    },
    "Banco Central - Café": {
        "url": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.7461/dados",
        "codigo": "7461"
    }
}

# Sidebar
with st.sidebar:
    st.header("Configurações")
    api_selecionada = st.selectbox(
        "Fonte de dados",
        list(API_OPTIONS.keys())
    )
    
    dias_historico = st.slider(
        "Período (dias)",
        min_value=7,
        max_value=365,
        value=30
    )

# Funções para cada API
def fetch_cepea_data(codigo, dias):
    """Busca dados do CEPEA (requer scraping ou verificar API real)"""
    # Exemplo simplificado - na prática precisa de tratamento específico
    try:
        url = f"https://www.cepea.esalq.usp.br/br/indicador/ajax/{codigo}.aspx"
        params = {
            "dias": dias,
            "tipo": "json"  # verificar parâmetros reais
        }
        response = requests.get(url, params=params, timeout=10)
        return response.json()
    except Exception as e:
        st.error(f"Erro ao acessar CEPEA: {str(e)}")
        return None

def fetch_ipeadata(codigo):
    """Busca dados do IPEAData"""
    try:
        url = f"http://www.ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO='{codigo}')"
        response = requests.get(url, timeout=10)
        data = response.json()
        return pd.DataFrame(data['value'])
    except Exception as e:
        st.error(f"Erro ao acessar IPEAData: {str(e)}")
        return pd.DataFrame()

def fetch_bcb_data(codigo, dias):
    """Busca dados do Banco Central"""
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=dias)
        
        url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"
        params = {
            "formato": "json",
            "dataInicial": start_date.strftime("%d/%m/%Y"),
            "dataFinal": end_date.strftime("%d/%m/%Y")
        }
        response = requests.get(url, params=params, timeout=10)
        return pd.DataFrame(response.json())
    except Exception as e:
        st.error(f"Erro ao acessar BCB: {str(e)}")
        return pd.DataFrame()

# Processamento dos dados
def process_data(df, fonte):
    """Processa os dados conforme a fonte"""
    if fonte == "CEPEA":
        df['data'] = pd.to_datetime(df['data'])
        df['preco'] = pd.to_numeric(df['valor'])
    elif fonte == "IPEAData":
        df['data'] = pd.to_datetime(df['VALDATA'])
        df['preco'] = pd.to_numeric(df['VALVALOR'])
    elif fonte == "Banco Central":
        df['data'] = pd.to_datetime(df['data'], dayfirst=True)
        df['preco'] = pd.to_numeric(df['valor'])
    
    return df.sort_values('data').dropna()

# Interface principal
def main():
    st.write(f"Fonte selecionada: **{api_selecionada}**")
    
    with st.spinner("Carregando dados..."):
        try:
            # Seleciona a API
            api_info = API_OPTIONS[api_selecionada]
            
            if "CEPEA" in api_selecionada:
                dados = fetch_cepea_data(api_info['codigo'], dias_historico)
                df = process_data(pd.DataFrame(dados), "CEPEA")
            elif "IPEAData" in api_selecionada:
                df = fetch_ipeadata(api_info['codigo'])
                df = process_data(df, "IPEAData")
            elif "Banco Central" in api_selecionada:
                df = fetch_bcb_data(api_info['codigo'], dias_historico)
                df = process_data(df, "Banco Central")
            
            if df.empty:
                st.warning("Não foram encontrados dados para o período selecionado.")
                return
            
            # Mostra métricas
            ultimo = df.iloc[-1]
            cols = st.columns(3)
            cols[0].metric("Último Preço", f"R$ {ultimo['preco']:.2f}")
            cols[1].metric("Data", ultimo['data'].strftime("%d/%m/%Y"))
            
            # Gráfico nativo do Streamlit
            st.subheader("Evolução de Preços")
            st.line_chart(df.set_index('data')['preco'])
            
            # Dados brutos
            st.subheader("Dados Completos")
            st.dataframe(df.sort_values('data', ascending=False))
            
        except Exception as e:
            st.error(f"Erro: {str(e)}")

if __name__ == "__main__":
    main()
