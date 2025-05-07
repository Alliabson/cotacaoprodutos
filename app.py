import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import json

# Configuração da página
st.set_page_config(
    page_title="Cotações Agropecuárias - Dados Oficiais",
    page_icon="🌱",
    layout="wide"
)

# Título do aplicativo
st.title("🌱 Cotações Agropecuárias Oficiais")
st.markdown("Dados em tempo real das principais fontes governamentais")

# Dicionário de APIs e produtos
API_PRODUTOS = {
    "CEPEA": {
        "Boi Gordo": {"endpoint": "boi-gordo", "unidade": "R$/@", "fonte": "CEPEA/ESALQ"},
        "Bezerro": {"endpoint": "bezerro", "unidade": "R$/cabeça", "fonte": "CEPEA/ESALQ"},
        "Milho": {"endpoint": "milho", "unidade": "R$/sc60kg", "fonte": "CEPEA/ESALQ"},
        "Soja": {"endpoint": "soja", "unidade": "R$/sc60kg", "fonte": "CEPEA/ESALQ"}
    },
    "IPEAData": {
        "Soja (Paraná)": {"codigo": "PPM12_SOJA12", "unidade": "US$/sc60kg", "fonte": "IPEAData"},
        "Café Arábica": {"codigo": "PPM12_CAFE12", "unidade": "US$/sc60kg", "fonte": "IPEAData"},
        "Boi Gordo (SP)": {"codigo": "PPM12_BOI12", "unidade": "R$/@", "fonte": "IPEAData"}
    },
    "Banco Central": {
        "Café Arábica (BCB)": {"codigo": "7461", "unidade": "US$/sc60kg", "fonte": "BCB"},
        "Boi Gordo (BCB)": {"codigo": "1", "unidade": "R$/@", "fonte": "BCB"},
        "Soja (BCB)": {"codigo": "2", "unidade": "US$/sc60kg", "fonte": "BCB"}
    }
}

# Sidebar
with st.sidebar:
    st.header("Filtros")
    
    fonte_selecionada = st.selectbox(
        "Selecione a fonte",
        list(API_PRODUTOS.keys())
    )
    
    produto_selecionado = st.selectbox(
        "Selecione o produto",
        list(API_PRODUTOS[fonte_selecionada].keys())
    )
    
    dias_historico = st.slider(
        "Período (dias)",
        min_value=7,
        max_value=365,
        value=30
    )

# Funções para buscar dados
def buscar_cepea(endpoint, dias):
    """Busca dados do CEPEA via API alternativa"""
    try:
        # Esta é uma URL alternativa que encontrei para o CEPEA
        url = f"https://www.cepea.esalq.usp.br/wp-admin/admin-ajax.php?action=ajax_indicador&tipo={endpoint}&periodo={dias}"

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        dados = response.json()
        
        if not dados or 'data' not in dados:
            return pd.DataFrame()
            
        df = pd.DataFrame({
            'data': pd.to_datetime(dados['data']),
            'preco': pd.to_numeric(dados['valor'])
        })
        
        return df.dropna()
        
    except Exception as e:
        st.error(f"Erro ao buscar dados do CEPEA: {str(e)}")
        return pd.DataFrame()

def buscar_ipeadata(codigo):
    """Busca dados do IPEAData"""
    try:
        url = f"http://www.ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO='{codigo}')"
        
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        
        dados = response.json()
        
        if not dados or 'value' not in dados:
            return pd.DataFrame()
            
        df = pd.DataFrame(dados['value'])
        
        if len(df) == 0:
            return pd.DataFrame()
            
        df = df.rename(columns={
            'VALDATA': 'data',
            'VALVALOR': 'preco'
        })
        
        df['data'] = pd.to_datetime(df['data'])
        df['preco'] = pd.to_numeric(df['preco'])
        
        return df.dropna()
        
    except Exception as e:
        st.error(f"Erro ao buscar dados do IPEAData: {str(e)}")
        return pd.DataFrame()

def buscar_bcb(codigo, dias):
    """Busca dados do Banco Central"""
    try:
        data_final = datetime.now()
        data_inicial = data_final - timedelta(days=dias)
        
        url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"
        params = {
            'formato': 'json',
            'dataInicial': data_inicial.strftime('%d/%m/%Y'),
            'dataFinal': data_final.strftime('%d/%m/%Y')
        }
        
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        
        dados = response.json()
        
        if not dados:
            return pd.DataFrame()
            
        df = pd.DataFrame(dados)
        
        df = df.rename(columns={
            'data': 'data',
            'valor': 'preco'
        })
        
        df['data'] = pd.to_datetime(df['data'], dayfirst=True)
        df['preco'] = pd.to_numeric(df['preco'])
        
        return df.dropna()
        
    except Exception as e:
        st.error(f"Erro ao buscar dados do BCB: {str(e)}")
        return pd.DataFrame()

# Interface principal
def main():
    produto_info = API_PRODUTOS[fonte_selecionada][produto_selecionado]
    
    st.header(f"{produto_selecionado} ({produto_info['unidade']})")
    st.caption(f"Fonte: {produto_info['fonte']}")
    
    with st.spinner(f"Buscando dados de {produto_selecionado}..."):
        try:
            # Seleciona a fonte de dados
            if fonte_selecionada == "CEPEA":
                df = buscar_cepea(produto_info['endpoint'], dias_historico)
            elif fonte_selecionada == "IPEAData":
                df = buscar_ipeadata(produto_info['codigo'])
                # Filtra pelos dias solicitados
                if not df.empty:
                    data_corte = datetime.now() - timedelta(days=dias_historico)
                    df = df[df['data'] >= data_corte]
            elif fonte_selecionada == "Banco Central":
                df = buscar_bcb(produto_info['codigo'], dias_historico)
            
            if df.empty:
                st.warning("Não foram encontrados dados para o período selecionado.")
                return
            
            # Mostra métricas
            ultimo = df.iloc[-1]
            cols = st.columns(3)
            cols[0].metric("Último Preço", f"R$ {ultimo['preco']:.2f}" if "R$" in produto_info['unidade'] else f"US$ {ultimo['preco']:.2f}")
            cols[1].metric("Data", ultimo['data'].strftime("%d/%m/%Y"))
            variacao = ((ultimo['preco'] - df.iloc[-2]['preco']) / df.iloc[-2]['preco']) * 100 if len(df) > 1 else 0
            cols[2].metric("Variação", f"{variacao:.2f}%", delta=f"{variacao:.2f}%")
            
            # Gráfico
            st.subheader("Evolução de Preços")
            st.line_chart(df.set_index('data')['preco'])
            
            # Dados completos
            st.subheader("Histórico Completo")
            st.dataframe(
                df.sort_values('data', ascending=False).assign(
                    data=lambda x: x['data'].dt.strftime('%d/%m/%Y')
                ).style.format({'preco': '{:.2f}'}),
                use_container_width=True,
                height=300
            )
            
            # Botão para download
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"cotacao_{produto_selecionado.lower().replace(' ', '_')}.csv",
                mime='text/csv'
            )
            
        except Exception as e:
            st.error(f"Erro inesperado: {str(e)}")
            st.exception(e)

if __name__ == "__main__":
    main()
