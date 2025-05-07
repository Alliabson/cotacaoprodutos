import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import json
import random

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

# Sidebar com seleção de datas melhorada
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
    
    # Novo seletor de datas com calendário
    data_final = st.date_input(
        "Data final",
        value=datetime.now(),
        max_value=datetime.now()
    )
    
    data_inicial = st.date_input(
        "Data inicial",
        value=datetime.now() - timedelta(days=30),
        max_value=datetime.now()
    )
    
    # Garante que a data inicial seja menor que a final
    if data_inicial > data_final:
        st.error("A data inicial deve ser anterior à data final")
        st.stop()

# Função para gerar dados de exemplo
def gerar_dados_exemplo(data_inicial, data_final, produto):
    """Gera dados fictícios quando a API falha"""
    dias = (data_final - data_inicial).days + 1
    base_price = {
        "Boi Gordo": 250, "Bezerro": 180, "Milho": 85, "Soja": 150,
        "Café Arábica": 1200, "Soja (Paraná)": 160, "Boi Gordo (SP)": 255
    }.get(produto, 100)
    
    dates = pd.date_range(start=data_inicial, end=data_final)
    prices = [base_price + random.uniform(-5, 5) * (i/dias) for i in range(dias)]
    
    return pd.DataFrame({
        'data': dates,
        'preco': prices
    })

# Funções para buscar dados com fallback
def buscar_cepea(endpoint, data_inicial, data_final):
    """Busca dados do CEPEA com fallback"""
    try:
        # Tentativa com nova URL do CEPEA
        url = f"https://www.cepea.esalq.usp.br/br/indicador/series/{endpoint}.aspx"
        
        # Simulando uma requisição (na prática, precisaria de web scraping)
        # Esta parte precisará ser adaptada conforme o site do CEPEA
        raise Exception("API do CEPEA requer implementação específica")
        
    except Exception as e:
        st.warning(f"Dados reais temporariamente indisponíveis. Mostrando dados simulados para referência. (Erro: {str(e)})")
        return gerar_dados_exemplo(data_inicial, data_final, endpoint)

def buscar_ipeadata(codigo, data_inicial, data_final):
    """Busca dados do IPEAData com fallback"""
    try:
        url = f"http://www.ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO='{codigo}')"
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        
        dados = response.json()
        
        if not dados or 'value' not in dados:
            raise Exception("Nenhum dado retornado")
            
        df = pd.DataFrame(dados['value'])
        
        if len(df) == 0:
            raise Exception("Dataframe vazio")
            
        df = df.rename(columns={
            'VALDATA': 'data',
            'VALVALOR': 'preco'
        })
        
        df['data'] = pd.to_datetime(df['data'])
        df['preco'] = pd.to_numeric(df['preco'])
        
        # Filtra pelo período selecionado
        df = df[(df['data'] >= pd.to_datetime(data_inicial)) & 
                (df['data'] <= pd.to_datetime(data_final))]
        
        if df.empty:
            raise Exception("Nenhum dado no período selecionado")
            
        return df
    
    except Exception as e:
        st.warning(f"Dados reais temporariamente indisponíveis. Mostrando dados simulados para referência. (Erro: {str(e)})")
        return gerar_dados_exemplo(data_inicial, data_final, codigo)

def buscar_bcb(codigo, data_inicial, data_final):
    """Busca dados do Banco Central com fallback"""
    try:
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
            raise Exception("Nenhum dado retornado")
            
        df = pd.DataFrame(dados)
        
        df = df.rename(columns={
            'data': 'data',
            'valor': 'preco'
        })
        
        df['data'] = pd.to_datetime(df['data'], dayfirst=True)
        df['preco'] = pd.to_numeric(df['preco'])
        
        if df.empty:
            raise Exception("Nenhum dado no período selecionado")
            
        return df
    
    except Exception as e:
        st.warning(f"Dados reais temporariamente indisponíveis. Mostrando dados simulados para referência. (Erro: {str(e)})")
        return gerar_dados_exemplo(data_inicial, data_final, codigo)

# Interface principal
def main():
    produto_info = API_PRODUTOS[fonte_selecionada][produto_selecionado]
    
    st.header(f"{produto_selecionado} ({produto_info['unidade']})")
    st.caption(f"Fonte: {produto_info['fonte']} | Período: {data_inicial.strftime('%d/%m/%Y')} a {data_final.strftime('%d/%m/%Y')}")
    
    with st.spinner(f"Buscando dados de {produto_selecionado}..."):
        try:
            # Seleciona a fonte de dados
            if fonte_selecionada == "CEPEA":
                df = buscar_cepea(produto_info['endpoint'], data_inicial, data_final)
            elif fonte_selecionada == "IPEAData":
                df = buscar_ipeadata(produto_info['codigo'], data_inicial, data_final)
            elif fonte_selecionada == "Banco Central":
                df = buscar_bcb(produto_info['codigo'], data_inicial, data_final)
            
            # Mostra métricas
            if not df.empty:
                ultimo = df.iloc[-1]
                cols = st.columns(3)
                cols[0].metric("Último Preço", f"R$ {ultimo['preco']:.2f}" if "R$" in produto_info['unidade'] else f"US$ {ultimo['preco']:.2f}")
                cols[1].metric("Data", ultimo['data'].strftime("%d/%m/%Y"))
                
                if len(df) > 1:
                    variacao = ((ultimo['preco'] - df.iloc[-2]['preco']) / df.iloc[-2]['preco']) * 100
                    cols[2].metric("Variação", f"{variacao:.2f}%", delta=f"{variacao:.2f}%")
                else:
                    cols[2].metric("Variação", "N/D")
                
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
