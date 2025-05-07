import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import json
import random
from functools import lru_cache
import time

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

# Sidebar com seleção de datas
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
    
    data_final = st.date_input(
        "Data final",
        value=datetime.now(),
        max_value=datetime.now(),
        format="DD/MM/YYYY"
    )
    
    data_inicial = st.date_input(
        "Data inicial",
        value=datetime.now() - timedelta(days=30),
        max_value=datetime.now(),
        format="DD/MM/YYYY"
    )
    
    if data_inicial > data_final:
        st.error("A data inicial deve ser anterior à data final")
        st.stop()

# Função para gerar dados de exemplo
def gerar_dados_exemplo(data_inicial, data_final, produto):
    """Gera dados fictícios quando a API falha"""
    dias = (data_final - data_inicial).days + 1
    base_price = {
        "Boi Gordo": 250, "Bezerro": 180, "Milho": 85, "Soja": 150,
        "Café Arábica": 1200, "Soja (Paraná)": 160, "Boi Gordo (SP)": 255,
        "Café Arábica (BCB)": 1250, "Boi Gordo (BCB)": 245, "Soja (BCB)": 155
    }.get(produto, 100)
    
    dates = pd.date_range(start=data_inicial, end=data_final)
    prices = [base_price + random.uniform(-5, 5) * (i/dias) for i in range(dias)]
    
    return pd.DataFrame({
        'data': dates,
        'preco': prices
    })

# Função para buscar dados do CEPEA com web scraping
@lru_cache(maxsize=32)
def buscar_cepea(endpoint, data_inicial, data_final):
    """Busca dados do CEPEA com web scraping"""
    try:
        # URL do CEPEA para a série histórica
        url = f"https://www.cepea.esalq.usp.br/br/indicador/{endpoint}.aspx"
        
        # Simulando um navegador para evitar bloqueio
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # Fazendo a requisição
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        # Parseando o HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Encontrando a tabela de dados (ajuste conforme a estrutura real do site)
        tabela = soup.find('table', {'class': 'tb_dados'})
        
        if not tabela:
            raise Exception("Tabela de dados não encontrada no CEPEA")
        
        # Extraindo os dados da tabela
        linhas = tabela.find_all('tr')
        dados = []
        
        for linha in linhas[1:]:  # Pula o cabeçalho
            cols = linha.find_all('td')
            if len(cols) >= 2:
                data = cols[0].get_text(strip=True)
                valor = cols[1].get_text(strip=True).replace(',', '.')
                
                try:
                    data_dt = datetime.strptime(data, '%d/%m/%Y').date()
                    valor_float = float(valor)
                    
                    if data_inicial <= data_dt <= data_final:
                        dados.append({
                            'data': data_dt,
                            'preco': valor_float
                        })
                except ValueError:
                    continue
        
        if not dados:
            raise Exception("Nenhum dado válido encontrado no período")
        
        df = pd.DataFrame(dados)
        df['data'] = pd.to_datetime(df['data'])
        df = df.sort_values('data').reset_index(drop=True)
        
        return df
    
    except Exception as e:
        st.warning(f"Dados reais do CEPEA temporariamente indisponíveis. Mostrando dados simulados para referência. (Erro: {str(e)})")
        return gerar_dados_exemplo(data_inicial, data_final, endpoint)

# Função para buscar dados do IPEAData com cache
@lru_cache(maxsize=32)
def buscar_ipeadata(codigo, data_inicial, data_final):
    """Busca dados do IPEAData com tratamento de erros"""
    try:
        # Nova API do IPEAData (v4)
        url = f"http://www.ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO='{codigo}')"
        
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        
        dados = response.json()
        
        if not dados or 'value' not in dados:
            raise Exception("Resposta da API não contém dados válidos")
            
        df = pd.DataFrame(dados['value'])
        
        if len(df) == 0:
            raise Exception("Dataframe vazio retornado pela API")
            
        # Processamento dos dados
        df = df.rename(columns={
            'VALDATA': 'data',
            'VALVALOR': 'preco'
        })
        
        df['data'] = pd.to_datetime(df['data'])
        df['preco'] = pd.to_numeric(df['preco'], errors='coerce')
        df = df.dropna(subset=['preco'])
        
        # Filtro por período
        mask = (df['data'] >= pd.to_datetime(data_inicial)) & (df['data'] <= pd.to_datetime(data_final))
        df = df.loc[mask].copy()
        
        if df.empty:
            raise Exception("Nenhum dado disponível para o período selecionado")
            
        return df.sort_values('data').reset_index(drop=True)
    
    except Exception as e:
        st.warning(f"Dados reais do IPEAData temporariamente indisponíveis. Mostrando dados simulados para referência. (Erro: {str(e)})")
        return gerar_dados_exemplo(data_inicial, data_final, codigo)

# Função para buscar dados do Banco Central com cache e retry
@lru_cache(maxsize=32)
def buscar_bcb(codigo, data_inicial, data_final):
    """Busca dados do BCB com tratamento de erros e retry"""
    max_retries = 3
    retry_delay = 2  # segundos
    
    for attempt in range(max_retries):
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
                raise Exception("Resposta vazia da API do BCB")
                
            df = pd.DataFrame(dados)
            
            # Processamento dos dados
            df = df.rename(columns={'valor': 'preco'})
            df['data'] = pd.to_datetime(df['data'], dayfirst=True)
            df['preco'] = pd.to_numeric(df['preco'], errors='coerce')
            df = df.dropna(subset=['preco'])
            
            if df.empty:
                raise Exception("Nenhum dado válido retornado")
                
            return df.sort_values('data').reset_index(drop=True)
            
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                st.warning(f"Dados reais do BCB temporariamente indisponíveis. Mostrando dados simulados para referência. (Erro: {str(e)})")
                return gerar_dados_exemplo(data_inicial, data_final, codigo)
            time.sleep(retry_delay)
        except Exception as e:
            st.warning(f"Dados reais do BCB temporariamente indisponíveis. Mostrando dados simulados para referência. (Erro: {str(e)})")
            return gerar_dados_exemplo(data_inicial, data_final, codigo)

# Interface principal
def main():
    produto_info = API_PRODUTOS[fonte_selecionada][produto_selecionado]
    
    st.header(f"{produto_selecionado} ({produto_info['unidade']})")
    st.caption(f"Fonte: {produto_info['fonte']} | Período: {data_inicial.strftime('%d/%m/%Y')} a {data_final.strftime('%d/%m/%Y')}")
    
    with st.spinner(f"Buscando dados de {produto_selecionado}..."):
        try:
            # Seleciona a fonte de dados apropriada
            if fonte_selecionada == "CEPEA":
                df = buscar_cepea(produto_info['endpoint'], data_inicial, data_final)
            elif fonte_selecionada == "IPEAData":
                df = buscar_ipeadata(produto_info['codigo'], data_inicial, data_final)
            elif fonte_selecionada == "Banco Central":
                df = buscar_bcb(produto_info['codigo'], data_inicial, data_final)
            
            # Verifica se temos dados válidos
            if df.empty:
                st.warning("Nenhum dado encontrado para o período selecionado.")
                df = gerar_dados_exemplo(data_inicial, data_final, produto_selecionado)
            
            # Mostra métricas
            if not df.empty:
                ultimo = df.iloc[-1]
                cols = st.columns(3)
                
                # Formata o valor conforme a unidade
                valor_formatado = f"R$ {ultimo['preco']:.2f}" if "R$" in produto_info['unidade'] else f"US$ {ultimo['preco']:.2f}"
                cols[0].metric("Último Preço", valor_formatado)
                
                cols[1].metric("Data", ultimo['data'].strftime("%d/%m/%Y"))
                
                if len(df) > 1:
                    variacao = ((ultimo['preco'] - df.iloc[-2]['preco']) / df.iloc[-2]['preco']) * 100
                    cols[2].metric("Variação", f"{variacao:.2f}%", delta=f"{variacao:.2f}%")
                else:
                    cols[2].metric("Variação", "N/D")
                
                # Gráfico interativo
                st.subheader("Evolução de Preços")
                st.line_chart(df.set_index('data')['preco'])
                
                # Tabela de dados
                st.subheader("Histórico Completo")
                
                # Formatação da tabela
                df_display = df.copy()
                df_display['data'] = df_display['data'].dt.strftime('%d/%m/%Y')
                df_display['preco'] = df_display['preco'].apply(
                    lambda x: f"R$ {x:.2f}" if "R$" in produto_info['unidade'] else f"US$ {x:.2f}")
                
                st.dataframe(
                    df_display.sort_values('data', ascending=False),
                    use_container_width=True,
                    height=300,
                    hide_index=True
                )
                
                # Botão para download
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"cotacao_{produto_selecionado.lower().replace(' ', '_')}.csv",
                    mime='text/csv',
                    key=f"download_{produto_selecionado}"
                )
            
        except Exception as e:
            st.error(f"Erro inesperado ao processar os dados: {str(e)}")
            st.exception(e)

if __name__ == "__main__":
    main()
