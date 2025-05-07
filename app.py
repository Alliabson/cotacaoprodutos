import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import json
import re
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

# Dicionário de APIs e produtos atualizado
API_PRODUTOS = {
    "CEPEA": {
        "Boi Gordo": {"endpoint": "boi", "unidade": "R$/@", "fonte": "CEPEA/ESALQ"},
        "Milho": {"endpoint": "milho", "unidade": "R$/sc60kg", "fonte": "CEPEA/ESALQ"},
        "Soja": {"endpoint": "soja", "unidade": "R$/sc60kg", "fonte": "CEPEA/ESALQ"}
    },
    "IPEAData": {
        "Soja (Paraná)": {"codigo": "PPM12_SOJA12", "unidade": "US$/sc60kg", "fonte": "IPEAData"},
        "Café Arábica": {"codigo": "PPM12_CAFE12", "unidade": "US$/sc60kg", "fonte": "IPEAData"}
    },
    "Banco Central": {
        "Café Arábica (BCB)": {"codigo": "7461", "unidade": "US$/sc60kg", "fonte": "BCB"},
        "Boi Gordo (BCB)": {"codigo": "1", "unidade": "R$/@", "fonte": "BCB"}
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

# Função para verificar dados
def verificar_dados(df, produto):
    """Verifica se os dados estão dentro de faixas razoáveis"""
    faixas = {
        "Boi Gordo": (200, 350),
        "Milho": (50, 120),
        "Soja": (100, 200),
        "Café Arábica": (800, 1500),
        "Soja (Paraná)": (120, 200),
        "Boi Gordo (BCB)": (200, 350),
        "Café Arábica (BCB)": (800, 1500)
    }
    
    if produto in faixas:
        min_val, max_val = faixas[produto]
        if df['preco'].min() < min_val or df['preco'].max() > max_val:
            st.warning(f"Valores fora da faixa esperada para {produto} ({min_val}-{max_val})")
            return False
    return True

# Função para buscar dados do CEPEA corrigida
@lru_cache(maxsize=32)
def buscar_cepea(endpoint, data_inicial, data_final):
    """Busca dados reais do CEPEA com scraping atualizado"""
    try:
        url = f"https://www.cepea.esalq.usp.br/br/indicador/{endpoint}.aspx"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Encontrando os dados na nova estrutura do CEPEA
        script_tag = soup.find('script', string=re.compile('var dados = '))
        if not script_tag:
            raise Exception("Dados não encontrados no HTML")
        
        # Extraindo os dados do JavaScript
        script_text = script_tag.string
        dados_str = re.search(r'var dados = (\[.*?\])', script_text).group(1)
        dados = json.loads(dados_str)
        
        # Processando os dados
        registros = []
        for item in dados:
            data = datetime.strptime(item['data'], '%d/%m/%Y').date()
            if data_inicial <= data <= data_final:
                preco = float(item['valor'].replace('.', '').replace(',', '.'))
                registros.append({'data': data, 'preco': preco})
        
        if not registros:
            raise Exception("Nenhum dado no período selecionado")
        
        df = pd.DataFrame(registros)
        df['data'] = pd.to_datetime(df['data'])
        
        return df.sort_values('data')
    
    except Exception as e:
        st.error(f"Erro ao buscar dados do CEPEA: {str(e)}")
        return None

# Função para buscar IPEAData atualizada
@lru_cache(maxsize=32)
def buscar_ipeadata(codigo, data_inicial, data_final):
    """Busca dados do IPEAData com nova API"""
    try:
        url = f"https://www.ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO='{codigo}')"
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        
        dados = response.json()
        df = pd.DataFrame(dados.get('value', []))
        
        if df.empty:
            raise Exception("Nenhum dado retornado")
        
        df = df.rename(columns={'VALDATA': 'data', 'VALVALOR': 'preco'})
        df['data'] = pd.to_datetime(df['data'])
        df['preco'] = pd.to_numeric(df['preco'])
        
        df = df[(df['data'] >= pd.to_datetime(data_inicial)) & 
                (df['data'] <= pd.to_datetime(data_final))]
        
        if df.empty:
            raise Exception("Nenhum dado no período")
        
        return df.sort_values('data')
    
    except Exception as e:
        st.error(f"Erro ao buscar IPEAData: {str(e)}")
        return None

# Função para buscar BCB com verificação
@lru_cache(maxsize=32)
def buscar_bcb(codigo, data_inicial, data_final):
    """Busca dados do BCB com verificação"""
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
        df = pd.DataFrame(dados)
        
        if df.empty:
            raise Exception("Nenhum dado retornado")
        
        df = df.rename(columns={'data': 'data', 'valor': 'preco'})
        df['data'] = pd.to_datetime(df['data'], dayfirst=True)
        df['preco'] = pd.to_numeric(df['preco'])
        
        df = df[(df['data'] >= pd.to_datetime(data_inicial)) & 
                (df['data'] <= pd.to_datetime(data_final))]
        
        if df.empty:
            raise Exception("Nenhum dado no período")
        
        return df.sort_values('data')
    
    except Exception as e:
        st.error(f"Erro ao buscar BCB: {str(e)}")
        return None

# Interface principal
def main():
    produto_info = API_PRODUTOS[fonte_selecionada][produto_selecionado]
    
    st.header(f"{produto_selecionado} ({produto_info['unidade']})")
    st.caption(f"Fonte: {produto_info['fonte']} | Período: {data_inicial.strftime('%d/%m/%Y')} a {data_final.strftime('%d/%m/%Y')}")
    
    with st.spinner("Buscando dados..."):
        try:
            # Busca dados conforme a fonte selecionada
            if fonte_selecionada == "CEPEA":
                df = buscar_cepea(produto_info['endpoint'], data_inicial, data_final)
            elif fonte_selecionada == "IPEAData":
                df = buscar_ipeadata(produto_info['codigo'], data_inicial, data_final)
            elif fonte_selecionada == "Banco Central":
                df = buscar_bcb(produto_info['codigo'], data_inicial, data_final)
            
            # Se não conseguir dados reais, mostra mensagem clara
            if df is None or df.empty:
                st.error("Não foi possível obter dados reais para este produto/período.")
                st.info("Tente ajustar as datas ou selecionar outro produto.")
                return
            
            # Verifica se os dados estão dentro de faixas razoáveis
            if not verificar_dados(df, produto_selecionado):
                st.warning("Os valores podem não estar atualizados. Verifique a fonte oficial.")
            
            # Exibição dos resultados
            ultimo = df.iloc[-1]
            cols = st.columns(3)
            
            # Formatação do valor conforme a unidade
            valor_formatado = (f"R$ {ultimo['preco']:,.2f}" if "R$" in produto_info['unidade'] 
                             else f"US$ {ultimo['preco']:,.2f}")
            
            cols[0].metric("Último Preço", valor_formatado)
            cols[1].metric("Data", ultimo['data'].strftime("%d/%m/%Y"))
            
            if len(df) > 1:
                variacao = ((ultimo['preco'] - df.iloc[-2]['preco']) / df.iloc[-2]['preco']) * 100
                cols[2].metric("Variação", f"{variacao:.2f}%", delta=f"{variacao:.2f}%")
            
            # Gráfico com formatação melhorada
            st.subheader("Evolução de Preços")
            st.line_chart(df.set_index('data')['preco'])
            
            # Tabela com dados formatados
            st.subheader("Histórico Completo")
            df_display = df.copy()
            df_display['data'] = df_display['data'].dt.strftime('%d/%m/%Y')
            df_display['preco'] = df_display['preco'].apply(
                lambda x: f"R$ {x:,.2f}" if "R$" in produto_info['unidade'] else f"US$ {x:,.2f}")
            
            st.dataframe(
                df_display.sort_values('data', ascending=False),
                use_container_width=True,
                height=400,
                hide_index=True
            )
            
            # Botão de download
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                "Download CSV",
                csv,
                f"cotacao_{produto_selecionado.replace(' ', '_')}.csv",
                "text/csv",
                key='download-csv'
            )
            
            # Link para fonte oficial
            if fonte_selecionada == "CEPEA":
                st.markdown(f"[Ver no site do CEPEA](https://www.cepea.esalq.usp.br/br/indicador/{produto_info['endpoint']}.aspx)")
            elif fonte_selecionada == "IPEAData":
                st.markdown(f"[Ver no IPEAData](http://www.ipeadata.gov.br/Default.aspx)")
            elif fonte_selecionada == "Banco Central":
                st.markdown(f"[Ver no BCB](https://www.bcb.gov.br/)")
            
        except Exception as e:
            st.error(f"Erro inesperado: {str(e)}")

if __name__ == "__main__":
    main()
