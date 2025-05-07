import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import json
import re
import os
from functools import lru_cache
import time
import numpy as np

# Configuração da página
st.set_page_config(
    page_title="Cotações Agropecuárias - Dados Oficiais",
    page_icon="🌱",
    layout="wide"
)

# Título do aplicativo
st.title("🌱 Cotações Agropecuárias Oficiais")
st.markdown("Dados em tempo real das principais fontes governamentais")

# Dicionário de produtos atualizado
PRODUTOS = {
    "CEPEA": {
        "Boi Gordo": {"codigo": "boi", "unidade": "R$/@", "fonte": "CEPEA/ESALQ"},
        "Milho": {"codigo": "milho", "unidade": "R$/sc60kg", "fonte": "CEPEA/ESALQ"},
        "Soja": {"codigo": "soja", "unidade": "R$/sc60kg", "fonte": "CEPEA/ESALQ"}
    },
    "CONAB": {
        "Preço Médio Soja (Paraná)": {"codigo": "soja-parana", "unidade": "R$/sc60kg", "fonte": "CONAB"},
        "Preço Médio Milho (MG)": {"codigo": "milho-mg", "unidade": "R$/sc60kg", "fonte": "CONAB"}
    }
}

# Sidebar com seleção de dados
with st.sidebar:
    st.header("Filtros")
    
    fonte_selecionada = st.selectbox(
        "Selecione a fonte",
        list(PRODUTOS.keys())
    
    produto_selecionado = st.selectbox(
        "Selecione o produto",
        list(PRODUTOS[fonte_selecionada].keys()))
    
    data_final = st.date_input(
        "Data final",
        value=datetime.now(),
        max_value=datetime.now(),
        format="DD/MM/YYYY")
    
    data_inicial = st.date_input(
        "Data inicial",
        value=datetime.now() - timedelta(days=30),
        max_value=datetime.now(),
        format="DD/MM/YYYY")
    
    if data_inicial > data_final:
        st.error("A data inicial deve ser anterior à data final")
        st.stop()

    # Opção para upload de arquivo com dados oficiais
    st.markdown("---")
    st.subheader("Opção alternativa")
    arquivo_dados = st.file_uploader(
        "Ou envie arquivo com dados oficiais (CSV/Excel)",
        type=["csv", "xlsx"],
        help="Formato esperado: colunas 'data' (DD/MM/AAAA) e 'preco' (valores numéricos)")

# Função para verificar dados
def verificar_dados(df, produto):
    """Verifica se os dados estão dentro de faixas razoáveis"""
    faixas = {
        "Boi Gordo": (200, 350),
        "Milho": (50, 120),
        "Soja": (100, 200),
        "Preço Médio Soja (Paraná)": (100, 200),
        "Preço Médio Milho (MG)": (50, 120)
    }
    
    if produto in faixas:
        min_val, max_val = faixas[produto]
        if df['preco'].min() < min_val or df['preco'].max() > max_val:
            st.warning(f"Valores fora da faixa esperada para {produto} ({min_val}-{max_val})")
            return False
    return True

# Função para buscar dados do CEPEA com tratamento robusto
@st.cache_data(ttl=3600, show_spinner="Buscando dados do CEPEA...")
def buscar_cepea(codigo, data_inicial, data_final):
    """Busca dados reais do CEPEA com múltiplas tentativas"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    for tentativa in range(3):  # 3 tentativas
        try:
            url = f"https://www.cepea.esalq.usp.br/br/indicador/{codigo}.aspx"
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Tentativa 1: Extrair do JavaScript
            script_tag = soup.find('script', string=re.compile('var dados = '))
            if script_tag:
                script_text = script_tag.string
                match = re.search(r'var dados = (\[.*?\])', script_text)
                if match:
                    dados_str = match.group(1)
                    dados = json.loads(dados_str)
                    
                    registros = []
                    for item in dados:
                        try:
                            data = datetime.strptime(item['data'], '%d/%m/%Y').date()
                            preco = float(item['valor'].replace('.', '').replace(',', '.'))
                            if data_inicial <= data <= data_final:
                                registros.append({'data': data, 'preco': preco})
                        except (ValueError, KeyError):
                            continue
                    
                    if registros:
                        df = pd.DataFrame(registros)
                        df['data'] = pd.to_datetime(df['data'])
                        return df.sort_values('data')

            # Tentativa 2: Extrair da tabela HTML
            tabela = soup.find('table', {'class': 'tb_dados'})
            if tabela:
                linhas = tabela.find_all('tr')
                registros = []
                
                for linha in linhas[1:]:  # Pular cabeçalho
                    cols = linha.find_all('td')
                    if len(cols) >= 2:
                        try:
                            data = datetime.strptime(cols[0].get_text(strip=True), '%d/%m/%Y').date()
                            valor = cols[1].get_text(strip=True).replace('.', '').replace(',', '.')
                            preco = float(valor)
                            
                            if data_inicial <= data <= data_final:
                                registros.append({'data': data, 'preco': preco})
                        except (ValueError, AttributeError):
                            continue
                
                if registros:
                    df = pd.DataFrame(registros)
                    df['data'] = pd.to_datetime(df['data'])
                    return df.sort_values('data')

            raise Exception("Dados não encontrados na página")

        except requests.exceptions.RequestException as e:
            if tentativa == 2:  # Última tentativa
                st.error(f"Falha ao acessar o CEPEA após 3 tentativas. Erro: {str(e)}")
                return None
            time.sleep(2)  # Espera antes de tentar novamente
            continue
            
        except Exception as e:
            st.error(f"Erro ao processar dados do CEPEA: {str(e)}")
            return None

    return None

# Função para carregar dados de arquivo
def carregar_arquivo(arquivo):
    """Carrega dados de arquivo CSV ou Excel"""
    try:
        if arquivo.name.endswith('.csv'):
            df = pd.read_csv(arquivo)
        else:
            df = pd.read_excel(arquivo)
        
        # Verifica colunas necessárias
        if 'data' not in df.columns or 'preco' not in df.columns:
            st.error("Arquivo deve conter colunas 'data' e 'preco'")
            return None
        
        # Converte datas
        df['data'] = pd.to_datetime(df['data'], dayfirst=True)
        df['preco'] = pd.to_numeric(df['preco'], errors='coerce')
        df = df.dropna(subset=['data', 'preco'])
        
        return df
    
    except Exception as e:
        st.error(f"Erro ao ler arquivo: {str(e)}")
        return None

# Interface principal
def main():
    produto_info = PRODUTOS[fonte_selecionada][produto_selecionado]
    
    st.header(f"{produto_selecionado} ({produto_info['unidade']})")
    st.caption(f"Fonte: {produto_info['fonte']} | Período: {data_inicial.strftime('%d/%m/%Y')} a {data_final.strftime('%d/%m/%Y')}")
    
    # Verifica se há arquivo para upload
    if arquivo_dados is not None:
        df = carregar_arquivo(arquivo_dados)
        fonte = "Arquivo enviado pelo usuário"
    else:
        with st.spinner(f"Buscando dados de {produto_selecionado}..."):
            if fonte_selecionada == "CEPEA":
                df = buscar_cepea(produto_info['codigo'], data_inicial, data_final)
                fonte = produto_info['fonte']
            else:
                st.warning("Fonte selecionada requer upload de arquivo com dados oficiais")
                df = None
    
    if df is None or df.empty:
        st.error("Não foi possível obter dados para este produto/período.")
        st.info("Sugestões:")
        st.info("1. Tente ajustar as datas")
        st.info("2. Verifique se o produto está disponível no site oficial")
        st.info("3. Use a opção de enviar arquivo com dados oficiais")
        return
    
    # Verificação de qualidade dos dados
    if not verificar_dados(df, produto_selecionado):
        st.warning("Os valores podem não estar atualizados ou corretos. Verifique a fonte oficial.")
    
    # Processamento dos dados
    df = df[(df['data'] >= pd.to_datetime(data_inicial)) & 
            (df['data'] <= pd.to_datetime(data_final))]
    df = df.sort_values('data')
    
    # Exibição dos resultados
    ultimo = df.iloc[-1]
    cols = st.columns(3)
    
    # Formatação do valor
    valor_formatado = f"R$ {ultimo['preco']:,.2f}" if "R$" in produto_info['unidade'] else f"US$ {ultimo['preco']:,.2f}"
    cols[0].metric("Último Preço", valor_formatado)
    cols[1].metric("Data", ultimo['data'].strftime("%d/%m/%Y"))
    
    if len(df) > 1:
        variacao = ((ultimo['preco'] - df.iloc[-2]['preco']) / df.iloc[-2]['preco']) * 100
        cols[2].metric("Variação", f"{variacao:.2f}%", delta=f"{variacao:.2f}%")
    
    # Gráfico
    st.subheader("Evolução de Preços")
    st.line_chart(df.set_index('data')['preco'])
    
    # Tabela com dados
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
    
    # Botões de ação
    col1, col2 = st.columns(2)
    
    with col1:
        # Botão de download
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            "Download CSV",
            csv,
            f"cotacao_{produto_selecionado.replace(' ', '_')}.csv",
            "text/csv",
            key='download-csv'
        )
    
    with col2:
        # Link para fonte oficial
        if fonte_selecionada == "CEPEA":
            st.markdown(f"[🔍 Ver no site do CEPEA](https://www.cepea.esalq.usp.br/br/indicador/{produto_info['codigo']}.aspx)")
        elif fonte_selecionada == "CONAB":
            st.markdown(f"[🔍 Ver no site da CONAB](https://www.conab.gov.br/)")

if __name__ == "__main__":
    main()
