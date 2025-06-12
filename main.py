import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import os
import sys

# Adiciona o diretório pai ao path para importar utils
# Garante que a estrutura do seu projeto seja:
# seu_projeto/
# ├── main.py
# └── utils/
#     ├── __init__.py
#     ├── api_connector.py
#     ├── data_processor.py
#     └── visualization.py
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from utils.api_connector import CepeaAPI
    from utils.data_processor import DataProcessor
    from utils.visualization import Visualizer
except ImportError as e:
    st.error(f"Erro ao importar módulos essenciais. Verifique a estrutura das pastas 'utils' e o conteúdo dos arquivos. Detalhes: {str(e)}")
    st.stop() # Interrompe a execução do app se as importações falharem

def check_dependencies():
    """Verifica se as dependências opcionais (como statsmodels) estão instaladas."""
    try:
        import statsmodels
        return True
    except ImportError:
        st.warning("""
        Algumas funcionalidades avançadas (como a Análise Sazonal) estão desativadas porque 
        o pacote `statsmodels` não está instalado. Para ativar todas 
        as funcionalidades, instale com: `pip install statsmodels`
        """)
        return False

def setup_page():
    """Configura o cabeçalho e título da página Streamlit."""
    st.set_page_config(
        page_title="CEPEA Analytics",
        page_icon="🌱",
        layout="wide"
    )
    st.title("🌱 Análise de Cotações Agrícolas")
    st.markdown("Dados da CEPEA - ESALQ/USP")

@st.cache_data(ttl=86400) # Cacheia os produtos por 24 horas para evitar requisições repetidas
def load_products_safe():
    """Carrega a lista de produtos disponíveis da API de forma segura."""
    try:
        api = CepeaAPI()
        products = api.get_available_products()
        if not products:
            st.error("Não foi possível carregar a lista de produtos disponíveis da API. Verifique a conexão ou a configuração em `api_connector.py`.")
            return []
        return products
    except Exception as e:
        st.error(f"Erro ao carregar produtos: {str(e)}")
        return []

@st.cache_data(ttl=3600) # Cacheia os dados históricos por 1 hora
def get_processed_data_safe(product_code, start_date, end_date):
    """Obtém dados históricos da API e os processa, com tratamento de erros."""
    try:
        api = CepeaAPI()
        with st.spinner(f"Carregando dados históricos para {product_code}..."):
            raw_data = api.get_historical_prices(product_code, start_date, end_date)
        
        if raw_data.empty:
            return pd.DataFrame() # Retorna DataFrame vazio se não houver dados ou houver falha no scraping

        processed_data = DataProcessor.prepare_analysis_data(raw_data)
        
        if processed_data.empty:
            st.warning(f"Os dados para o produto {product_code} foram carregados, mas o processamento resultou em um DataFrame vazio. Verifique `data_processor.py`.")
            return pd.DataFrame()

        # Formatação final dos valores para 2 casas decimais
        if 'price' in processed_data.columns and processed_data['price'] is not None:
            processed_data['price'] = processed_data['price'].round(2)
        if 'price_usd' in processed_data.columns and processed_data['price_usd'] is not None:
            processed_data['price_usd'] = processed_data['price_usd'].round(2)
            
        # Garante que a coluna 'date' seja datetime e esteja ordenada para cálculo de métricas
        if 'date' in processed_data.columns:
            processed_data['date'] = pd.to_datetime(processed_data['date'])
            processed_data = processed_data.sort_values(by='date')
        else:
            st.warning(f"A coluna 'date' não foi encontrada nos dados processados para {product_code}. Verifique `data_processor.py`.")
            return pd.DataFrame()

        return processed_data
    except Exception as e:
        st.error(f"Erro ao obter ou processar dados históricos para {product_code}: {str(e)}. Verifique `api_connector.py` e `data_processor.py`.")
        return pd.DataFrame()

def display_product_metrics(product_name, df, product_unit="unidade"):
    """Exibe os cartões de métricas para um produto."""
    if df.empty:
        st.warning(f"Não há dados para exibir métricas para {product_name} no período selecionado.")
        return

    df_sorted = df.sort_values(by='date').copy()
    
    if len(df_sorted) < 2: # Precisa de pelo menos 2 pontos para calcular variação
        st.warning(f"Dados insuficientes para {product_name} para calcular variação (apenas {len(df_sorted)} ponto(s)).")
        st.metric(label=f"Preço Atual ({product_name})", value=f"R$ {df_sorted.iloc[-1]['price']:.2f} / {product_unit}")
        return

    start_price_row = df_sorted.iloc[0]
    end_price_row = df_sorted.iloc[-1]

    start_price_brl = start_price_row.get('price')
    end_price_brl = end_price_row.get('price')
    
    start_price_usd = start_price_row.get('price_usd')
    end_price_usd = end_price_row.get('price_usd')

    # Calcula variação percentual BRL
    percentage_change_brl = 0.0
    if pd.notna(start_price_brl) and start_price_brl != 0:
        percentage_change_brl = ((end_price_brl - start_price_brl) / start_price_brl) * 100
    
    # Calcula variação percentual USD (se disponível)
    percentage_change_usd = 0.0
    if pd.notna(start_price_usd) and start_price_usd != 0:
        percentage_change_usd = ((end_price_usd - start_price_usd) / start_price_usd) * 100

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(label=f"Preço Atual (BRL)", value=f"R$ {end_price_brl:.2f} / {product_unit}" if pd.notna(end_price_brl) else "N/A")
    with col2:
        st.metric(label=f"Preço Inicial (BRL)", value=f"R$ {start_price_brl:.2f} / {product_unit}" if pd.notna(start_price_brl) else "N/A")
    with col3:
        st.metric(
            label=f"Variação % (BRL)",
            value=f"{percentage_change_brl:.2f}%",
            delta=f"{percentage_change_brl:.2f}%" if percentage_change_brl != 0 else None,
            delta_color="inverse" if percentage_change_brl > 0 else ("normal" if percentage_change_brl < 0 else "off")
        )
    
    # Exibe métricas USD se o produto tiver preço em USD válido
    if pd.notna(start_price_usd) and pd.notna(end_price_usd):
        col4, col5, col6 = st.columns(3)
        with col4:
            st.metric(label=f"Preço Atual (USD)", value=f"US$ {end_price_usd:.2f} / {product_unit}" if pd.notna(end_price_usd) else "N/A")
        with col5:
            st.metric(label=f"Preço Inicial (USD)", value=f"US$ {start_price_usd:.2f} / {product_unit}" if pd.notna(start_price_usd) else "N/A")
        with col6:
            st.metric(
                label=f"Variação % (USD)",
                value=f"{percentage_change_usd:.2f}%",
                delta=f"{percentage_change_usd:.2f}%" if percentage_change_usd != 0 else None,
                delta_color="inverse" if percentage_change_usd > 0 else ("normal" if percentage_change_usd < 0 else "off")
            )


def main():
    setup_page()
    has_full_functionality = check_dependencies()
    
    with st.sidebar:
        st.header("🔍 Filtros")
        
        products = load_products_safe() # Já tem spinner dentro do get_processed_data_safe

        if not products:
            # load_products_safe já exibe um erro, então só retorna aqui
            return
            
        selected_product_names = st.multiselect(
            "Produtos",
            options=[p['name'] for p in products],
            default=[products[0]['name']] if products else []
        )
        
        today = datetime.now().date() # Usar .date() para comparar com st.date_input
        default_start_date = today - timedelta(days=365) # Padrão de 1 ano
        
        start_date = st.date_input(
            "Data inicial",
            value=default_start_date,
            max_value=today # Não permite datas futuras
        )
        end_date = st.date_input(
            "Data final",
            value=today,
            max_value=today # Não permite datas futuras
        )
        
        analysis_type = st.selectbox(
            "Tipo de análise",
            ["Histórico", "Sazonal", "Comparativo"]
        )

    if not selected_product_names:
        st.info("Selecione um ou mais produtos no menu lateral para iniciar a análise.")
        return
        
    if start_date > end_date:
        st.error("A 'Data inicial' não pode ser posterior à 'Data final'. Por favor, ajuste as datas.")
        return
        
    dfs = []
    # Cria um mapa de nome do produto para suas informações completas (incluindo unit e code)
    product_name_to_info_map = {p['name']: p for p in products}

    for product_name in selected_product_names:
        product_info = product_name_to_info_map.get(product_name)
        if product_info:
            product_code = product_info['code']
            df = get_processed_data_safe(product_code, start_date, end_date)
            if not df.empty:
                dfs.append((product_name, df, product_info['unit'])) # Adiciona a unidade para exibição
            else:
                st.warning(f"Nenhum dado válido encontrado para '{product_name}' no período selecionado. Tente ajustar as datas.")
        else:
            st.error(f"Informações do produto não encontradas para '{product_name}'.")
    
    if not dfs:
        st.error("Nenhum dado válido foi carregado para os filtros selecionados. Por favor, verifique sua conexão ou tente outros produtos/datas.")
        return
        
    # --- Apresentação dos resultados baseada no tipo de análise ---
    
    if analysis_type == "Histórico":
        for name, df, unit in dfs:
            st.subheader(f"📈 {name} - Análise Histórica")
            display_product_metrics(name, df, unit) # Passa a unidade para a função de métricas
            st.plotly_chart(
                Visualizer.create_historical_plot(df, name),
                use_container_width=True
            )
            
    elif analysis_type == "Sazonal":
        if not has_full_functionality:
            st.error("A Análise Sazonal requer o pacote `statsmodels`. Instale com: `pip install statsmodels`")
        else:
            st.subheader("📊 Análise Sazonal")
            cols = st.columns(len(dfs)) # Cria colunas dinamicamente para os gráficos
            for i, (name, df, unit) in enumerate(dfs):
                with cols[i]:
                    st.markdown(f"**{name}**")
                    try:
                        if len(df) < 730: # Aproximadamente 2 anos de dados diários para decomposição
                            st.warning(f"{name}: São necessários pelo menos 730 dias de dados para uma análise sazonal significativa. Dados atuais: {len(df)} dias.")
                            continue
                        st.plotly_chart(
                            Visualizer.create_seasonal_plot(df, name),
                            use_container_width=True
                        )
                    except Exception as e:
                        st.error(f"Erro ao gerar análise sazonal para {name}: {str(e)}")
            
    elif analysis_type == "Comparativo":
        if len(dfs) > 1:
            st.subheader("🔗 Análise Comparativa de Preços")
            # Extrai apenas os DataFrames e nomes para a função de comparação
            comparison_dfs = [df for name, df, unit in dfs]
            comparison_names = [name for name, df, unit in dfs]

            st.plotly_chart(
                Visualizer.create_correlation_plot(
                    comparison_dfs,
                    comparison_names
                ),
                use_container_width=True
            )
        else:
            st.warning("Selecione pelo menos 2 produtos para realizar uma análise comparativa.")
            
    # --- Expander para ver os dados completos ---
    with st.expander("📝 Ver dados completos"):
        for name, df, unit in dfs:
            st.subheader(f"Dados de {name}")
            st.dataframe(df)

if __name__ == "__main__":
    main()
