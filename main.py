import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import os
import sys

# Adiciona o diretório pai ao path para importar utils
# Certifique-se de que a estrutura do seu projeto seja:
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
    st.error(f"Erro ao importar módulos essenciais. Verifique a estrutura das pastas 'utils' e o conteúdo dos arquivos: {str(e)}")
    st.stop()

def check_dependencies():
    """Verifica se as dependências opcionais estão instaladas."""
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

@st.cache_data(ttl=86400) # Cacheia os produtos por 24 horas
def load_products_safe():
    """Carrega a lista de produtos disponíveis da API de forma segura."""
    try:
        api = CepeaAPI()
        products = api.get_available_products()
        if not products:
            st.error("Não foi possível carregar a lista de produtos da API. Verifique a conexão ou a API.")
            return []
        return products
    except Exception as e:
        st.error(f"Erro ao carregar produtos: {str(e)}")
        return []

@st.cache_data(ttl=3600) # Cacheia os dados históricos por 1 hora
def get_processed_data_safe(product_code, start_date, end_date):
    """Obtém e processa os dados históricos de forma segura."""
    try:
        api = CepeaAPI()
        with st.spinner(f"Carregando dados históricos para {product_code}..."):
            raw_data = api.get_historical_prices(product_code, start_date, end_date)
        
        if raw_data.empty:
            return pd.DataFrame() # Retorna DataFrame vazio se não houver dados

        # Processa os dados
        processed_data = DataProcessor.prepare_analysis_data(raw_data)
        
        if processed_data.empty:
            st.warning(f"Os dados para o produto {product_code} foram carregados, mas o processamento resultou em um DataFrame vazio.")
            return pd.DataFrame()

        # Formatação dos valores para 2 casas decimais
        if 'price' in processed_data.columns:
            processed_data['price'] = processed_data['price'].round(2)
        if 'price_usd' in processed_data.columns and processed_data['price_usd'] is not None:
            processed_data['price_usd'] = processed_data['price_usd'].round(2)
            
        # Garante que a coluna 'date' seja datetime e esteja ordenada para cálculo de métricas
        if 'date' in processed_data.columns:
            processed_data['date'] = pd.to_datetime(processed_data['date'])
            processed_data = processed_data.sort_values(by='date')
        else:
            st.warning(f"A coluna 'date' não foi encontrada nos dados processados para {product_code}.")
            return pd.DataFrame()

        return processed_data
    except Exception as e:
        st.error(f"Erro ao obter ou processar dados históricos para {product_code}: {str(e)}")
        return pd.DataFrame()

def display_product_metrics(product_name, df):
    """Exibe os cartões de métricas para um produto."""
    if df.empty:
        st.warning(f"Não há dados para exibir métricas para {product_name} no período selecionado.")
        return

    # Certifica-se de que o DataFrame está ordenado por data
    df_sorted = df.sort_values(by='date').copy()
    
    # Pega o primeiro e o último preço válido no período
    # Evita iloc[0] ou iloc[-1] se o DF tiver apenas 1 linha ou for problemático
    if len(df_sorted) > 0:
        start_price_row = df_sorted.iloc[0]
        end_price_row = df_sorted.iloc[-1]
    else:
        st.warning(f"Dados insuficientes para {product_name} para calcular métricas.")
        return

    start_price = start_price_row.get('price')
    end_price = end_price_row.get('price')
    
    # Adiciona a lógica para preços em USD se aplicável
    start_price_usd = start_price_row.get('price_usd')
    end_price_usd = end_price_row.get('price_usd')

    percentage_change_brl = 0.0
    if start_price is not None and pd.notna(start_price) and start_price != 0:
        percentage_change_brl = ((end_price - start_price) / start_price) * 100
    
    percentage_change_usd = 0.0
    if start_price_usd is not None and pd.notna(start_price_usd) and start_price_usd != 0:
        percentage_change_usd = ((end_price_usd - start_price_usd) / start_price_usd) * 100

    # Define a direção do delta para a métrica BRL
    delta_color_brl = "off"
    if percentage_change_brl > 0:
        delta_color_brl = "inverse" # Verde para alta
    elif percentage_change_brl < 0:
        delta_color_brl = "normal"  # Vermelho para baixa

    # Define a direção do delta para a métrica USD (se existir)
    delta_color_usd = "off"
    if percentage_change_usd > 0:
        delta_color_usd = "inverse" # Verde para alta
    elif percentage_change_usd < 0:
        delta_color_usd = "normal"  # Vermelho para baixa


    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(label=f"Preço Atual (BRL)", value=f"R$ {end_price:.2f}" if end_price is not None else "N/A")
    with col2:
        st.metric(label=f"Preço Inicial (BRL)", value=f"R$ {start_price:.2f}" if start_price is not None else "N/A")
    with col3:
        st.metric(
            label=f"Variação % (BRL)",
            value=f"{percentage_change_brl:.2f}%",
            delta=f"{percentage_change_brl:.2f}%" if percentage_change_brl != 0 else None,
            delta_color=delta_color_brl
        )
    
    # Exibe métricas USD se o produto tiver preço em USD
    if start_price_usd is not None and end_price_usd is not None:
        col4, col5, col6 = st.columns(3)
        with col4:
            st.metric(label=f"Preço Atual (USD)", value=f"US$ {end_price_usd:.2f}" if end_price_usd is not None else "N/A")
        with col5:
            st.metric(label=f"Preço Inicial (USD)", value=f"US$ {start_price_usd:.2f}" if start_price_usd is not None else "N/A")
        with col6:
            st.metric(
                label=f"Variação % (USD)",
                value=f"{percentage_change_usd:.2f}%",
                delta=f"{percentage_change_usd:.2f}%" if percentage_change_usd != 0 else None,
                delta_color=delta_color_usd
            )


def main():
    setup_page()
    has_full_functionality = check_dependencies()
    
    with st.sidebar:
        st.header("🔍 Filtros")
        
        with st.spinner("Carregando lista de produtos..."):
            products = load_products_safe()
        
        if not products:
            st.error("Não foi possível carregar a lista de produtos. O aplicativo não pode continuar.")
            return
            
        selected_product_names = st.multiselect(
            "Produtos",
            options=[p['name'] for p in products],
            default=[products[0]['name']] if products else []
        )
        
        today = datetime.now().date() # Usar .date() para comparar com st.date_input
        # Ajusta a data inicial para garantir que não seja após a data final padrão
        default_start_date = today - timedelta(days=365)
        
        start_date = st.date_input(
            "Data inicial",
            value=default_start_date,
            max_value=today
        )
        end_date = st.date_input(
            "Data final",
            value=today,
            max_value=today
        )
        
        analysis_type = st.selectbox(
            "Tipo de análise",
            ["Histórico", "Sazonal", "Comparativo"]
        )

    if not selected_product_names:
        st.warning("Selecione pelo menos um produto para iniciar a análise.")
        return
        
    if start_date > end_date:
        st.error("A 'Data inicial' não pode ser posterior à 'Data final'. Ajuste as datas.")
        return
        
    dfs = []
    product_name_to_code_map = {p['name']: p['code'] for p in products}

    for product_name in selected_product_names:
        product_code = product_name_to_code_map.get(product_name)
        if product_code:
            df = get_processed_data_safe(product_code, start_date, end_date)
            if not df.empty:
                dfs.append((product_name, df))
            else:
                st.warning(f"Nenhum dado válido encontrado para '{product_name}' no período selecionado. Tente ajustar as datas.")
        else:
            st.error(f"Código do produto não encontrado para '{product_name}'.")
    
    if not dfs:
        st.error("Nenhum dado válido foi carregado para os filtros selecionados. Por favor, verifique sua conexão ou tente outros produtos/datas.")
        return
        
    # --- Apresentação dos resultados baseada no tipo de análise ---
    
    if analysis_type == "Histórico":
        for name, df in dfs:
            st.subheader(f"📈 {name} - Análise Histórica")
            display_product_metrics(name, df) # Exibe os cartões de métricas
            st.plotly_chart(
                Visualizer.create_historical_plot(df, name),
                use_container_width=True
            )
            
    elif analysis_type == "Sazonal":
        if not has_full_functionality:
            st.error("Análise sazonal requer o pacote `statsmodels`. Instale com: `pip install statsmodels`")
        else:
            # Usa st.container para melhor organização visual quando há várias colunas
            st.subheader("📊 Análise Sazonal")
            cols = st.columns(len(dfs)) 
            for i, (name, df) in enumerate(dfs):
                with cols[i]:
                    st.markdown(f"**{name}**")
                    try:
                        # Para análise sazonal, é comum precisar de um período mais longo (e.g., 2 anos de dados diários)
                        if len(df) < 730: # Aproximadamente 2 anos de dados diários
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
            st.subheader("🔗 Análise Comparativa de Produtos")
            st.plotly_chart(
                Visualizer.create_correlation_plot(
                    [df for _, df in dfs],
                    [name for name, _ in dfs]
                ),
                use_container_width=True
            )
        else:
            st.warning("Selecione pelo menos 2 produtos para realizar uma análise comparativa.")
            
    # --- Expander para ver os dados completos ---
    with st.expander("📝 Ver dados completos"):
        for name, df in dfs:
            st.subheader(f"Dados de {name}")
            st.dataframe(df)

if __name__ == "__main__":
    main()
