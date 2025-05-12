import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from utils.api_connector import CepeaAPI
from utils.data_processor import DataProcessor
from utils.visualization import Visualizer
import os

def check_dependencies():
    try:
        import statsmodels
        return True
    except ImportError:
        st.warning("""
        Algumas funcionalidades avançadas estão desativadas porque 
        o pacote statsmodels não está instalado. Para ativar todas 
        as funcionalidades, instale com: 
        `pip install statsmodels`
        """)
        return False

# Configuração básica da página
def setup_page():
    st.set_page_config(
        page_title="CEPEA Analytics",
        page_icon="🌱",
        layout="wide"
    )
    st.title("🌱 Análise de Cotações Agrícolas")
    st.markdown("Dados da CEPEA - ESALQ/USP")

# Carregar produtos com tratamento de erro
def load_products_safe():
    try:
        api = CepeaAPI()
        return api.get_available_products()
    except Exception as e:
        st.error(f"Erro ao carregar produtos: {str(e)}")
        return []

# Obter dados processados
def get_data(product_code, start_date, end_date):
    try:
        api = CepeaAPI()
        raw_data = api.get_historical_prices(product_code, start_date, end_date)
        if not raw_data.empty:
            return DataProcessor.prepare_analysis_data(raw_data)
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Erro ao processar dados: {str(e)}")
        return pd.DataFrame()

# Interface principal
def main():
    setup_page()
    
    # Sidebar
    with st.sidebar:
        st.header("🔍 Filtros")
        products = load_products_safe()
        
        if not products:
            return
            
        selected = st.multiselect(
            "Produtos",
            options=[p['name'] for p in products],
            default=[products[0]['name']]
        )
        
        # Datas
        today = datetime.now()
        start = st.date_input(
            "Data inicial",
            value=today - timedelta(days=365),
            max_value=today
        )
        end = st.date_input(
            "Data final",
            value=today,
            max_value=today
        )
        
        analysis = st.selectbox(
            "Tipo de análise",
            ["Histórico", "Sazonal", "Comparativo"]
        )

    # Verificação básica
    if not selected or start > end:
        st.warning("Selecione pelo menos um produto e datas válidas")
        return
    
    # Obter dados
    dfs = []
    for product in selected:
        code = next((p['code'] for p in products if p['name'] == product), None)
        if code:
            df = get_data(code, start, end)
            if not df.empty:
                dfs.append((product, df))
    
    if not dfs:
        st.error("Nenhum dado encontrado para os filtros selecionados")
        return
    
    # Visualizações
    if analysis == "Histórico":
        for name, df in dfs:
            st.subheader(name)
            st.plotly_chart(
                Visualizer.create_historical_plot(df, name),
                use_container_width=True
            )
    
    elif analysis == "Sazonal":
        if not has_full_functionality:
            st.error("Análise sazonal requer statsmodels. Instale com: pip install statsmodels")
        else:
            cols = st.columns(len(dfs))
            for col, (name, df) in zip(cols, dfs):
                with col:
                    try:
                        st.plotly_chart(
                            Visualizer.create_seasonal_plot(df, name),
                            use_container_width=True
                        )
                    except Exception as e:
                        st.error(f"Erro na análise sazonal: {str(e)}")

    ##### Passos para Resolver o Problema:  
    elif analysis == "Sazonal":
        cols = st.columns(len(dfs))
        for col, (name, df) in zip(cols, dfs):
            with col:
                try:
                    if len(df) < 730:
                        st.warning(f"{name}: Necessários 730+ dias para análise sazonal")
                        continue
                        
                    st.plotly_chart(
                        Visualizer.create_seasonal_plot(df, name),
                        use_container_width=True
                    )
                except Exception as e:
                    st.error(f"{name}: {str(e)}")
    
    # Dados brutos
    with st.expander("📊 Ver dados completos"):
        for name, df in dfs:
            st.subheader(name)
            st.dataframe(df)

if __name__ == "__main__":
    main()
