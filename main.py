import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import os
import sys

# Adiciona o diretório pai ao path para importar utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from utils.api_connector import CepeaAPI
    from utils.data_processor import DataProcessor
    from utils.visualization import Visualizer
except ImportError as e:
    st.error(f"Erro ao importar módulos: {str(e)}")
    st.stop()

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

def setup_page():
    st.set_page_config(
        page_title="CEPEA Analytics",
        page_icon="🌱",
        layout="wide"
    )
    st.title("🌱 Análise de Cotações Agrícolas")
    st.markdown("Dados da CEPEA - ESALQ/USP")

def load_products_safe():
    try:
        api = CepeaAPI()
        return api.get_available_products()
    except Exception as e:
        st.error(f"Erro ao carregar produtos: {str(e)}")
        return []

def get_data(product_code, start_date, end_date):
    try:
        api = CepeaAPI()
        raw_data = api.get_historical_prices(product_code, start_date, end_date)
        
        if not raw_data.empty:
            # Processa os dados
            processed_data = DataProcessor.prepare_analysis_data(raw_data)
            
            # Formatação dos valores
            processed_data['price'] = processed_data['price'].round(2)
            if 'price_usd' in processed_data.columns:
                processed_data['price_usd'] = processed_data['price_usd'].round(2)
            
            return processed_data
        
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Erro ao processar dados: {str(e)}")
        return pd.DataFrame()

def main():
    setup_page()
    has_full_functionality = check_dependencies()
    
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

    if not selected or start > end:
        st.warning("Selecione pelo menos um produto e datas válidas")
        return
    
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
                        if len(df) < 730:
                            st.warning(f"{name}: Necessários 730+ dias para análise sazonal")
                            continue
                        st.plotly_chart(
                            Visualizer.create_seasonal_plot(df, name),
                            use_container_width=True
                        )
                    except Exception as e:
                        st.error(f"{name}: {str(e)}")
    
    elif analysis == "Comparativo":
        if len(dfs) > 1:
            st.plotly_chart(
                Visualizer.create_correlation_plot(
                    [df for _, df in dfs],
                    [name for name, _ in dfs]
                ),
                use_container_width=True
            )
        else:
            st.warning("Selecione pelo menos 2 produtos para comparação")
    
    with st.expander("📊 Ver dados completos"):
        for name, df in dfs:
            st.subheader(name)
            st.dataframe(df)

if __name__ == "__main__":
    main()
