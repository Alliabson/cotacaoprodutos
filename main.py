import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from utils.api_connector import CepeaAPI
from utils.data_processor import DataProcessor
from utils.visualization import Visualizer
import os

# Configurações da página
st.set_page_config(
    page_title="CEPEA Analytics Pro",
    page_icon="🌱",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Carregar produtos
@st.cache_data
def load_products():
    api = CepeaAPI()
    return api.get_available_products()

# Processar dados
@st.cache_data(ttl=3600, show_spinner="Carregando dados históricos...")
def get_processed_data(product_code, start_date, end_date):
    api = CepeaAPI()
    raw_data = api.get_historical_prices(product_code, start_date, end_date)
    if raw_data.empty:
        return raw_data
    return DataProcessor.prepare_analysis_data(raw_data)

# Interface principal
def main():
    st.title("🌱 CEPEA Analytics Pro")
    st.markdown("Análise avançada de cotações de produtos agrícolas")
    
    # Sidebar
    with st.sidebar:
        st.header("🔍 Parâmetros de Consulta")
        products = load_products()
        
        # Verifica se products não está vazio
        if not products:
            st.error("Não foi possível carregar a lista de produtos")
            return
            
        selected_products = st.multiselect(
            "Selecione os produtos",
            options=[p['name'] for p in products],
            default=[products[0]['name'] if products else None
        )
        
        # Converter nomes para códigos
        product_codes = []
        for name in selected_products:
            for p in products:
                if p['name'] == name:
                    product_codes.append(p['code'])
                    break
        
        # Seletor de datas
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(
                "Data inicial",
                value=datetime.now() - timedelta(days=365),
                min_value=datetime(2000, 1, 1),
                max_value=datetime.now()
            )
        with col2:
            end_date = st.date_input(
                "Data final",
                value=datetime.now(),
                min_value=datetime(2000, 1, 1),
                max_value=datetime.now()
            )
        
        analysis_type = st.selectbox(
            "Tipo de Análise",
            options=[
                "Histórico de Preços",
                "Análise Sazonal",
                "Distribuição de Preços",
                "Comparativo entre Produtos"
            ]
        )
        
        st.markdown("---")
        st.markdown("⚙️ **Opções Avançadas**")
        show_raw = st.checkbox("Mostrar dados brutos")
        show_stats = st.checkbox("Mostrar estatísticas", value=True)
    
    # Conteúdo principal
    if not selected_products:
        st.warning("Selecione pelo menos um produto")
        return
    
    tab1, tab2, tab3 = st.tabs(["📈 Visualização", "📊 Estatísticas", "💾 Exportar"])
    
    with tab1:
        dfs = []
        for code, name in zip(product_codes, selected_products):
            df = get_processed_data(code, start_date, end_date)
            if df.empty:
                st.error(f"Nenhum dado encontrado para {name}")
                continue
            dfs.append(df)
            
            if analysis_type == "Histórico de Preços":
                st.plotly_chart(
                    Visualizer.create_historical_plot(df, name),
                    use_container_width=True
                )
        
        elif analysis_type == "Análise Sazonal":
            cols = st.columns(len(selected_products))
            for col, df, name in zip(cols, dfs, selected_products):
                with col:
                    try:
                        st.plotly_chart(
                            Visualizer.create_seasonal_plot(df, name),
                            use_container_width=True
                        )
                    except ValueError as e:
                        st.error(str(e))
        
        elif analysis_type == "Distribuição de Preços":
            cols = st.columns(len(selected_products))
            for col, df, name in zip(cols, dfs, selected_products):
                with col:
                    st.plotly_chart(
                        Visualizer.create_price_distribution(df, name),
                        use_container_width=True
                    )
        
        elif analysis_type == "Comparativo entre Produtos":
            if len(selected_products) > 1:
                st.plotly_chart(
                    Visualizer.create_correlation_plot(dfs, selected_products),
                    use_container_width=True
                )
            else:
                st.warning("Selecione pelo menos 2 produtos para comparação")
    
    with tab2:
        if show_stats and dfs:
            st.subheader("Estatísticas Descritivas")
            for df, name in zip(dfs, selected_products):
                st.markdown(f"**{name}**")
                stats = df['price'].describe().reset_index()
                stats.columns = ['Estatística', 'Valor']
                st.table(stats.style.format({'Valor': '{:.2f}'}))
                
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Preço Atual", f"R$ {df['price'].iloc[-1]:.2f}")
                with col2:
                    change_30d = df['price'].iloc[-1] / df['price'].iloc[-30] - 1
                    st.metric("Variação 30 dias", f"{change_30d*100:.2f}%")
    
    with tab3:
        st.subheader("Exportar Dados")
        if dfs:
            format = st.selectbox("Formato de exportação", ["CSV", "Excel", "JSON"])
            
            for df, name in zip(dfs, selected_products):
                filename = f"cepea_{name.lower()}_{start_date}_{end_date}.{format.lower()}"
                
                if format == "CSV":
                    data = df.to_csv(index=False).encode('utf-8')
                elif format == "Excel":
                    data = df.to_excel(index=False)
                elif format == "JSON":
                    data = df.to_json(indent=2, orient='records').encode('utf-8')
                
                st.download_button(
                    label=f"Baixar {name}",
                    data=data,
                    file_name=filename,
                    mime=f"text/{format.lower()}" if format != "Excel" else "application/vnd.ms-excel"
                )
    
    if show_raw and dfs:
        with st.expander("📄 Dados Brutos"):
            for df, name in zip(dfs, selected_products):
                st.subheader(name)
                st.dataframe(df)

if __name__ == "__main__":
    main()
