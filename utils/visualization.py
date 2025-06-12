import plotly.graph_objects as go
from plotly.subplots import make_subplots

class Visualizer:
    @staticmethod
    def create_historical_plot(df, product_name):
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        # Preço em BRL
        fig.add_trace(
            go.Scatter(
                x=df['date'], y=df['price'],
                name='Preço (BRL)',
                line=dict(color='blue'),
                hovertemplate='<b>%{x|%d/%m/%Y}</b><br>R$ %{y:,.2f}<extra></extra>'
            ),
            secondary_y=False
        )
        
        # Preço em USD (se disponível e não nulo)
        if 'price_usd' in df.columns and not df['price_usd'].isnull().all():
            fig.add_trace(
                go.Scatter(
                    x=df['date'], y=df['price_usd'],
                    name='Preço (USD)',
                    line=dict(color='green'),
                    hovertemplate='<b>%{x|%d/%m/%Y}</b><br>US$ %{y:,.2f}<extra></extra>'
                ),
                secondary_y=True
            )
        
        # Médias móveis BRL
        if 'ma_30' in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df['date'], y=df['ma_30'],
                    name='Média 30d (BRL)',
                    line=dict(color='blue', dash='dot'),
                    hoverinfo='skip' # Não mostrar tooltip para a média móvel
                ),
                secondary_y=False
            )
            
            # Médias móveis USD (se disponível e não nulo)
            if 'ma_30_usd' in df.columns and not df['ma_30_usd'].isnull().all():
                fig.add_trace(
                    go.Scatter(
                        x=df['date'], y=df['ma_30_usd'],
                        name='Média 30d (USD)',
                        line=dict(color='green', dash='dot'),
                        hoverinfo='skip'
                    ),
                    secondary_y=True
                )
        
        fig.update_layout(
            title=f'{product_name} - Preços Históricos',
            xaxis_title='Data',
            yaxis_title=dict(text='Preço (BRL)', standoff=30), # standoff para afastar o título
            hovermode='x unified', # Unifica o hover para todas as traces na mesma data
            template='plotly_white',
            height=600
        )
        
        # Configura o eixo Y secundário apenas se houver dados USD
        if 'price_usd' in df.columns and not df['price_usd'].isnull().all():
            fig.update_yaxes(
                title_text='Preço (USD)',
                secondary_y=True
            )
        else:
            # Se não houver USD, garante que o eixo secundário não é exibido ou está desativado
            fig.update_yaxes(secondary_y=True, showgrid=False, zeroline=False, showticklabels=False, showline=False)
            
        return fig

    # --- Funções de Análise Sazonal e Comparativa (Assumindo que já existem ou serão implementadas) ---
    @staticmethod
    def create_seasonal_plot(df, product_name):
        # Esta função requer o pacote statsmodels e uma implementação mais complexa
        # Para um bom gráfico sazonal, você precisaria decompor a série temporal.
        # Exemplo BÁSICO (requer statsmodels):
        from statsmodels.tsa.seasonal import seasonal_decompose
        
        if len(df) < 2 * 365: # Mínimo de 2 anos para decomposição sazonal diária
             print(f"AVISO: Dados insuficientes para decomposição sazonal para {product_name}.")
             fig = go.Figure().update_layout(title=f"{product_name} - Dados insuficientes para Sazonalidade (min 2 anos)", xaxis_visible=False, yaxis_visible=False)
             return fig

        df_seasonal = df.set_index('date')['price'].asfreq('D') # Garante frequência diária, preenche faltantes com NaN
        df_seasonal = df_seasonal.fillna(method='ffill').fillna(method='bfill') # Preenche NaNs, importante para decomposição
        
        try:
            # O período para dados diários é 7 (semanal) ou 365 (anual)
            # Para sazonalidade anual, period=365 é comum.
            decomposition = seasonal_decompose(df_seasonal, model='additive', period=365)
            
            fig = make_subplots(rows=4, cols=1, 
                                subplot_titles=(f"{product_name} - Original", "Tendência", "Sazonalidade", "Resíduo"),
                                shared_xaxes=True)
            
            fig.add_trace(go.Scatter(x=df_seasonal.index, y=df_seasonal, mode='lines', name='Original'), row=1, col=1)
            fig.add_trace(go.Scatter(x=df_seasonal.index, y=decomposition.trend, mode='lines', name='Tendência'), row=2, col=1)
            fig.add_trace(go.Scatter(x=df_seasonal.index, y=decomposition.seasonal, mode='lines', name='Sazonalidade'), row=3, col=1)
            fig.add_trace(go.Scatter(x=df_seasonal.index, y=decomposition.resid, mode='lines', name='Resíduo'), row=4, col=1)
            
            fig.update_layout(height=800, title_text=f"Análise de Decomposição Sazonal para {product_name}", showlegend=False)
            fig.update_xaxes(rangeslider_visible=False)
            return fig
        except Exception as e:
            print(f"Erro na decomposição sazonal para {product_name}: {e}")
            fig = go.Figure().update_layout(title=f"{product_name} - Erro na Análise Sazonal: {e}", xaxis_visible=False, yaxis_visible=False)
            return fig

    @staticmethod
    def create_correlation_plot(dfs, product_names):
        if len(dfs) < 2:
            fig = go.Figure().update_layout(title="Selecione ao menos 2 produtos para Comparação", xaxis_visible=False, yaxis_visible=False)
            return fig
        
        # Alinhar os DataFrames pela data para correlação
        # Pega a data mínima e máxima combinada de todos os DFs
        min_date = min(df['date'].min() for df in dfs)
        max_date = max(df['date'].max() for df in dfs)
        combined_df = pd.DataFrame({'date': pd.date_range(start=min_date, end=max_date)})
        
        for name, df in zip(product_names, dfs):
            # Garante que 'date' é o índice para o merge e seleciona apenas 'price'
            df_temp = df[['date', 'price']].set_index('date')
            df_temp = df_temp.rename(columns={'price': name})
            combined_df = pd.merge(combined_df, df_temp, on='date', how='outer')
        
        combined_df = combined_df.set_index('date').sort_index()

        fig = go.Figure()
        
        # Adiciona traces para cada produto
        for name in product_names:
            if name in combined_df.columns:
                fig.add_trace(
                    go.Scatter(
                        x=combined_df.index, 
                        y=combined_df[name], 
                        mode='lines', 
                        name=f'Preço {name} (BRL)',
                        hovertemplate='<b>%{x|%d/%m/%Y}</b><br>' + f'{name}: R$' + '%{y:,.2f}<extra></extra>'
                    )
                )

        fig.update_layout(
            title='Comparativo de Preços entre Produtos (BRL)',
            xaxis_title='Data',
            yaxis_title='Preço (BRL)',
            hovermode='x unified',
            template='plotly_white',
            height=600
        )
        
        return fig
