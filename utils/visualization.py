import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from plotly.subplots import make_subplots

class Visualizer:
    @staticmethod
    def create_historical_plot(df, product_name):
        """Cria gráfico histórico com múltiplas camadas"""
        fig = go.Figure()
        
        # Linha principal
        fig.add_trace(go.Scatter(
            x=df['date'], y=df['price'],
            mode='lines',
            name='Preço Diário',
            line=dict(color='royalblue', width=2),
            hovertemplate='<b>%{x|%d/%m/%Y}</b><br>R$ %{y:.2f}<extra></extra>'
        ))
        
        # Médias móveis
        if 'ma_30' in df.columns:
            fig.add_trace(go.Scatter(
                x=df['date'], y=df['ma_30'],
                mode='lines',
                name='Média 30 dias',
                line=dict(color='orange', width=2, dash='dot'),
                hoverinfo='skip'
            ))
        
        # Extremos
        if 'is_max' in df.columns:
            max_df = df[df['is_max']]
            fig.add_trace(go.Scatter(
                x=max_df['date'], y=max_df['price'],
                mode='markers',
                name='Máximos Locais',
                marker=dict(color='red', size=8, symbol='triangle-up'),
                hovertemplate='<b>MÁXIMO</b><br>%{x|%d/%m/%Y}<br>R$ %{y:.2f}<extra></extra>'
            ))
        
        # Layout
        fig.update_layout(
            title=f'Histórico de Preços - {product_name}',
            xaxis_title='Data',
            yaxis_title='Preço (R$)',
            hovermode='x unified',
            template='plotly_white',
            height=600
        )
        
        return fig
    
    @staticmethod
    def create_seasonal_plot(df, product_name):
        """Cria visualização sazonal"""
        if 'seasonal' not in df.columns:
            raise ValueError("Dados não contêm decomposição sazonal")
            
        # Heatmap sazonal
        df['year'] = df['date'].dt.year
        df['month'] = df['date'].dt.month
        monthly = df.groupby(['year', 'month'])['price'].mean().reset_index()
        
        fig = px.imshow(
            monthly.pivot('month', 'year', 'price'),
            labels=dict(x="Ano", y="Mês", color="Preço"),
            title=f"Variação Sazonal - {product_name}",
            color_continuous_scale='Viridis'
        )
        
        return fig
    
    @staticmethod
    def create_price_distribution(df, product_name):
        """Cria visualização de distribuição de preços"""
        fig = make_subplots(rows=1, cols=2, subplot_titles=(
            f"Distribuição de Preços - {product_name}", 
            "Variação Percentual"
        ))
        
        # Histograma
        fig.add_trace(
            go.Histogram(
                x=df['price'],
                nbinsx=50,
                name='Distribuição',
                marker_color='skyblue'
            ),
            row=1, col=1
        )
        
        # Boxplot
        fig.add_trace(
            go.Box(
                y=df['price'],
                name='Boxplot',
                boxpoints='outliers',
                marker_color='lightseagreen'
            ),
            row=1, col=2
        )
        
        fig.update_layout(
            showlegend=False,
            template='plotly_white'
        )
        
        return fig
    
    @staticmethod
    def create_correlation_plot(df_list, product_names):
        """Cria matriz de correlação entre produtos"""
        if len(df_list) < 2:
            raise ValueError("Necessário pelo menos 2 produtos para correlação")
            
        # Criar DataFrame combinado
        combined = pd.concat([
            df.set_index('date')['price'].rename(name)
            for df, name in zip(df_list, product_names)
        ], axis=1).dropna()
        
        # Matriz de correlação
        corr = combined.corr()
        
        fig = go.Figure()
        fig.add_trace(
            go.Heatmap(
                z=corr.values,
                x=product_names,
                y=product_names,
                colorscale='RdBu',
                zmin=-1,
                zmax=1,
                text=corr.round(2).values,
                texttemplate="%{text}"
            )
        )
        
        fig.update_layout(
            title='Correlação entre Produtos',
            xaxis_title='Produto',
            yaxis_title='Produto',
            height=600
        )
        
        return fig
