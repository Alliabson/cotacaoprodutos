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
        
        # Preço em USD (se disponível)
        if 'price_usd' in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df['date'], y=df['price_usd'],
                    name='Preço (USD)',
                    line=dict(color='green'),
                    hovertemplate='<b>%{x|%d/%m/%Y}</b><br>US$ %{y:,.2f}<extra></extra>'
                ),
                secondary_y=True
            )
        
        # Médias móveis
        if 'ma_30' in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df['date'], y=df['ma_30'],
                    name='Média 30d (BRL)',
                    line=dict(color='blue', dash='dot'),
                    hoverinfo='skip'
                ),
                secondary_y=False
            )
            
            if 'ma_30_usd' in df.columns:
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
            yaxis_title=dict(text='Preço (BRL)', standoff=30),
            hovermode='x unified',
            template='plotly_white',
            height=600
        )
        
        fig.update_yaxes(
            title_text='Preço (USD)',
            secondary_y=True
        )
        
        return fig
