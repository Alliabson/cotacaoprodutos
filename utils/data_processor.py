import pandas as pd
import numpy as np

class DataProcessor:
    @staticmethod
    def add_moving_averages(df, window_sizes=[7, 30, 90]):
        """Adiciona médias móveis ao DataFrame"""
        for window in window_sizes:
            df[f'ma_{window}'] = df['price'].rolling(window=window).mean()
        return df
    
    @staticmethod
    def add_percentage_change(df):
        """Calcula variação percentual"""
        df['pct_change'] = df['price'].pct_change() * 100
        df['pct_change_30d'] = df['price'].pct_change(periods=30) * 100
        return df
    
    @staticmethod
    def detect_extremes(df, window=365):
        """Identifica máximos e mínimos locais"""
        df['rolling_max'] = df['price'].rolling(window=window).max()
        df['rolling_min'] = df['price'].rolling(window=window).min()
        df['is_max'] = df['price'] == df['rolling_max']
        df['is_min'] = df['price'] == df['rolling_min']
        return df
    
    @staticmethod
    def seasonal_decomposition(df, period=365):
        """Decomposição sazonal dos preços (opcional)"""
        try:
            from statsmodels.tsa.seasonal import seasonal_decompose
            result = seasonal_decompose(df.set_index('date')['price'], 
                                       period=period, 
                                       model='additive')
            df['trend'] = result.trend.values
            df['seasonal'] = result.seasonal.values
            df['residual'] = result.resid.values
        except ImportError:
            st.warning("statsmodels não instalado - decomposição sazonal desativada")
        return df
    
    @staticmethod
    def prepare_analysis_data(df):
        """Pipeline completo de processamento"""
        df = df.copy()
        df = DataProcessor.add_moving_averages(df)
        df = DataProcessor.add_percentage_change(df)
        df = DataProcessor.detect_extremes(df)
        if len(df) > 365:
            df = DataProcessor.seasonal_decomposition(df)
        return df
