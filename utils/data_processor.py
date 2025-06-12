import pandas as pd
import numpy as np

class DataProcessor:
        
    @staticmethod
    def add_moving_averages(df, window_sizes=[7, 30, 90]):
        """Adiciona médias móveis em BRL e USD"""
        # Garante que 'price' e 'price_usd' são numéricos
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        if 'price_usd' in df.columns:
            df['price_usd'] = pd.to_numeric(df['price_usd'], errors='coerce')

        for window in window_sizes:
            # Calcula MA para BRL
            df[f'ma_{window}'] = df['price'].rolling(window=window, min_periods=1).mean() # min_periods=1 para iniciar logo
            
            # Calcula MA para USD se a coluna existir e não for nula
            if 'price_usd' in df.columns and not df['price_usd'].isnull().all():
                df[f'ma_{window}_usd'] = df['price_usd'].rolling(window=window, min_periods=1).mean()
        return df
        
    @staticmethod
    def add_percentage_change(df):
        """Calcula variações percentuais"""
        # Garante que 'price' e 'price_usd' são numéricos
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        if 'price_usd' in df.columns:
            df['price_usd'] = pd.to_numeric(df['price_usd'], errors='coerce')

        df['pct_change'] = df['price'].pct_change() * 100
        df['pct_change_30d'] = df['price'].pct_change(periods=30) * 100
        
        if 'price_usd' in df.columns and not df['price_usd'].isnull().all():
            df['pct_change_usd'] = df['price_usd'].pct_change() * 100
            df['pct_change_30d_usd'] = df['price_usd'].pct_change(periods=30) * 100
        
        return df
        
    @staticmethod
    def prepare_analysis_data(df):
        """Pipeline completo de processamento"""
        df = df.copy()
        
        # Garante que 'date' é datetime
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df.dropna(subset=['date'], inplace=True) # Remove linhas com datas inválidas
            df.sort_values('date', inplace=True)
        else:
            raise ValueError("Dados devem conter coluna 'date'")

        # Garante que temos a coluna 'price'
        if 'price' not in df.columns or df['price'].isnull().all():
            raise ValueError("Dados devem conter a coluna 'price' com valores válidos após o carregamento.")

        # Processamento principal
        df = DataProcessor.add_moving_averages(df)
        df = DataProcessor.add_percentage_change(df)
        
        return df
