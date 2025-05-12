import pandas as pd
import numpy as np

class DataProcessor:
    @staticmethod
    def _convert_to_brl(df):
        """Garante que todos os valores monetários estão em BRL"""
        if 'price_usd' in df.columns and 'exchange_rate' in df.columns:
            df['price_brl'] = df['price']  # Salva original
            df['price'] = df['price_usd'] * df['exchange_rate']
        return df
    
    @staticmethod
    def add_moving_averages(df, window_sizes=[7, 30, 90]):
        """Adiciona médias móveis em BRL e USD"""
        for window in window_sizes:
            df[f'ma_{window}'] = df['price'].rolling(window=window).mean()
            if 'price_usd' in df.columns:
                df[f'ma_{window}_usd'] = df['price_usd'].rolling(window=window).mean()
        return df
    
    @staticmethod
    def add_percentage_change(df):
        """Calcula variações percentuais"""
        df['pct_change'] = df['price'].pct_change() * 100
        df['pct_change_30d'] = df['price'].pct_change(periods=30) * 100
        
        if 'price_usd' in df.columns:
            df['pct_change_usd'] = df['price_usd'].pct_change() * 100
            df['pct_change_30d_usd'] = df['price_usd'].pct_change(periods=30) * 100
        
        return df
    
    @staticmethod
    def prepare_analysis_data(df):
        """Pipeline completo de processamento"""
        df = df.copy()
        
        # Garante que temos colunas essenciais
        if 'price' not in df.columns:
            raise ValueError("Dados devem conter coluna 'price'")
        
        # Processamento principal
        df = DataProcessor.add_moving_averages(df)
        df = DataProcessor.add_percentage_change(df)
        
        # Ordena por data
        df.sort_values('date', inplace=True)
        
        return df
