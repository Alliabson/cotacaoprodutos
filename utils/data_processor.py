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
        """Decomposição sazonal com verificação de dados suficientes"""
        try:
            from statsmodels.tsa.seasonal import seasonal_decompose
            
            if len(df) < 2 * period:
                raise ValueError(f"Necessário mínimo de {2*period} dias para análise sazonal")
                
            result = seasonal_decompose(df.set_index('date')['price'], 
                                     period=period, 
                                     model='additive')
            df['trend'] = result.trend.values
            df['seasonal'] = result.seasonal.values
            df['residual'] = result.resid.values
            
        except ImportError:
            raise ImportError("statsmodels não instalado")
        except ValueError as e:
            raise ValueError(str(e))
        except Exception as e:
            raise Exception(f"Erro na decomposição: {str(e)}")
        
        return df
    
    @staticmethod
    def prepare_analysis_data(df):
        """Pipeline completo de processamento"""
        df = df.copy()
        df = DataProcessor.add_moving_averages(df)
        df = DataProcessor.add_percentage_change(df)
        df = DataProcessor.detect_extremes(df)
        if len(df) > 365:
            try:
                df = DataProcessor.seasonal_decomposition(df)
            except Exception as e:
                print(f"Warning: {str(e)}")
        return df
