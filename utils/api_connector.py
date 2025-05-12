import requests
import pandas as pd
import os
from datetime import datetime
import json
from dotenv import load_dotenv
import time

load_dotenv()

class CepeaAPI:
    def __init__(self):
        self.base_url = "https://www.cepea.esalq.usp.br/br/consultas"
        self.api_key = os.getenv("CEPEA_API_KEY") or "SUA_CHAVE_API"
        self.cache_dir = "../data/historical_cache"
        os.makedirs(self.cache_dir, exist_ok=True)
    
    def _get_cache_path(self, product_code, start_date, end_date):
        return os.path.join(
            self.cache_dir,
            f"{product_code}_{start_date}_{end_date}.parquet"
        )
    
    def _save_to_cache(self, df, cache_path):
        df.to_parquet(cache_path)
    
    def _load_from_cache(self, cache_path):
        if os.path.exists(cache_path):
            return pd.read_parquet(cache_path)
        return None
    
    def get_historical_prices(self, product_code, start_date, end_date):
        cache_path = self._get_cache_path(product_code, start_date, end_date)
        cached_data = self._load_from_cache(cache_path)
        
        if cached_data is not None:
            return cached_data
        
        # Simulação de chamada à API (substituir pela API real)
        dates = pd.date_range(start=start_date, end=end_date)
        data = {
            "date": dates,
            "price": [100 + 10 * (i % 30) + 5 * (i // 30) for i in range(len(dates))],
            "product": [product_code] * len(dates)
        }
        
        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])
        
        # Simular delay da API
        time.sleep(1)
        
        self._save_to_cache(df, cache_path)
        return df
    
    def get_available_products(self):
        with open('../data/products_list.json', 'r') as f:
            return json.load(f)
