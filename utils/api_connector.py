import requests
import pandas as pd
import os
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time
from bs4 import BeautifulSoup

load_dotenv()

class CepeaAPI:
    def __init__(self):
        self.base_url = "https://www.cepea.esalq.usp.br/br"
        self.api_key = os.getenv("CEPEA_API_KEY", "default_key")
        self.cache_dir = "data/cepea_cache"
        self.products_cache = "data/products_list.json"
        os.makedirs(self.cache_dir, exist_ok=True)
        os.makedirs(os.path.dirname(self.products_cache), exist_ok=True)

    def _get_cache_path(self, product_code, start_date, end_date):
        """Gera caminho único para arquivo de cache"""
        filename = f"{product_code}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.parquet"
        return os.path.join(self.cache_dir, filename)

    def _save_to_cache(self, df, cache_path):
        """Salva DataFrame em cache no formato parquet"""
        try:
            df.to_parquet(cache_path)
        except Exception as e:
            print(f"Erro ao salvar cache: {e}")

    def _load_from_cache(self, cache_path):
        """Carrega dados do cache se existirem"""
        if os.path.exists(cache_path):
            try:
                return pd.read_parquet(cache_path)
            except Exception as e:
                print(f"Erro ao ler cache: {e}")
        return None

    def _get_exchange_rate(self, date):
        """Obtém taxa de câmbio histórica"""
        try:
            date_str = date.strftime('%m-%d-%Y')
            url = f"https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarDia(dataCotacao=@dataCotacao)?@dataCotacao='{date_str}'&$format=json"
            response = requests.get(url, timeout=5)
            data = response.json()
            return float(data['value'][0]['cotacaoCompra'])
        except Exception:
            return 5.0  # Valor fallback

    def get_available_products(self):
        """Retorna lista de produtos disponíveis com cache"""
        try:
            # Tenta carregar do cache
            if os.path.exists(self.products_cache):
                with open(self.products_cache, 'r', encoding='utf-8') as f:
                    return json.load(f)
            
            # Lista padrão (substituir por API real quando disponível)
            default_products = [
                {"code": "BGI", "name": "Boi Gordo", "unit": "@", "currency": "BRL"},
                {"code": "MIL", "name": "Milho", "unit": "sc 60kg", "currency": "BRL"},
                {"code": "SOJ", "name": "Soja", "unit": "sc 60kg", "currency": "BRL"},
                {"code": "CAF", "name": "Café Arábica", "unit": "sc 60kg", "currency": "USD"},
                {"code": "SUC", "name": "Açúcar Cristal", "unit": "sc 50kg", "currency": "USD"}
            ]
            
            # Salva no cache
            with open(self.products_cache, 'w', encoding='utf-8') as f:
                json.dump(default_products, f, ensure_ascii=False, indent=2)
            
            return default_products
            
        except Exception as e:
            print(f"Erro ao carregar produtos: {e}")
            return []

    def get_historical_prices(self, product_code, start_date, end_date):
        """Obtém preços históricos com tratamento de moeda"""
        try:
            cache_path = self._get_cache_path(product_code, start_date, end_date)
            cached_data = self._load_from_cache(cache_path)
            
            if cached_data is not None:
                return cached_data

            # Simulação de dados - SUBSTITUIR pela API real
            dates = pd.date_range(start=start_date, end=end_date)
            base_price = 300.0
            
            data = {
                "date": dates,
                "price": [base_price + 10 * (i % 7) - 5 * (i // 7) for i in range(len(dates))],
                "product": product_code
            }
            
            df = pd.DataFrame(data)
            df['date'] = pd.to_datetime(df['date'])
            
            # Adiciona câmbio para produtos em USD
            product_info = next((p for p in self.get_available_products() if p['code'] == product_code), None)
            if product_info and product_info.get('currency') == 'USD':
                df['exchange_rate'] = df['date'].apply(self._get_exchange_rate)
                df['price_brl'] = df['price'] * df['exchange_rate']
            
            time.sleep(0.5)  # Simula delay de API
            
            self._save_to_cache(df, cache_path)
            return df
            
        except Exception as e:
            print(f"Erro ao obter dados históricos: {e}")
            return pd.DataFrame()

    def _call_cepea_api(self, endpoint, params=None):
        """Método genérico para chamar a API CEPEA"""
        try:
            url = f"{self.base_url}/{endpoint}"
            headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key != "default_key" else {}
            
            response = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=10
            )
            response.raise_for_status()
            
            return response.json()
        except Exception as e:
            print(f"Erro na chamada à API: {e}")
            return None
