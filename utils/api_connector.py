import pandas as pd
from datetime import datetime, timedelta
import os
import json
import investpy # A NOVA BIBLIOTECA
import time

class CepeaAPI: # O nome da classe ainda pode ser CepeaAPI, mas ela buscará dados de outras fontes
    def __init__(self):
        self.cache_dir = "data/commodity_cache" # Novo nome para o cache
        self.products_cache = "data/products_list.json"
        os.makedirs(self.cache_dir, exist_ok=True)
        os.makedirs(os.path.dirname(self.products_cache), exist_ok=True)
        
        # Mapeamento para produtos investpy.
        # VOCÊ PODE PRECISAR AJUSTAR OS NOMES DOS SÍMBOLOS/COMMODITIES do investpy!
        # Consulte a documentação do investpy ou use investpy.get_commodities_list() para encontrar os nomes exatos.
        # Ex: "Live Cattle" é o mais próximo de Boi Gordo.
        self.product_investpy_map = {
            "BGI": {"investpy_name": "Live Cattle", "country": "United States", "currency": "USD", "unit": "@"},
            "MIL": {"investpy_name": "US Corn", "country": "United States", "currency": "USD", "unit": "bu"}, # Bushel, pode ser convertido
            "SOJ": {"investpy_name": "US Soybeans", "country": "United States", "currency": "USD", "unit": "bu"},
            "CAF": {"investpy_name": "Coffee", "country": "United States", "currency": "USD", "unit": "lb"}, # Libra, pode ser convertido
            "SUC": {"investpy_name": "Sugar", "country": "United States", "currency": "USD", "unit": "lb"},
            # Você pode buscar outros países ou tipos de contratos se houver.
            # investpy.get_commodities_list() pode te ajudar a encontrar os nomes exatos.
        }

    def _get_cache_path(self, product_code, start_date, end_date):
        filename = f"{product_code}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.parquet"
        return os.path.join(self.cache_dir, filename)

    def _save_to_cache(self, df, cache_path):
        try:
            df.to_parquet(cache_path, index=False)
        except Exception as e:
            print(f"Erro ao salvar cache: {e}")

    def _load_from_cache(self, cache_path):
        if os.path.exists(cache_path):
            try:
                return pd.read_parquet(cache_path)
            except Exception as e:
                print(f"Erro ao ler cache: {e}")
        return None

    def _get_exchange_rate(self, date):
        """Obtém taxa de câmbio histórica do Dólar (compra) do Banco Central do Brasil."""
        if date > datetime.now().date():
            return 5.0 # Fallback para datas futuras
        try:
            date_str = date.strftime('%m-%d-%Y')
            url = f"https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarDia(dataCotacao=@dataCotacao)?@dataCotacao='{date_str}'&$format=json"
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            data = response.json()
            if data and 'value' in data and len(data['value']) > 0:
                return float(data['value'][0]['cotacaoCompra'])
            else:
                return 5.0
        except Exception as e:
            # print(f"Erro ao obter taxa de câmbio para {date.strftime('%Y-%m-%d')}: {e}. Usando fallback.") # Descomente para depurar
            return 5.0

    def get_available_products(self):
        """Retorna lista de produtos disponíveis com cache."""
        try:
            if os.path.exists(self.products_cache):
                with open(self.products_cache, 'r', encoding='utf-8') as f:
                    return json.load(f)
            
            products_list = []
            for code, info in self.product_investpy_map.items():
                products_list.append({
                    "code": code,
                    "name": info["investpy_name"], 
                    "unit": info["unit"], 
                    "currency": info["currency"]
                })
            
            with open(self.products_cache, 'w', encoding='utf-8') as f:
                json.dump(products_list, f, ensure_ascii=False, indent=2)
            
            return products_list
            
        except Exception as e:
            print(f"Erro ao carregar produtos: {e}")
            return []

    def get_historical_prices(self, product_code, start_date, end_date):
        """Obtém preços históricos usando investpy, com tratamento de moeda e cache."""
        try:
            cache_path = self._get_cache_path(product_code, start_date, end_date)
            cached_data = self._load_from_cache(cache_path)
            
            if cached_data is not None and not cached_data.empty:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Retornando dados do cache para {product_code}")
                return cached_data

            product_info = self.product_investpy_map.get(product_code)
            if not product_info:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] ERRO: Produto {product_code} não mapeado para investpy.")
                return pd.DataFrame()

            investpy_name = product_info["investpy_name"]
            country = product_info["country"] # Geralmente 'United States' para commodities futuras
            source_currency = product_info["currency"]

            print(f"[{datetime.now().strftime('%H:%M:%S')}] Buscando dados com investpy para {investpy_name} de {start_date.strftime('%Y-%m-%d')} a {end_date.strftime('%Y-%m-%d')}")

            # Busca dados usando investpy
            # investpy.get_commodity_historical_data() busca pelo nome da commodity (investpy_name)
            # Pode precisar ajustar o nome da commodity se não for encontrado.
            df = investpy.get_commodity_historical_data(
                commodity=investpy_name,
                country=country,
                from_date=start_date.strftime('%d/%m/%Y'),
                to_date=end_date.strftime('%d/%m/%Y')
            )
            
            if df.empty:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] investpy não retornou dados para {investpy_name}.")
                return pd.DataFrame()

            # Normaliza as colunas do DataFrame retornado pelo investpy
            df = df.rename(columns={'Date': 'date', 'Close': 'price'}) # 'Close' é o preço de fechamento
            df['date'] = pd.to_datetime(df['date'])
            df['product'] = product_code
            
            # Garante que 'price' é numérico
            df['price'] = pd.to_numeric(df['price'], errors='coerce')

            # Tratamento de moedas: investpy retorna a moeda do commodity (geralmente USD).
            # Vamos padronizar: 'price' será sempre BRL e 'price_usd' será USD.
            
            if source_currency == 'USD':
                df['price_usd'] = df['price'].copy() # O preço original do investpy é o USD
                df['exchange_rate'] = df['date'].apply(lambda x: self._get_exchange_rate(x.date()))
                df['price'] = df['price_usd'] * df['exchange_rate'] # Converte USD para BRL
            else: # Se a moeda da fonte for BRL (improvável para commodities do investpy, mas por segurança)
                df['price_usd'] = pd.NA # Não há preço em USD direto
                df['exchange_rate'] = pd.NA # Não há taxa de câmbio usada para conversão principal
            
            # Remover NaNs que podem ter surgido de datas ou preços inválidos
            df.dropna(subset=['date', 'price'], inplace=True)
            
            # Ordena por data
            df = df.sort_values(by='date').reset_index(drop=True)
            
            self._save_to_cache(df, cache_path)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Dados para {product_code} obtidos com investpy e salvos no cache. {len(df)} registros.")
            return df
            
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ERRO GERAL ao obter dados históricos com investpy para {product_code}: {e}")
            return pd.DataFrame()
