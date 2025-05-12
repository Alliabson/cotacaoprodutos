import requests
import pandas as pd
import os
from datetime import datetime, timedelta
import json
from dotenv import load_dotenv
import time
from bs4 import BeautifulSoup

load_dotenv()

import requests
import pandas as pd
import os
import json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

class CepeaAPI:
    def __init__(self):
        self.base_url = "https://www.cepea.esalq.usp.br/br"
        self.products_cache = "data/products_cache.json"
        os.makedirs(os.path.dirname(self.products_cache), exist_ok=True)

    def get_available_products(self):
        """Retorna a lista de produtos disponíveis com códigos"""
        try:
            # Tenta carregar do cache
            if os.path.exists(self.products_cache):
                with open(self.products_cache, 'r') as f:
                    return json.load(f)
            
            # Lista de produtos padrão (substitua pela chamada real à API)
            default_products = [
                {"code": "BGI", "name": "Boi Gordo", "unit": "arroba"},
                {"code": "MIL", "name": "Milho", "unit": "saca 60kg"},
                {"code": "SOJ", "name": "Soja", "unit": "saca 60kg"},
                {"code": "CAF", "name": "Café Arábica", "unit": "saca 60kg"},
                {"code": "SUC", "name": "Açúcar Cristal", "unit": "saca 50kg"}
            ]
            
            # Salva no cache
            with open(self.products_cache, 'w') as f:
                json.dump(default_products, f)
            
            return default_products
            
        except Exception as e:
            print(f"Erro ao carregar produtos: {e}")
            return []
    
    def _get_historical_exchange(self, date):
        """Obtém cotação histórica do BCB"""
        try:
            date_str = date.strftime('%m-%d-%Y')
            url = f"https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarDia(dataCotacao=@dataCotacao)?@dataCotacao='{date_str}'&$format=json"
            response = requests.get(url, timeout=5)
            data = response.json()
            return float(data['value'][0]['cotacaoCompra'])
        except Exception:
            return self._get_current_exchange_rate()
    
    def _parse_cepea_data(self, html_content):
        """Parseia os dados do HTML do CEPEA"""
        soup = BeautifulSoup(html_content, 'html.parser')
        table = soup.find('table', {'class': 'tabela'})
        
        data = []
        for row in table.find_all('tr')[1:]:  # Pula cabeçalho
            cols = row.find_all('td')
            if len(cols) >= 5:
                date = datetime.strptime(cols[0].text.strip(), '%d/%m/%Y').date()
                price_brl = float(cols[1].text.strip().replace('.', '').replace(',', '.'))
                daily_var = cols[2].text.strip()
                monthly_var = cols[3].text.strip()
                price_usd = float(cols[4].text.strip().replace(',', '.')) if cols[4].text.strip() else None
                
                data.append({
                    'date': date,
                    'price': price_brl,
                    'daily_var': daily_var,
                    'monthly_var': monthly_var,
                    'price_usd': price_usd
                })
        
        return pd.DataFrame(data)
    
    def get_historical_prices(self, product_code, start_date, end_date):
        cache_path = self._get_cache_path(product_code, start_date, end_date)
        
        # Tenta carregar do cache
        if os.path.exists(cache_path):
            return pd.read_parquet(cache_path)
        
        try:
            # Simulação - substituir pela chamada real à API do CEPEA
            dates = pd.date_range(start=start_date, end=end_date)
            base_price = 300.0  # Valor base para simulação
            
            data = []
            for i, date in enumerate(dates):
                price_brl = base_price + (10 * (i % 7)) - (5 * (i // 7))
                exchange_rate = self._get_historical_exchange(date)
                price_usd = price_brl / exchange_rate
                
                data.append({
                    'date': date,
                    'price': price_brl,
                    'price_usd': price_usd,
                    'exchange_rate': exchange_rate
                })
            
            df = pd.DataFrame(data)
            df.to_parquet(cache_path)
            return df
            
        except Exception as e:
            print(f"Erro ao obter dados: {e}")
            return pd.DataFrame()
