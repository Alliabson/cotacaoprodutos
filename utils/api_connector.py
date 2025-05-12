import requests
import pandas as pd
import os
from datetime import datetime, timedelta
import json
from dotenv import load_dotenv
import time
from bs4 import BeautifulSoup

load_dotenv()

class CepeaAPI:
    def __init__(self):
        self.base_url = "https://www.cepea.esalq.usp.br/br"
        self.cache_dir = "data/historical_cache"
        self.exchange_cache = "data/exchange_rates.parquet"
        os.makedirs(self.cache_dir, exist_ok=True)
        
    def _get_current_exchange_rate(self):
        """Obtém a cotação atual do dólar comercial"""
        try:
            url = "https://www.bcb.gov.br/conteudo/home-ptbr/indicadores/cambio/cambio.csv"
            response = requests.get(url, timeout=10)
            lines = response.text.split('\n')
            last_line = [l for l in lines if l.startswith('USD')][-1]
            rate = float(last_line.split(';')[3].replace(',', '.'))
            return rate
        except Exception:
            return 5.0  # Fallback
    
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
