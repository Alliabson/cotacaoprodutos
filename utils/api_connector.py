import requests
import pandas as pd
import os
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time
from bs4 import BeautifulSoup
import re 

load_dotenv()

class CepeaAPI:
    def __init__(self):
        self.base_url_cotacoes = "https://www.cepea.esalq.usp.br/br/indicador/"
        self.cache_dir = "data/cepea_cache"
        self.products_cache = "data/products_list.json"
        os.makedirs(self.cache_dir, exist_ok=True)
        os.makedirs(os.path.dirname(self.products_cache), exist_ok=True)
        
        # Mapeamento do nome do produto na URL, do ID do indicador e da moeda.
        # VOCÊ PRECISA CONFERIR ESTES IDs E NOMES NA URL PARA CADA PRODUTO!
        self.product_info_map = {
            "BGI": {"url_name": "boi-gordo", "id_indicador": "2", "currency": "BRL"},
            "MIL": {"url_name": "milho", "id_indicador": "7", "currency": "BRL"},
            "SOJ": {"url_name": "soja", "id_indicador": "14", "currency": "BRL"},
            "CAF": {"url_name": "cafe", "id_indicador": "4", "currency": "USD"}, # Café Arábica
            "SUC": {"url_name": "acucar", "id_indicador": "1", "currency": "BRL"} # Açúcar Cristal (o CEPEA tem em BRL e USD, aqui assumindo BRL padrão)
            # Para Açúcar Cristal em USD, pode ser que o id_indicador seja diferente ou precise de outra tabela.
            # {"code": "SUC_USD", "name": "Açúcar Cristal (USD)", "url_name": "acucar", "id_indicador": "??", "currency": "USD"}
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
        if date > datetime.now().date():
            return 5.0 
        try:
            date_str = date.strftime('%m-%d-%Y')
            url = f"https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarDia(dataCotacao=@dataCotacao)?@dataCotacao='{date_str}'&$format=json"
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            data = response.json()
            if data and 'value' in data and len(data['value']) > 0:
                # Retorna a cotação de compra
                return float(data['value'][0]['cotacaoCompra'])
            else:
                print(f"Aviso: Nenhuma cotação de dólar encontrada para a data {date_str}. Usando fallback.")
                return 5.0
        except Exception as e:
            print(f"Erro ao obter taxa de câmbio para {date.strftime('%Y-%m-%d')}: {e}. Usando fallback.")
            return 5.0

    def get_available_products(self):
        """Retorna lista de produtos disponíveis com cache."""
        try:
            if os.path.exists(self.products_cache):
                with open(self.products_cache, 'r', encoding='utf-8') as f:
                    return json.load(f)
            
            products_list = []
            for code, info in self.product_info_map.items():
                products_list.append({
                    "code": code,
                    "name": info["url_name"].replace("-", " ").title(), # Converte "boi-gordo" para "Boi Gordo"
                    "unit": self._get_default_unit(code), # Função auxiliar para unidades
                    "currency": info["currency"]
                })
            
            with open(self.products_cache, 'w', encoding='utf-8') as f:
                json.dump(products_list, f, ensure_ascii=False, indent=2)
            
            return products_list
            
        except Exception as e:
            print(f"Erro ao carregar produtos: {e}")
            return []

    def _get_default_unit(self, product_code):
        """Retorna unidade padrão para exibição."""
        units = {
            "BGI": "@",
            "MIL": "sc 60kg",
            "SOJ": "sc 60kg",
            "CAF": "sc 60kg",
            "SUC": "sc 50kg"
        }
        return units.get(product_code, "unidade")

    def _scrape_cepea_data(self, product_code, start_date, end_date):
        """
        Realiza o web scraping do site do CEPEA para obter dados históricos.
        Esta função é específica para o formato de tabela de 'Série de Preços' do CEPEA.
        """
        product_info = self.product_info_map.get(product_code)
        if not product_info:
            print(f"Informações de URL/ID para o produto {product_code} não configuradas.")
            return pd.DataFrame()

        url_name = product_info["url_name"]
        id_indicador = product_info["id_indicador"]
        
        # A URL para a série de preços histórica com filtro por indicador
        # NOTA: O CEPEA tem um filtro de data na página, mas não diretamente via parâmetros GET.
        # Precisaremos carregar a página e tentar extrair a tabela completa, e então filtrar.
        url_to_scrape = f"{self.base_url_cotacoes}{url_name}.aspx?id_indicador={id_indicador}"
        
        print(f"Tentando scraping de: {url_to_scrape} para {product_code}")

        try:
            response = requests.get(url_to_scrape, timeout=15) # Aumenta o timeout
            response.raise_for_status() # Lança exceção para status 4xx/5xx
            soup = BeautifulSoup(response.text, 'html.parser')

            # Tenta encontrar a tabela que contém os dados históricos
            # O CEPEA geralmente tem tabelas com colunas como "Data", "VALOR R$*", "VAR./DIA", "VALOR US$*"
            # Vamos procurar por uma tabela com essas colunas.
            
            # Uma forma robusta é usar pd.read_html e depois identificar a tabela correta
            dfs = pd.read_html(response.text, decimal=',', thousands='.', flavor='bs4') # flavor='bs4' usa BeautifulSoup
            
            # Tentando encontrar a tabela que mais se parece com a de histórico
            target_df = pd.DataFrame()
            for df_candidate in dfs:
                # Critérios para identificar a tabela correta
                if 'Data' in df_candidate.columns and ('VALOR R$*' in df_candidate.columns or 'VALOR US$*' in df_candidate.columns or 'VALOR R$' in df_candidate.columns):
                    target_df = df_candidate
                    break
            
            if target_df.empty:
                print(f"Aviso: Tabela de histórico não encontrada na página para {product_code} ({url_to_scrape}).")
                return pd.DataFrame()

            df_temp = target_df.copy() # Trabalha em uma cópia

            # Renomear e limpar colunas
            col_mapping = {}
            for col in df_temp.columns:
                if 'Data' in col: col_mapping[col] = 'date'
                elif 'VALOR R$*' in col or 'VALOR R$' in col: col_mapping[col] = 'price' # Preço em BRL
                elif 'VALOR US$*' in col or 'VALOR US$' in col: col_mapping[col] = 'price_usd_scraped' # Preço em USD (raspadinho)
                # Adicione mais mapeamentos se houver outras colunas importantes, como variação

            df_temp = df_temp.rename(columns=col_mapping)
            
            # Assegura que as colunas essenciais existem
            if 'date' not in df_temp.columns or ('price' not in df_temp.columns and 'price_usd_scraped' not in df_temp.columns):
                print(f"Erro: Colunas 'date' ou 'price'/'price_usd' não encontradas após renomeação para {product_code}.")
                return pd.DataFrame()

            # Converte 'date' para datetime
            df_temp['date'] = pd.to_datetime(df_temp['date'], format='%d/%m/%Y', errors='coerce')
            df_temp.dropna(subset=['date'], inplace=True)

            # Limpa e converte 'price' e 'price_usd_scraped' para numérico
            # Remove pontos, substitui vírgulas por pontos e converte para float
            for col in ['price', 'price_usd_scraped']:
                if col in df_temp.columns:
                    df_temp[col] = df_temp[col].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                    df_temp[col] = pd.to_numeric(df_temp[col], errors='coerce')
            
            # Remove linhas com valores nulos nos preços
            df_temp.dropna(subset=['price'] if 'price' in df_temp.columns else ['price_usd_scraped'], inplace=True)

            # Filtra pelo período desejado
            df_temp = df_temp[(df_temp['date'] >= start_date) & (df_temp['date'] <= end_date)].copy()
            
            if not df_temp.empty:
                df_temp['product'] = product_code
                
                # Se o produto for em USD na fonte (CEPEA), 'price' do scraper é o USD.
                # Vamos padronizar: 'price' será sempre BRL e 'price_usd' será USD.
                if product_info["currency"] == 'USD' and 'price_usd_scraped' in df_temp.columns:
                    # O 'price' raspado na coluna 'VALOR R$*' é o preço em BRL
                    # O 'price_usd_scraped' é o preço em USD
                    df_temp['price_usd'] = df_temp['price_usd_scraped']
                    # O 'price' já vem como BRL da coluna 'VALOR R$*', então não precisamos converter com câmbio aqui
                    # Apenas manter a coluna 'price' como BRL
                    df_temp.drop(columns=['price_usd_scraped'], inplace=True)
                    
                elif product_info["currency"] == 'BRL':
                    # Se o produto é em BRL, 'price' já é BRL.
                    # As colunas de USD (se existirem) podem ser nulas ou removidas para evitar confusão
                    if 'price_usd_scraped' in df_temp.columns:
                        df_temp['price_usd'] = df_temp['price_usd_scraped']
                        df_temp.drop(columns=['price_usd_scraped'], inplace=True)
                    else:
                        df_temp['price_usd'] = None # Ou pd.NA
                
                # Garante que as colunas finais estão presentes
                final_cols = ['date', 'price', 'product']
                if 'price_usd' in df_temp.columns:
                    final_cols.append('price_usd')

                return df_temp[final_cols]
            
        except requests.exceptions.RequestException as req_err:
            print(f"Erro de requisição ao CEPEA para {product_code} ({url_to_scrape}): {req_err}")
        except Exception as e:
            print(f"Erro inesperado no scraping para {product_code} ({url_to_scrape}): {e}")
            # print(response.text) # Descomente para depurar o HTML completo
            
        return pd.DataFrame()

    def get_historical_prices(self, product_code, start_date, end_date):
        """Obtém preços históricos com tratamento de moeda e cache."""
        try:
            cache_path = self._get_cache_path(product_code, start_date, end_date)
            cached_data = self._load_from_cache(cache_path)
            
            if cached_data is not None and not cached_data.empty:
                print(f"Retornando dados do cache para {product_code}")
                # Certifica-se que as colunas 'price_usd' e 'exchange_rate' estão presentes
                # se o produto original for USD e foi cacheado corretamente
                if self.product_info_map.get(product_code, {}).get("currency") == "USD" and 'price_usd' not in cached_data.columns:
                    # Se o cache não tiver price_usd e for um produto USD, refaz o cálculo
                    print(f"Aviso: Cache de {product_code} (USD) não contém 'price_usd'. Recalculando.")
                    cached_data['price_usd'] = cached_data['price'] / cached_data['exchange_rate'] if 'exchange_rate' in cached_data.columns and not cached_data['exchange_rate'].isnull().all() else None
                return cached_data

            # Se não houver cache, tenta o scraping
            print(f"Iniciando scraping para {product_code} de {start_date.strftime('%Y-%m-%d')} a {end_date.strftime('%Y-%m-%d')}")
            df = self._scrape_cepea_data(product_code, start_date, end_date)
            
            if df.empty:
                print(f"Scraping não retornou dados para {product_code}. Retornando DataFrame vazio.")
                return pd.DataFrame()

            # Garantir tipos de dados corretos após scraping
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df['price'] = pd.to_numeric(df['price'], errors='coerce')
            if 'price_usd' in df.columns:
                df['price_usd'] = pd.to_numeric(df['price_usd'], errors='coerce')
            
            df.dropna(subset=['date', 'price'], inplace=True) # Remove linhas com datas ou preços inválidos
            
            # Adiciona câmbio se necessário (se o produto é BRL mas queremos USD também ou vice-versa)
            # A lógica de _scrape_cepea_data já tenta deixar 'price' como BRL e 'price_usd' como USD.
            # Aqui, garantimos que 'exchange_rate' está sempre lá para produtos USD,
            # ou para permitir a conversão de BRL para USD se necessário.
            
            product_currency_type = self.product_info_map.get(product_code, {}).get('currency')
            
            if product_currency_type == 'BRL' and 'price_usd' not in df.columns or df['price_usd'].isnull().all():
                 # Se é BRL e não tem USD, calcula USD com base na taxa de câmbio
                 df['exchange_rate'] = df['date'].apply(lambda x: self._get_exchange_rate(x.date())) # Usa .date() para _get_exchange_rate
                 df['price_usd'] = df['price'] / df['exchange_rate'] if 'exchange_rate' in df.columns else None
            elif product_currency_type == 'USD' and 'price' not in df.columns or df['price'].isnull().all():
                 # Se é USD e não tem BRL, calcula BRL com base na taxa de câmbio
                 df['exchange_rate'] = df['date'].apply(lambda x: self._get_exchange_rate(x.date()))
                 df['price'] = df['price_usd'] * df['exchange_rate'] if 'price_usd' in df.columns and 'exchange_rate' in df.columns else None
            else:
                # Para produtos que já vêm com ambas as moedas ou apenas uma e não precisam de conversão secundária.
                # Ou para produtos USD que já tem price_usd e price (BRL) raspados.
                # Garante que exchange_rate exista se price_usd existe.
                if 'price_usd' in df.columns and ('exchange_rate' not in df.columns or df['exchange_rate'].isnull().all()):
                     df['exchange_rate'] = df['date'].apply(lambda x: self._get_exchange_rate(x.date()))

            # Limpar NaNs que podem surgir das conversões
            df.dropna(subset=['price'], inplace=True)
            
            self._save_to_cache(df, cache_path)
            return df
            
        except Exception as e:
            print(f"Erro geral ao obter dados históricos para {product_code}: {e}")
            return pd.DataFrame()
