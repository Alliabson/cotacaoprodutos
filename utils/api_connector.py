import requests
import pandas as pd
import os
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time
from bs4 import BeautifulSoup
import re # Importa regex para limpeza de strings

load_dotenv()

class CepeaAPI:
    def __init__(self):
        # Base URL para as páginas de indicadores históricos do CEPEA
        self.base_url_cotacoes = "https://www.cepea.esalq.usp.br/br/indicador/"
        self.cache_dir = "data/cepea_cache"
        self.products_cache = "data/products_list.json"
        os.makedirs(self.cache_dir, exist_ok=True)
        os.makedirs(os.path.dirname(self.products_cache), exist_ok=True)
        
        # Mapeamento do nome do produto na URL, do ID do indicador e da moeda.
        # ESSA É A PARTE MAIS CRÍTICA QUE VOCÊ PRECISA CONFERIR NO SITE DO CEPEA!
        # Vá em cada indicador no site do CEPEA, clique em "Série de Preços" e verifique a URL e o id_indicador.
        # As URLs e IDs aqui são as que funcionaram em testes recentes, mas podem mudar.
        self.product_info_map = {
            "BGI": {"url_name": "boi-gordo", "id_indicador": "2", "currency": "BRL"},
            "MIL": {"url_name": "milho", "id_indicador": "7", "currency": "BRL"},
            "SOJ": {"url_name": "soja", "id_indicador": "14", "currency": "BRL"},
            "CAF": {"url_name": "cafe", "id_indicador": "4", "currency": "USD"}, # Café Arábica (geralmente cotado em USD no CEPEA)
            "SUC": {"url_name": "acucar", "id_indicador": "1", "currency": "BRL"} # Açúcar Cristal (id_indicador 1 é geralmente BRL, confira no site)
            # Para outros produtos, adicione aqui, ex:
            # "TRIGO": {"url_name": "trigo", "id_indicador": "ID_CORRETO_DO_SITE", "currency": "BRL"},
        }

    def _get_cache_path(self, product_code, start_date, end_date):
        """Gera caminho único para arquivo de cache"""
        filename = f"{product_code}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.parquet"
        return os.path.join(self.cache_dir, filename)

    def _save_to_cache(self, df, cache_path):
        """Salva DataFrame em cache no formato parquet"""
        try:
            df.to_parquet(cache_path, index=False) # Adicionado index=False para não salvar o índice
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
        """Obtém taxa de câmbio histórica do Dólar (compra) do Banco Central do Brasil."""
        # O BCB não fornece dados para o futuro. Se a data for futura, retorna um fallback.
        if date > datetime.now().date():
            return 5.0 # Fallback para datas futuras
        try:
            date_str = date.strftime('%m-%d-%Y')
            url = f"https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarDia(dataCotacao=@dataCotacao)?@dataCotacao='{date_str}'&$format=json"
            response = requests.get(url, timeout=5)
            response.raise_for_status() # Lança um erro para status de resposta ruins (4xx ou 5xx)
            data = response.json()
            if data and 'value' in data and len(data['value']) > 0:
                # Retorna a cotação de compra (cotacaoCompra) que é mais relevante para o preço de produtos
                return float(data['value'][0]['cotacaoCompra'])
            else:
                return 5.0 # Valor fallback se a API não retornar dados
        except Exception as e:
            # print(f"Erro ao obter taxa de câmbio para {date.strftime('%Y-%m-%d')}: {e}. Usando fallback.") # Descomente para depurar
            return 5.0  # Valor fallback

    def get_available_products(self):
        """Retorna lista de produtos disponíveis com cache.
        Esta lista é definida manualmente com base em `product_info_map`.
        """
        try:
            # Tenta carregar do cache
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
            
            # Salva no cache
            with open(self.products_cache, 'w', encoding='utf-8') as f:
                json.dump(products_list, f, ensure_ascii=False, indent=2)
            
            return products_list
            
        except Exception as e:
            print(f"Erro ao carregar produtos: {e}")
            return []

    def _get_default_unit(self, product_code):
        """Retorna unidade padrão para exibição."""
        units = {
            "BGI": "@",        # Arroba
            "MIL": "sc 60kg",  # Saca de 60 kg
            "SOJ": "sc 60kg",  # Saca de 60 kg
            "CAF": "sc 60kg",  # Saca de 60 kg
            "SUC": "sc 50kg"   # Saca de 50 kg
        }
        return units.get(product_code, "unidade")

    def _scrape_cepea_data(self, product_code, start_date, end_date):
        """
        Realiza o web scraping do site do CEPEA para obter dados históricos.
        Esta função é específica para o formato de tabela de 'Série de Preços' do CEPEA.
        Tenta encontrar a tabela principal de histórico com base nos cabeçalhos esperados.
        """
        product_info = self.product_info_map.get(product_code)
        if not product_info:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ERRO: Informações de URL/ID para o produto {product_code} não configuradas em product_info_map.")
            return pd.DataFrame()

        url_name = product_info["url_name"]
        id_indicador = product_info["id_indicador"]
        
        # A URL para a série de preços histórica com filtro por indicador
        url_to_scrape = f"{self.base_url_cotacoes}{url_name}.aspx?id_indicador={id_indicador}"
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Iniciando scraping para {product_code} na URL: {url_to_scrape}")

        try:
            # Adiciona headers para simular um navegador e evitar bloqueios
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "Connection": "keep-alive"
            }
            response = requests.get(url_to_scrape, timeout=20, headers=headers) # Aumenta o timeout
            response.raise_for_status() # Lança exceção para status 4xx/5xx
            
            # --- PRINTS PARA DEPURAR O CONTEÚDO BRUTO ---
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Status da requisição: {response.status_code}")
            # Descomente a linha abaixo PARA VER O HTML COMPLETO (PODE SER MUITO GRANDE!)
            # with open(f"debug_html_{product_code}.html", "w", encoding="utf-8") as f:
            #     f.write(response.text)
            # print(f"[{datetime.now().strftime('%H:%M:%S')}] HTML salvo em debug_html_{product_code}.html para inspeção.")
            # print(f"[{datetime.now().strftime('%H:%M:%S')}] Primeiros 500 caracteres do HTML recebido:\n{response.text[:500]}")
            # ---------------------------------------------

            # Regex para encontrar cabeçalhos de colunas relevantes
            table_match_regex = r'Data|VALOR R\$|\bÀ Vista\b|\bPreço à vista\b|\bVALOR US\$|\bVAR\./|\bPreço'
            
            target_df = pd.DataFrame()
            try:
                # Tenta ler todas as tabelas que correspondem ao regex nos cabeçalhos
                dfs = pd.read_html(response.text, decimal=',', thousands='.', flavor='bs4', match=table_match_regex)
                
                print(f"[{datetime.now().strftime('%H:%M:%S')}] pd.read_html encontrou {len(dfs)} tabelas que correspondem ao regex.")
                
                # Percorre os DataFrames encontrados e tenta identificar o correto
                for df_candidate in dfs:
                    # Verifique se o DataFrame candidato tem as colunas esperadas para o histórico
                    has_date_col = any(re.search(r'data', str(col), re.IGNORECASE) for col in df_candidate.columns)
                    has_price_col_brl = any(re.search(r'VALOR R\$|\bÀ Vista\b|\bPreço à vista\b|\bPreço', str(col), re.IGNORECASE) for col in df_candidate.columns)
                    has_price_col_usd = any(re.search(r'VALOR US\$', str(col), re.IGNORECASE) for col in df_candidate.columns)
                    
                    if has_date_col and (has_price_col_brl or has_price_col_usd):
                        target_df = df_candidate
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] Tabela identificada com sucesso por pd.read_html.")
                        break
                
                if target_df.empty:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] AVISO: Nenhuma tabela relevante encontrada por pd.read_html. Tentando BeautifulSoup diretamente...")
                    # Fallback para buscar com BeautifulSoup se pd.read_html não encontrar
                    soup = BeautifulSoup(response.text, 'html.parser')
                    table_element = soup.find('table', class_='tabela-indicador') # Tenta classe comum do CEPEA
                    if not table_element:
                        # Tenta encontrar a tabela que contém um cabeçalho específico
                        table_element = soup.find('table', string=re.compile(r'Data.*VALOR R\$', re.DOTALL | re.IGNORECASE))
                    
                    if table_element:
                        # Converte a tabela BeautifulSoup para DataFrame
                        rows = []
                        # Pega os cabeçalhos (idealmente de thead)
                        headers_th = [th.text.strip() for th in table_element.find('thead').find_all('th')] if table_element.find('thead') else []
                        if not headers_th: # Se não achou thead, tenta pegar de tr/th na primeira linha
                            first_row = table_element.find('tr')
                            if first_row:
                                headers_th = [th.text.strip() for th in first_row.find_all(['th', 'td'])]
                        
                        # Pega as linhas de dados (tbody, ou todas as tr se não houver tbody)
                        data_rows = table_element.find('tbody').find_all('tr') if table_element.find('tbody') else table_element.find_all('tr')[1:] # Ignora primeira linha se headers_th for do tr

                        for tr in data_rows:
                            cells = [td.text.strip() for td in tr.find_all('td')]
                            if cells: # Garante que a linha não está vazia
                                rows.append(cells)
                        
                        if headers_th and rows and len(rows[0]) == len(headers_th):
                            target_df = pd.DataFrame(rows, columns=headers_th)
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] Tabela encontrada com BeautifulSoup e convertida usando cabeçalhos.")
                        elif rows and not headers_th and len(rows) > 1: # Tenta usar a primeira linha como cabeçalho se não achou thead/th
                             target_df = pd.DataFrame(rows[1:], columns=rows[0])
                             print(f"[{datetime.now().strftime('%H:%M:%S')}] Tabela encontrada com BeautifulSoup, usando primeira linha como cabeçalho.")
                        elif rows: # Se só tem linhas e nenhum cabeçalho claro
                            target_df = pd.DataFrame(rows)
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] Tabela encontrada com BeautifulSoup, sem cabeçalhos explícitos.")
                        else:
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] AVISO: Tabela encontrada com BeautifulSoup, mas sem linhas ou cabeçalhos válidos.")
                            return pd.DataFrame()
                    else:
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] AVISO: Nenhuma tabela de histórico encontrada com BeautifulSoup.")
                        return pd.DataFrame()

            except Exception as pd_html_err:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] ERRO ao tentar ler HTML com pandas/BeautifulSoup para {product_code}: {pd_html_err}")
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Conteúdo HTML para depuração (primeiros 1000 caracteres):\n{response.text[:1000]}")
                return pd.DataFrame()


            if target_df.empty:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] ERRO: DataFrame final vazio após tentativas de scraping para {product_code}.")
                return pd.DataFrame()

            df_temp = target_df.copy() # Trabalha em uma cópia

            # --- Mapeamento e Limpeza de Colunas ---
            # Esta parte é CRUCIAL e precisa ser adaptada se os nomes mudarem no site do CEPEA.
            # Use Regex para ser mais flexível com espaços, asteriscos, etc.
            
            # Encontra as colunas de Data e Preço (R$ e US$)
            date_col = next((col for col in df_temp.columns if re.search(r'data', str(col), re.IGNORECASE)), None)
            price_brl_col = next((col for col in df_temp.columns if re.search(r'VALOR R\$|\bÀ Vista\b|\bPreço à vista\b|\bPreço', str(col), re.IGNORECASE)), None)
            price_usd_col = next((col for col in df_temp.columns if re.search(r'VALOR US\$', str(col), re.IGNORECASE)), None)

            if not date_col:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] ERRO: Coluna 'Data' não encontrada no DataFrame raspado para {product_code}. Colunas disponíveis: {df_temp.columns.tolist()}")
                return pd.DataFrame()
            
            # Renomeia as colunas
            df_temp = df_temp.rename(columns={date_col: 'date'})
            if price_brl_col:
                df_temp = df_temp.rename(columns={price_brl_col: 'price'})
            if price_usd_col:
                df_temp = df_temp.rename(columns={price_usd_col: 'price_usd_scraped'})
            
            # Remove colunas que não serão usadas para o resultado final ou podem atrapalhar
            cols_to_drop = [col for col in df_temp.columns if col not in ['date', 'price', 'price_usd_scraped']]
            df_temp = df_temp.drop(columns=cols_to_drop, errors='ignore')

            # Converte 'date' para datetime
            df_temp['date'] = pd.to_datetime(df_temp['date'], format='%d/%m/%Y', errors='coerce')
            df_temp.dropna(subset=['date'], inplace=True) # Remove linhas com datas inválidas

            # Limpa e converte colunas de preço para numérico
            for col_name in ['price', 'price_usd_scraped']:
                if col_name in df_temp.columns:
                    # Converte para string para aplicar replace, depois para numérico
                    df_temp[col_name] = df_temp[col_name].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                    df_temp[col_name] = pd.to_numeric(df_temp[col_name], errors='coerce')
                   
            # Remove linhas com valores nulos nos preços que são cruciais
            if 'price' in df_temp.columns and not df_temp['price'].isnull().all():
                df_temp.dropna(subset=['price'], inplace=True)
            elif 'price_usd_scraped' in df_temp.columns and not df_temp['price_usd_scraped'].isnull().all():
                df_temp.dropna(subset=['price_usd_scraped'], inplace=True)
            else:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] ERRO: Nenhuma coluna de preço válida (R$ ou US$) encontrada após limpeza e conversão para {product_code}.")
                return pd.DataFrame()


            # Filtra pelo período desejado
            df_temp = df_temp[(df_temp['date'] >= start_date) & (df_temp['date'] <= end_date)].copy()
            
            if not df_temp.empty:
                df_temp['product'] = product_code
                
                # Padronização final das colunas de preço
                # Se 'price_usd_scraped' existe, significa que o CEPEA fornece o valor em USD diretamente.
                if 'price_usd_scraped' in df_temp.columns:
                    df_temp['price_usd'] = df_temp['price_usd_scraped']
                    # A coluna 'price' já deve conter o valor em BRL raspado.
                else:
                    df_temp['price_usd'] = pd.NA # Define como NA se não houver coluna USD raspada
                
                final_cols_to_keep = ['date', 'price', 'product']
                if 'price_usd' in df_temp.columns and not df_temp['price_usd'].isnull().all():
                     final_cols_to_keep.append('price_usd')
                
                df_final = df_temp[final_cols_to_keep].copy()
                
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Scraping para {product_code} concluído. {len(df_final)} registros. Amostra de Preços (BRL): {df_final['price'].head().tolist()}")
                return df_final
            else:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] AVISO: DataFrame vazio após filtro de datas para {product_code}.")
                return pd.DataFrame()
            
        except requests.exceptions.RequestException as req_err:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ERRO de requisição ao CEPEA para {product_code} ({url_to_scrape}): {req_err}")
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ERRO INESPERADO no scraping para {product_code} ({url_to_scrape}): {e}")
            # print(f"[{datetime.now().strftime('%H:%M:%S')}] Conteúdo HTML completo para depuração:\n{response.text}") # Descomentar para depuração profunda
            
        return pd.DataFrame()

    def get_historical_prices(self, product_code, start_date, end_date):
        """Obtém preços históricos com tratamento de moeda e cache."""
        try:
            cache_path = self._get_cache_path(product_code, start_date, end_date)
            cached_data = self._load_from_cache(cache_path)
            
            if cached_data is not None and not cached_data.empty:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Retornando dados do cache para {product_code}")
                # Reverifica e recalcula price_usd se o cache estiver incompleto para produtos USD
                product_currency_type = self.product_info_map.get(product_code, {}).get('currency')
                if product_currency_type == "USD" and ('price_usd' not in cached_data.columns or cached_data['price_usd'].isnull().all()):
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] AVISO: Cache de {product_code} (USD) incompleto. Recalculando USD.")
                    # Assume que 'price' no cache para produtos USD seria o BRL
                    cached_data['exchange_rate'] = cached_data['date'].apply(lambda x: self._get_exchange_rate(x.date()))
                    cached_data['price_usd'] = cached_data['price'] / cached_data['exchange_rate'] if 'exchange_rate' in cached_data.columns and not cached_data['exchange_rate'].isnull().all() else pd.NA
                    cached_data.dropna(subset=['price_usd'], inplace=True) # Remover NaNs da conversão
                return cached_data

            # Se não houver cache, tenta o scraping
            df = self._scrape_cepea_data(product_code, start_date, end_date)
            
            if df.empty:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Scraping não retornou dados para {product_code}. Retornando DataFrame vazio.")
                return pd.DataFrame()

            # Garantir tipos de dados corretos após scraping
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df['price'] = pd.to_numeric(df['price'], errors='coerce')
            if 'price_usd' in df.columns:
                df['price_usd'] = pd.to_numeric(df['price_usd'], errors='coerce')
            
            df.dropna(subset=['date', 'price'], inplace=True) # Remove linhas com datas ou preços inválidos
            
            # Finaliza o tratamento de moedas se 'price_usd' ainda estiver nulo ou faltar
            product_currency_type = self.product_info_map.get(product_code, {}).get('currency')
            
            # Se o produto é BRL na fonte e queremos a coluna USD (calculada)
            if product_currency_type == 'BRL' and ('price_usd' not in df.columns or df['price_usd'].isnull().all()):
                 df['exchange_rate'] = df['date'].apply(lambda x: self._get_exchange_rate(x.date()))
                 df['price_usd'] = df['price'] / df['exchange_rate'] # Converte BRL para USD
            # Se o produto é USD na fonte e queremos a coluna BRL (calculada)
            elif product_currency_type == 'USD' and ('price' not in df.columns or df['price'].isnull().all()):
                 # Isso não deve acontecer se o scraper pegou ambas as colunas como esperado.
                 # Mas como fallback, se só tiver price_usd e precisar de price (BRL)
                 df['exchange_rate'] = df['date'].apply(lambda x: self._get_exchange_rate(x.date()))
                 df['price'] = df['price_usd'] * df['exchange_rate'] # Converte USD para BRL
            else:
                # Caso já tenha ambas as moedas ou apenas a principal e não precise de conversão.
                # Garante que 'exchange_rate' esteja presente se 'price_usd' estiver.
                if 'price_usd' in df.columns and ('exchange_rate' not in df.columns or df['exchange_rate'].isnull().all()):
                     df['exchange_rate'] = df['date'].apply(lambda x: self._get_exchange_rate(x.date()))


            # Limpar NaNs que podem surgir das conversões
            df.dropna(subset=['price'], inplace=True)
            
            self._save_to_cache(df, cache_path)
            return df
            
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ERRO GERAL ao obter dados históricos para {product_code}: {e}")
            return pd.DataFrame()
