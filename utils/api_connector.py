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

    # api_connector.py (Apenas a função _scrape_cepea_data foi modificada, o resto permanece como na última versão)

def _scrape_cepea_data(self, product_code, start_date, end_date):
    """
    Realiza o web scraping do site do CEPEA para obter dados históricos.
    Esta função é específica para o formato de tabela de 'Série de Preços' do CEPEA.
    Tenta encontrar a tabela principal de histórico com base nos cabeçalhos esperados.
    """
    product_info = self.product_info_map.get(product_code)
    if not product_info:
        print(f"ERRO: Informações de URL/ID para o produto {product_code} não configuradas em product_info_map.")
        return pd.DataFrame()

    url_name = product_info["url_name"]
    id_indicador = product_info["id_indicador"]
    
    # A URL para a série de preços histórica com filtro por indicador
    url_to_scrape = f"{self.base_url_cotacoes}{url_name}.aspx?id_indicador={id_indicador}"
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Tentando scraping de: {url_to_scrape} para {product_code}")

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

        # Tentar encontrar a tabela principal de histórico
        # pd.read_html pode falhar se o HTML for muito "bagunçado" ou se a tabela for carregada por JS.
        # Vamos tentar ser mais específicos na busca.

        # Critérios para encontrar a tabela certa:
        # Ela deve conter colunas como 'Data', 'VALOR R$*', 'VAR./DIA', 'VAR./MÊS', 'VALOR US$*'
        # O 'match' regex ajuda a ser mais flexível com variações nos nomes das colunas
        table_match_regex = r'Data|VALOR R\$|\@ Vista|\@ Prazo|Preço|VALOR US\$'
        
        try:
            # Tenta ler todas as tabelas e encontrar a que mais se encaixa
            dfs = pd.read_html(response.text, decimal=',', thousands='.', flavor='bs4', match=table_match_regex)
            
            target_df = pd.DataFrame()
            
            # Percorre os DataFrames encontrados e tenta identificar o correto
            for df_candidate in dfs:
                # Verifique se o DataFrame candidato tem as colunas esperadas para o histórico
                # As colunas mais confiáveis são "Data" e pelo menos uma coluna de valor (R$ ou US$)
                has_date_col = any(col for col in df_candidate.columns if 'Data' in str(col))
                has_price_col_brl = any(col for col in df_candidate.columns if 'VALOR R$' in str(col) or 'À Vista' in str(col) or 'Preço à vista' in str(col))
                has_price_col_usd = any(col for col in df_candidate.columns if 'VALOR US$' in str(col))
                
                if has_date_col and (has_price_col_brl or has_price_col_usd):
                    target_df = df_candidate
                    # print(f"[{datetime.now().strftime('%H:%M:%S')}] Tabela identificada com sucesso!") # Para depuração
                    break
            
            if target_df.empty:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] AVISO: Nenhuma tabela relevante encontrada por pd.read_html para {product_code} na URL {url_to_scrape}.")
                # Se pd.read_html falhou, tente buscar com BeautifulSoup diretamente
                soup = BeautifulSoup(response.text, 'html.parser')
                # Tenta encontrar a tabela que tem um cabeçalho específico ou classe
                # Esta parte pode precisar de ajuste manual após inspecionar o HTML
                table_element = soup.find('table', class_='tabela-indicador') # Exemplo de classe, verificar no site
                if not table_element:
                     table_element = soup.find('table', string=re.compile(r'Data.*VALOR R\$')) # Outra heurística
                
                if table_element:
                    # Se encontrou uma tabela com BS4, tenta converter para DataFrame
                    # headers = [th.text.strip() for th in table_element.find_all('th')] # Cabeçalhos podem estar dentro de <thead>
                    rows = []
                    for tr in table_element.find_all('tr'):
                        cells = [td.text.strip() for td in tr.find_all(['td', 'th'])] # Pega td ou th
                        if cells: # Garante que a linha não está vazia
                            rows.append(cells)
                    if rows:
                        # A primeira linha pode ser o cabeçalho. Verifique se tem mais de uma linha.
                        if len(rows) > 1:
                            target_df = pd.DataFrame(rows[1:], columns=rows[0])
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] Tabela encontrada com BeautifulSoup e convertida.")
                        else:
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] AVISO: Tabela encontrada com BeautifulSoup, mas sem dados suficientes.")
                            return pd.DataFrame()
                    else:
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] AVISO: Tabela encontrada com BeautifulSoup, mas sem linhas.")
                        return pd.DataFrame()
                else:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] AVISO: Nenhuma tabela de histórico encontrada com BeautifulSoup.")
                    return pd.DataFrame()

        except Exception as pd_html_err:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ERRO ao tentar pd.read_html para {product_code}: {pd_html_err}")
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Conteúdo HTML para depuração (primeiros 500 caracteres):\n{response.text[:500]}")
            # Se pd.read_html falhou por algum motivo, retorna vazio
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
        price_brl_col = next((col for col in df_temp.columns if re.search(r'VALOR R\$|\bÀ Vista\b|\bPreço à vista\b', str(col), re.IGNORECASE)), None)
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

        # Converte 'date' para datetime
        df_temp['date'] = pd.to_datetime(df_temp['date'], format='%d/%m/%Y', errors='coerce')
        df_temp.dropna(subset=['date'], inplace=True) # Remove linhas com datas inválidas

        # Limpa e converte colunas de preço para numérico
        # Remove tudo que não é número ou vírgula, depois substitui vírgula por ponto
        for col_name in ['price', 'price_usd_scraped']:
            if col_name in df_temp.columns:
                # Converte para string para aplicar replace, depois para numérico
                df_temp[col_name] = df_temp[col_name].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                df_temp[col_name] = pd.to_numeric(df_temp[col_name], errors='coerce')
                
        # Remove linhas com valores nulos nos preços que são cruciais
        if 'price' in df_temp.columns:
            df_temp.dropna(subset=['price'], inplace=True)
        elif 'price_usd_scraped' in df_temp.columns:
            df_temp.dropna(subset=['price_usd_scraped'], inplace=True)
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ERRO: Nenhuma coluna de preço válida (R$ ou US$) encontrada após limpeza para {product_code}.")
            return pd.DataFrame()


        # Filtra pelo período desejado
        df_temp = df_temp[(df_temp['date'] >= start_date) & (df_temp['date'] <= end_date)].copy()
        
        if not df_temp.empty:
            df_temp['product'] = product_code
            
            # Padronização: 'price' será sempre BRL, 'price_usd' será USD (se aplicável)
            final_price_cols = []
            
            # Lógica para produtos em USD na fonte (Café, Açúcar se for o caso)
            # A coluna 'VALOR R$*' (raspada para 'price') já é BRL, e 'VALOR US$*' (raspada para 'price_usd_scraped') é USD.
            # Então, se 'price_usd_scraped' existe, significa que o CEPEA fornece o valor em USD diretamente.
            
            if 'price_usd_scraped' in df_temp.columns:
                df_temp['price_usd'] = df_temp['price_usd_scraped']
                final_price_cols.append('price_usd')
                # A coluna 'price' já contém o valor em BRL raspado.
            else:
                # Se não tem 'price_usd_scraped', assume que 'price' é BRL e não há USD direto.
                df_temp['price_usd'] = pd.NA # Define como NA se não houver coluna USD raspada
            
            final_cols_to_keep = ['date', 'price', 'product'] + final_price_cols
            
            # Remove colunas intermediárias (se houver)
            df_final = df_temp[final_cols_to_keep].copy()
            
            return df_final
        
    except requests.exceptions.RequestException as req_err:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ERRO de requisição ao CEPEA para {product_code} ({url_to_scrape}): {req_err}")
    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ERRO INESPERADO no scraping para {product_code} ({url_to_scrape}): {e}")
        # Comentar para não poluir demais, descomentar para depuração profunda:
        # print(f"[{datetime.now().strftime('%H:%M:%S')}] Conteúdo HTML completo para depuração:\n{response.text}") 
        
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
