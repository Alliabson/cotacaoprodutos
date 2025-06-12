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
        # A URL base do CEPEA para cotações históricas
        self.base_url_cotacoes = "https://www.cepea.esalq.usp.br/br/indicador/boi-gordo.aspx" # Exemplo para Boi Gordo
        self.base_url = "https://www.cepea.esalq.usp.br/br" # Para outros recursos
        self.api_key = os.getenv("CEPEA_API_KEY", "default_key") # Chave não será usada para scraping CEPEA
        self.cache_dir = "data/cepea_cache"
        self.products_cache = "data/products_list.json"
        os.makedirs(self.cache_dir, exist_ok=True)
        os.makedirs(os.path.dirname(self.products_cache), exist_ok=True)
        
        # Mapeamento para URLs específicas de produtos do CEPEA.
        # VOCÊ PRECISARÁ EXPANDIR ISSO para todos os produtos que deseja scraping!
        self.product_url_map = {
            "BGI": "https://www.cepea.esalq.usp.br/br/indicador/boi-gordo.aspx",
            "MIL": "https://www.cepea.esalq.usp.br/br/indicador/milho.aspx",
            "SOJ": "https://www.cepea.esalq.usp.br/br/indicador/soja.aspx",
            "CAF": "https://www.cepea.esalq.usp.br/br/indicador/cafe.aspx", # Geralmente Café Arábica
            "SUC": "https://www.cepea.esalq.usp.br/br/indicador/acucar.aspx", # Geralmente Açúcar Cristal
            # Adicione mais produtos conforme necessário
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
                # print(f"Carregando do cache: {cache_path}") # Para depuração
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
                return float(data['value'][0]['cotacaoCompra'])
            else:
                print(f"Aviso: Nenhuma cotação de dólar encontrada para a data {date_str}. Usando fallback.")
                return 5.0 # Valor fallback se a API não retornar dados
        except Exception as e:
            print(f"Erro ao obter taxa de câmbio para {date.strftime('%Y-%m-%d')}: {e}. Usando fallback.")
            return 5.0  # Valor fallback

    def get_available_products(self):
        """Retorna lista de produtos disponíveis com cache.
        Esta lista é definida manualmente, pois não há uma API para listar todos os produtos.
        """
        try:
            # Tenta carregar do cache
            if os.path.exists(self.products_cache):
                with open(self.products_cache, 'r', encoding='utf-8') as f:
                    return json.load(f)
            
            # Lista padrão dos produtos suportados pelo scraper
            default_products = [
                {"code": "BGI", "name": "Boi Gordo", "unit": "@", "currency": "BRL"},
                {"code": "MIL", "name": "Milho", "unit": "sc 60kg", "currency": "BRL"},
                {"code": "SOJ", "name": "Soja", "unit": "sc 60kg", "currency": "BRL"},
                {"code": "CAF", "name": "Café Arábica", "unit": "sc 60kg", "currency": "USD"}, # Preço em USD no CEPEA
                {"code": "SUC", "name": "Açúcar Cristal", "unit": "sc 50kg", "currency": "USD"} # Preço em USD no CEPEA
            ]
            
            # Salva no cache
            with open(self.products_cache, 'w', encoding='utf-8') as f:
                json.dump(default_products, f, ensure_ascii=False, indent=2)
            
            return default_products
            
        except Exception as e:
            print(f"Erro ao carregar produtos: {e}")
            return []

    def _scrape_cepea_data(self, product_code, start_date, end_date):
        """
        Realiza o web scraping do site do CEPEA para obter dados históricos.
        Atenção: Este método é sensível a mudanças na estrutura do site do CEPEA.
        """
        product_url = self.product_url_map.get(product_code)
        if not product_url:
            print(f"URL para o produto {product_code} não configurada para scraping.")
            return pd.DataFrame()

        # O CEPEA permite buscar por mês e ano. Precisamos iterar pelos meses.
        # Vamos buscar um ano por vez para evitar requisições muito longas que podem ser bloqueadas.
        all_data = []
        current_date = start_date

        while current_date <= end_date:
            year = current_date.year
            month = current_date.month
            
            # Formato da URL para consulta de histórico mensal (pode variar!)
            # Ex: https://www.cepea.esalq.usp.br/br/indicador/milho.aspx?id_indicador=2&data=01/01/2024
            # A URL exata e os parâmetros de data podem precisar de ajuste
            
            # Tentando usar a URL principal e filtrar pela tabela de histórico
            # O CEPEA geralmente tem uma seção de "Histórico de Preços" com uma tabela.
            # A URL pode não aceitar parâmetros de data diretamente na requisição GET para filtrar.
            # O mais provável é que a página já contenha a tabela, e precisamos extrair dela.

            # URL base do indicador
            # A tabela pode estar na página principal do indicador
            url_to_scrape = product_url
            
            # print(f"Scraping {url_to_scrape} para {product_code} - {month}/{year}") # Para depuração

            try:
                response = requests.get(url_to_scrape, timeout=10)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')

                # Tentar encontrar a tabela de histórico de preços
                # Isso é genérico, pode precisar de ajuste fino baseado no HTML do CEPEA
                # Exemplo: A tabela pode ter um id ou uma classe específica.
                # Procure por uma tabela que contenha "Data", "Preço", etc.
                
                # Para o boi gordo, a tabela geralmente tem o cabeçalho 'Data' 'Preço à vista' 'Preço a Prazo'
                # Vamos buscar por uma tabela que contenha 'Preço à vista'
                
                table = None
                tables = soup.find_all('table')
                for t in tables:
                    if "Preço à vista" in t.get_text(): # Uma heurística para encontrar a tabela certa
                        table = t
                        break

                if not table:
                    print(f"Aviso: Tabela de histórico não encontrada na página para {product_code}.")
                    # Tentar uma abordagem mais ampla, buscando por qualquer tabela e depois por cabeçalhos
                    # Ou inspecionar o HTML mais a fundo.
                    # Para simplificar aqui, vamos tentar usar pd.read_html
                    try:
                        # pd.read_html é muito útil para raspar tabelas, mas pode pegar várias
                        dfs = pd.read_html(response.text, decimal=',', thousands='.')
                        if dfs:
                            # Tenta encontrar o DataFrame que contém 'Data' e 'Preço'
                            for df_temp in dfs:
                                if 'Data' in df_temp.columns and any(col in df_temp.columns for col in ['Preço à vista', 'Preço', 'À Vista']):
                                    table = df_temp # Usar o df diretamente
                                    break
                            if table is None:
                                print(f"Aviso: Nenhuma tabela relevante encontrada por pd.read_html para {product_code}.")
                                current_date += timedelta(days=30) # Avança um mês para tentar a próxima data
                                continue # Continua para a próxima iteração
                        else:
                            print(f"Aviso: pd.read_html não encontrou tabelas na página para {product_code}.")
                            current_date += timedelta(days=30) # Avança um mês para tentar a próxima data
                            continue # Continua para a próxima iteração
                    except Exception as e:
                        print(f"Erro ao tentar pd.read_html para {product_code}: {e}")
                        current_date += timedelta(days=30) # Avança um mês para tentar a próxima data
                        continue # Continua para a próxima iteração

                if isinstance(table, pd.DataFrame):
                    df_temp = table # Já é um DataFrame
                else: # Se for um objeto BeautifulSoup.Tag (tabela HTML)
                    headers = [th.text.strip() for th in table.find_all('th')]
                    rows = []
                    for tr in table.find_all('tr')[1:]: # Ignora o cabeçalho
                        cells = [td.text.strip() for td in tr.find_all('td')]
                        if len(cells) == len(headers):
                            rows.append(cells)
                    df_temp = pd.DataFrame(rows, columns=headers)

                # Limpeza e padronização dos dados extraídos
                if not df_temp.empty:
                    # Mapear colunas para nomes padrão
                    # Isso é crucial, pois os nomes das colunas podem variar entre produtos
                    col_mapping = {}
                    if 'Data' in df_temp.columns: col_mapping['Data'] = 'date'
                    if 'Preço à vista' in df_temp.columns: col_mapping['Preço à vista'] = 'price'
                    elif 'Preço' in df_temp.columns: col_mapping['Preço'] = 'price'
                    elif 'À Vista' in df_temp.columns: col_mapping['À Vista'] = 'price' # Para alguns casos
                    if 'Preço Prazo' in df_temp.columns: col_mapping['Preço Prazo'] = 'price_prazo' # Exemplo
                    
                    df_temp = df_temp.rename(columns=col_mapping)
                    
                    # Converte 'date' para datetime, tratando possíveis erros
                    df_temp['date'] = pd.to_datetime(df_temp['date'], format='%d/%m/%Y', errors='coerce')
                    df_temp.dropna(subset=['date'], inplace=True) # Remove linhas com datas inválidas

                    # Converte 'price' para numérico, tratando vírgulas e pontos
                    # Remove tudo que não for número ou vírgula, depois substitui vírgula por ponto
                    # Pode haver valores como '---' ou vazios, tratar como NaN
                    df_temp['price'] = df_temp['price'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                    df_temp['price'] = pd.to_numeric(df_temp['price'], errors='coerce')
                    df_temp.dropna(subset=['price'], inplace=True) # Remove linhas com preços inválidos
                    
                    # Filtra pelo período desejado
                    df_temp = df_temp[(df_temp['date'] >= start_date) & (df_temp['date'] <= end_date)]
                    
                    if not df_temp.empty:
                        # Adiciona a coluna do produto
                        df_temp['product'] = product_code
                        all_data.append(df_temp[['date', 'price', 'product']])
                        
            except requests.exceptions.RequestException as req_err:
                print(f"Erro de requisição ao CEPEA para {product_code}: {req_err}")
                break # Sai do loop se houver erro de conexão
            except Exception as e:
                print(f"Erro inesperado no scraping para {product_code}: {e}")
                # Pode ser útil logar o HTML para depuração em caso de erros
                # with open(f"debug_html_{product_code}_{year}_{month}.html", "w", encoding="utf-8") as f:
                #     f.write(response.text)
                break # Sai do loop em caso de erro grave
            
            # O CEPEA não tem um filtro de data na URL. A abordagem é pegar toda a tabela e filtrar.
            # Então, após a primeira requisição, se já tivermos dados, não precisamos mais iterar meses.
            # O loop while current_date <= end_date é mais para cenários onde a API permite um filtro.
            # Para o CEPEA, vamos fazer uma única requisição para a página do indicador e filtrar o DataFrame.
            break # Sai do loop após a primeira tentativa, pois a página geralmente tem todos os dados recentes.


        if all_data:
            combined_df = pd.concat(all_data).drop_duplicates(subset=['date', 'product']).sort_values('date').reset_index(drop=True)
            return combined_df
        return pd.DataFrame()

    def get_historical_prices(self, product_code, start_date, end_date):
        """Obtém preços históricos com tratamento de moeda e cache."""
        try:
            cache_path = self._get_cache_path(product_code, start_date, end_date)
            cached_data = self._load_from_cache(cache_path)
            
            if cached_data is not None and not cached_data.empty:
                # print(f"Retornando dados do cache para {product_code}") # Para depuração
                return cached_data

            # Se não houver cache, tenta o scraping
            print(f"Iniciando scraping para {product_code} de {start_date.strftime('%Y-%m-%d')} a {end_date.strftime('%Y-%m-%d')}")
            df = self._scrape_cepea_data(product_code, start_date, end_date)
            
            if df.empty:
                print(f"Scraping não retornou dados para {product_code}. Tentando fallback.")
                # Fallback para dados simulados se o scraping falhar
                dates = pd.date_range(start=start_date, end=end_date)
                base_price = 300.0 # Valor genérico
                data = {
                    "date": dates,
                    "price": [base_price + 10 * (i % 7) - 5 * (i // 7) for i in range(len(dates))],
                    "product": product_code
                }
                df = pd.DataFrame(data)
                df['date'] = pd.to_datetime(df['date'])


            df['date'] = pd.to_datetime(df['date']) # Garante que a coluna de data é datetime
            
            # Adiciona câmbio para produtos em USD
            product_info = next((p for p in self.get_available_products() if p['code'] == product_code), None)
            if product_info and product_info.get('currency') == 'USD':
                # Só calcula a taxa de câmbio para as datas que existem no DataFrame
                # Certifique-se de que a coluna 'price' (raspadinha) é o valor em USD
                df['price_usd'] = df['price'].copy() # O preço raspado é o USD
                df['exchange_rate'] = df['date'].apply(self._get_exchange_rate)
                df['price'] = df['price_usd'] * df['exchange_rate'] # Converte para BRL
            else:
                 # Se não for USD, garante que não teremos price_usd e exchange_rate
                 df['price_usd'] = None # Ou pd.NA
                 df['exchange_rate'] = None # Ou pd.NA

            # Opcional: remover NaNs que podem ter surgido de datas ou preços inválidos
            df.dropna(subset=['date', 'price'], inplace=True)
            
            self._save_to_cache(df, cache_path)
            return df
            
        except Exception as e:
            print(f"Erro geral ao obter dados históricos: {e}")
            return pd.DataFrame()

    # O método _call_cepea_api não será usado para este scraping
    # def _call_cepea_api(self, endpoint, params=None):
    #     """Método genérico para chamar a API CEPEA - NÃO USADO PARA WEB SCRAPING"""
    #     pass
