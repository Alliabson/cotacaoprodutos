import requests
import os
from datetime import datetime, timedelta
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# API gratuita - você pode substituir por outras como:
# - API do Banco Central
# - API do CEPEA (https://www.cepea.esalq.usp.br/br)
# - API do IpeaData (http://www.ipeadata.gov.br/api/)
# - API do Yahoo Finance (para commodities globais)

def fetch_agro_data(produto: str, dias_historico: int = 30):
    """
    Busca dados de cotações agropecuárias de uma API gratuita.
    
    Args:
        produto (str): Código do produto (ex: 'boi-gordo')
        dias_historico (int): Número de dias de histórico a buscar
        
    Returns:
        dict: Dados da cotação no formato JSON
    """
    # Exemplo com API fictícia - substitua pela API real que desejar
    base_url = "https://api.example.com/agro"
    
    # Parâmetros da requisição
    params = {
        "produto": produto,
        "inicio": (datetime.now() - timedelta(days=dias_historico)).strftime("%Y-%m-%d"),
        "fim": datetime.now().strftime("%Y-%m-%d")
    }
    
    # Adicione sua chave de API se necessário
    headers = {}
    if "API_KEY" in os.environ:
        headers["Authorization"] = f"Bearer {os.environ['API_KEY']}"
    
    try:
        response = requests.get(
            f"{base_url}/cotacoes",
            params=params,
            headers=headers,
            timeout=10
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise Exception(f"Erro na requisição à API: {str(e)}")
