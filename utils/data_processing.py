import pandas as pd
from datetime import datetime

def process_quotes_data(data: dict) -> pd.DataFrame:
    """
    Processa os dados brutos da API e retorna um DataFrame formatado.
    
    Args:
        data (dict): Dados brutos da API
        
    Returns:
        pd.DataFrame: DataFrame com colunas: data, preco, variacao_dia
    """
    if not data or "cotacoes" not in data:
        return pd.DataFrame()
    
    # Converte para DataFrame
    df = pd.DataFrame(data["cotacoes"])
    
    # Processa as colunas
    df["data"] = pd.to_datetime(df["data"]).dt.date
    df["preco"] = pd.to_numeric(df["preco"], errors="coerce")
    df["variacao_dia"] = pd.to_numeric(df["variacao_dia"], errors="coerce")
    
    # Remove linhas com valores faltantes
    df = df.dropna(subset=["preco"])
    
    # Ordena por data
    df = df.sort_values("data")
    
    return df
