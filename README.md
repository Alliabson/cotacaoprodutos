# Agro-Cepea-App

Aplicativo para análise de cotações de produtos agrícolas utilizando dados da CEPEA.

## Funcionalidades

- 📊 Visualização histórica de preços
- 🌱 Análise sazonal de commodities
- 📈 Comparativo entre múltiplos produtos
- 💾 Exportação de dados em múltiplos formatos

## Instalação

1. Clone o repositório:
   ```bash
   git clone https://github.com/seu-usuario/agro-cepea-app.git
   ```

2. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   ```

3. Crie um arquivo `.env` na raiz do projeto com sua chave da API CEPEA:
   ```env
   CEPEA_API_KEY=sua_chave_aqui
   ```

4. Execute o aplicativo:
   ```bash
   streamlit run main.py
   ```

## Estrutura do Projeto

```
agro-cepea-app
│── /data
│   ├── historical_cache/          # Armazenamento local de dados históricos
│   └── products_list.json         # Lista de produtos disponíveis para consulta
│── /utils
│   ├── api_connector.py           # Conexão com API CEPEA
│   ├── data_processor.py          # Processamento de dados brutos
│   └── visualization.py           # Funções de visualização customizadas
│── main.py                        # Aplicativo principal Streamlit
│── requirements.txt               # Dependências do projeto
│── README.md                      # Documentação
```

## Licença

MIT License
