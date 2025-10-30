import pandas as pd 
from datetime import datetime, timedelta
import time

pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

try:
    dados = pd.read_parquet('raw_crm_vendas_20251027.parquet')
    print(f'arquivo carregado com sucesso')
except:
    print(f'Eroo ao carregar o arquivo')

dados['valor_contrato'] = dados['valor_contrato'].fillna(0.00)

dados['data_conversao'] = pd.to_datetime(dados['data_conversao'], errors='coerce')

valor_sentinela = pd.to_datetime('2262-01-01')

condicao = (dados['status_lead'] != 'Fechado')

dados.loc[condicao, 'data_conversao'] = valor_sentinela

dados['vendedor'] = dados['vendedor'].str.lower().str.strip()

print(dados.head())

data = datetime.now()
nome_parquet = f'stg_crm_vendas_{data.strftime('%Y%m%d')}.parquet'

try:
    dados.to_parquet(nome_parquet, index=False)
    print(f'arquivo salvo com sucesso como {nome_parquet}')
except:
    print(f'Erro ao salvar o arquivo {nome_parquet}')