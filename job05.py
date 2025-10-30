#Tarefa:
#
#Carregamento: Carregue os Parquets de Marketing (raw_mkt_ads...parquet) e Suporte (raw_svc_logs...parquet).
#
#Unificação de Esquema (Schema Unification): Crie um DataFrame de Staging para cada fonte, selecionando apenas as colunas que são necessárias para o cálculo de CAC/LTV:
#
#Staging Marketing (stg_mkt_ads): Apenas id, name, e ingestion_timestamp. (Simulando ID da Campanha, Nome da Campanha e Metadado).
#
#Staging Suporte (stg_svc_logs): Apenas ticket_id, status, tempo_resposta_min, agente_id, e ingestion_timestamp.
#
#Padronização de Nome: Renomeie a coluna name no DataFrame de Marketing para nome_campanha para padronizar o nome no Data Lake.
#
#Armazenamento no Staging: Salve os dois DataFrames resultantes nos seguintes arquivos Parquet:
#
#stg_mkt_ads_YYMMDD.parquet
#
#stg_svc_logs_YYMMDD.parquet

import pandas as pd
from datetime import datetime, timedelta
import time

pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
timestamp_ingestao = datetime.now()

dados_mkt = pd.read_parquet('raw_mkt_ads_20251028.parquet')
dados_log = pd.read_parquet('raw_svc_logs_20251028.parquet')

print(dados_mkt.head())

print('$'*30)

print(dados_log.head())

print('$'*30)
print('$'*30)
print('$'*30)
print('$'*30)

stg_mkt_ads = dados_mkt[['id', 'name', 'data_ingestao']]
stg_mkt_ads.rename(columns={'name': 'nome_campanha'}, inplace=True)
print(stg_mkt_ads.head())
print('$'*30)
print('$'*30)
print('$'*30)
print('$'*30)

stg_svc_logs = dados_log[['ticket_id', 'status', 'tempo_resposta_min', 'agente_id', 'data_ingestao']]
print(stg_svc_logs.head())

print(stg_mkt_ads.info())
print('$'*30)
print('$'*30)
print('$'*30)
print('$'*30)
print(stg_svc_logs.info())

nome_mkt = f'stg_mkt_ads_{timestamp_ingestao.strftime('%Y%m%d')}.parquet'
nome_logs = f'stg_svc_logs_{timestamp_ingestao.strftime('%Y%m%d')}.parquet'

try:
    stg_mkt_ads.to_parquet(nome_mkt, index=False)
    print(f'Arquivo de marketing salvo como {nome_mkt}')
except:
    print(f'Erro ao salvar o arquivo {stg_mkt_ads}')

try:
    stg_svc_logs.to_parquet(nome_logs, index=False)
    print(f'Arquivo de marketing salvo como {nome_logs}')
except:
    print(f'Erro ao salvar o arquivo {stg_svc_logs}')

