#Tarefa:
#
#Ingestão: Carregue o arquivo CSV que acabamos de gerar (crm_vendas_export.csv) no Pandas. Atenção ao delimitador usado no CSV (;).
#
#Preparação Bruta: Adicione uma coluna chamada ingestion_timestamp com o carimbo de data/hora atual da ingestão (prática padrão em Data Lakes).
#
#Armazenamento Bruto (Data Lake): Salve o DataFrame resultante em um arquivo Parquet chamado raw_crm_vendas_YYMMDD.parquet.
#
#Para a entrega, envie:
#
#O código Python que realiza as etapas.
#
#O nome do arquivo Parquet que você gerou.

import pandas as pd
import json
import os
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns

timestamp_ingestao = datetime.now()
pd.set_option('display.max_columns', None)

caminho = 'crm_vendas_export.csv'
try:
    df_crm = pd.read_csv(caminho, sep=';')
    print(f'arquivo {caminho} lido com sucesso')
except:
    print(f'Arquivo {caminho} não encontrado')

df_crm['data_ingestão'] = timestamp_ingestao

data_formatada = timestamp_ingestao.strftime("%Y%m%d")
nome_parquet = f"raw_crm_vendas_{data_formatada}.parquet"
try:
    df_crm.to_parquet(nome_parquet, index=False)
    print(f"\n[SUCESSO] DataFrame salvo como '{nome_parquet}'")

except Exception as e:
    print(f"\n[ERRO] Ocorreu um erro inesperado: {e}")


## Breve Analise Exploratoria
#print("--- Estrutura e tamanho dos dados(shape) ---")
#print(df_crm.shape)
#
#print("--- Informações Gerais (info) ---")
#df_crm.info()
#
#print("\n--- Primeiras 5 Linhas (head) ---")
#print(df_crm.head(5))
#
#print("\n--- ultimas 5 Linhas (tail) ---")
#print(df_crm.tail(5))
#
#print('\n--- analise coluna numericas (describe) ---')
#print(df_crm['valor_contrato'].describe())
