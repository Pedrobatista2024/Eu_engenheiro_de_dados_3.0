#Tarefa:
#
#Dimensão de Vendedor: Crie a tabela dim_vendedor a partir da coluna vendedor do arquivo Staging de Vendas (stg_crm_vendas...parquet).
#
#Deve conter: id_vendedor (ID sequencial gerado por você) e nome_vendedor (mantendo a padronização em minúsculo).
#
#Dimensão de Ticket/Agente: Crie a tabela dim_agente_suporte a partir da coluna agente_id do arquivo Staging de Suporte (stg_svc_logs...parquet).
#
#Deve conter: id_agente (ID sequencial gerado por você) e nome_agente (o agente_id como nome).
#
#Tabela de Fato Unificada:
#
#Crie a tabela de Fato ft_negocio.
#
#Esta tabela deve ser o resultado de um JOIN entre os dados de Vendas (stg_crm_vendas) e as novas dimensões de Vendedor e Agente, mapeando os IDs de volta para as linhas de Vendas.
#
#Para a entrega, envie:
#
#O código Python que realiza a leitura dos arquivos Staging necessários (Vendas e Suporte).
#
#O código que cria e exibe o .head() dos DataFrames dim_vendedor e dim_agente_suporte.
#
#O código que realiza o JOIN e exibe o .head() da tabela de Fato resultante.



import pandas as pd
from datetime import datetime, timedelta
import time
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
timestamp_ingestao = datetime.now()

try:
    dados_vendas = pd.read_parquet('stg_crm_vendas_20251030.parquet')
    print('dados_vendas carregados com sucesso')
except:
    print(f'ocorreu um erro ao carrregar o arquivo')

print(f' dados: VENDAS')
print(dados_vendas.head())
print('*'* 30)

dim_vendedor = pd.DataFrame()
dim_vendedor['nome_vendedor'] = dados_vendas['vendedor'].unique()
dim_vendedor = dim_vendedor.reset_index()
dim_vendedor['index'] = dim_vendedor['index'] + 1
dim_vendedor = dim_vendedor.rename(columns={'index': 'id_vendedor'})
print(f' dados: DIM_VENDEDOR')
print(dim_vendedor.head())
print('*'* 30)

try:
    dados_logs = pd.read_parquet('stg_svc_logs_20251030.parquet')
    print('dados_logs carregados com sucesso')
except:
    print(f'ocorreu um erro ao carrregar o arquivo')

print(f' dados: LOGS')
print(dados_logs.head())
print('*'* 30)

dim_agente_suporte = pd.DataFrame()
dim_agente_suporte['nome_agente'] = dados_logs['agente_id'].unique()
dim_agente_suporte = dim_agente_suporte.reset_index()
dim_agente_suporte['index'] = dim_agente_suporte['index'] + 1
dim_agente_suporte = dim_agente_suporte.rename(columns={'index': 'id_agente'})

print(f' dados: DIM_AGENTE_SUPORTE')
print(dim_agente_suporte.head())
print('*'* 30)
print('*'* 30)
print('*'* 30)


ft_negocio = dados_vendas.copy()

print('dados: FT_NEGOCIO')
print(ft_negocio.head())
print('*'* 30)

ft_negocio = ft_negocio.merge(
    dim_vendedor[['id_vendedor', 'nome_vendedor']],
    left_on='vendedor', 
    right_on='nome_vendedor',
    how='left' 
)

print('dados: FT_NEGOCIO01')
print(ft_negocio.head())
print('*'* 30)


ft_negocio.drop(columns=['vendedor', 'nome_vendedor'], inplace=True)

print('dados: FT_NEGOCIO02')
print(ft_negocio.head())
print('*'* 30)

ft_negocio.rename(columns={'id_vendedor': 'id_vendedor_fk'}, inplace=True)

print('dados: FT_NEGOCIO03')
print(ft_negocio.head())
print('*'* 30)

dados_logs = dados_logs.merge(
    dim_agente_suporte[['id_agente', 'nome_agente']],
    left_on='agente_id', 
    right_on='nome_agente',
    how='left'
)

print('dados: dados_logs')
print(dados_logs.head())
print('*'* 30)


dados_logs.drop(columns=['agente_id', 'nome_agente'], inplace=True)

print('dados: dados_logs01')
print(dados_logs.head())
print('*'* 30)

dados_logs.rename(columns={'id_agente': 'id_agente_fk'}, inplace=True)

print('dados: dados_logs02')
print(dados_logs.head())
print('*'* 30)

ft_negocio = ft_negocio.merge(
    dados_logs[['ticket_id', 'id_agente_fk', 'tempo_resposta_min', 'status']],
    left_on='lead_id', 
    right_on='ticket_id', 
    how='left',
)

print('dados: FT_NEGOCIO04')
print(ft_negocio.head())
print('*'* 30)



ft_negocio.drop(columns=['ticket_id', 'status'], inplace=True)

print('dados: FT_NEGOCIO05')
print(ft_negocio.head())
print('*'* 30)



colunas_fato_final = [
    'lead_id', 
    'data_criacao',
    'data_conversao', 
    'valor_contrato', 
    'status_lead',
    'id_vendedor_fk',      
    'id_agente_fk',       
    'tempo_resposta_min',  
    'data_ingestão'
]

ft_negocio = ft_negocio[colunas_fato_final]


print("\n--- Resultado Final: dim_vendedor.head() ---")
print(dim_vendedor.head())

print("\n--- Resultado Final: dim_agente_suporte.head() ---")
print(dim_agente_suporte.head())

print("\n--- Resultado Final: ft_negocio.head() (Tabela de Fato Unificada) ---")
print(ft_negocio.head())

print(f"\n[SUCESSO] Modelagem Dimensional concluída. Tabela de Fato pronta com as FKs.")

dim_vendedor.to_csv('dim_vendedor.csv', index=False)
dim_agente_suporte.to_csv('dim_agente_suporte.csv', index=False)
ft_negocio.to_csv('ft_negocio.csv', index=False)