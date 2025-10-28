#Atividade 3/10: Ingestão de Dados de Suporte (Fonte Logs - JSON Aninhado)
#A terceira fonte de dados é a mais desafiadora: os logs do sistema de suporte. Estes logs vêm como JSON Aninhado, o que é muito comum em sistemas de logs e APIs REST mal modeladas.
#
#O Desafio: Para que os Analistas de Negócios possam calcular o Custo de Serviço (parte do LTV), precisamos desanexar (flattening) os detalhes do suporte para que cada ticket seja uma linha limpa no nosso Data Lake.
#
#Fonte de Simulação: Você usará o arquivo suporte_logs_brutos.json que foi gerado no SETUP.
#
#Tarefa:
#
#Ingestão: Carregue o arquivo suporte_logs_brutos.json no Pandas. Atenção: o JSON está aninhado sob a chave tickets.
#
#Transformação Bruta (Flattening): Desanexe os dados da chave detalhes_suporte para que as chaves aninhadas (agente_id, categoria, prioridade) se tornem colunas separadas no DataFrame principal.
#
#Preparação Bruta: Adicione a coluna ingestion_timestamp com o carimbo de data/hora atual.
#
#Armazenamento Bruto (Data Lake): Salve o DataFrame resultante em um arquivo Parquet chamado raw_svc_logs_YYMMDD.parquet.

import pandas as pd
from datetime import datetime, timedelta
import time

pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
timestamp_ingestao = datetime.now()

df = pd.read_json('suporte_logs_brutos.json')

df_normalizado = pd.json_normalize(df['tickets'])

df_normalizado.columns = df_normalizado.columns.str.replace('detalhes_suporte.', '', regex=False)

df_normalizado['data_ingestao'] = timestamp_ingestao

nome_parquet = f'raw_svc_logs_{timestamp_ingestao.strftime('%Y%m%d')}.parquet'

try:
    df_normalizado.to_parquet(nome_parquet, index=False)
    print(f"\n[SUCESSO] DataFrame salvo como '{nome_parquet}'")

except Exception as e:
    print(f"\n[ERRO] Ocorreu um erro inesperado: {e}")