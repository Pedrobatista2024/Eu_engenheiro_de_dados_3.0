#Atividade 2/10: Ingestão de Dados de Marketing (Fonte Ads - API)
#O desafio é ingerir os dados de custo e performance de marketing da plataforma de anúncios, que requer paginação.
#
#Fonte de Simulação: Utilizaremos a Rick and Morty API para simular a lógica de paginação (info.next).
#
#Tarefa:
#
#Ingestão com Paginação: Desenvolva um código Python que colete dados de todas as páginas da seguinte API: https://rickandmortyapi.com/api/character/.
#
#Tratamento de Dados: Para cada página coletada, extraia apenas os dados da chave results e os unifique em uma lista de registros.
#
#Preparação Bruta: Conclua a coleta, converta a lista de registros para um DataFrame e adicione a coluna ingestion_timestamp com o carimbo de data/hora atual.
#
#Armazenamento Bruto (Data Lake): Salve o DataFrame resultante em um arquivo Parquet chamado raw_mkt_ads_YYMMDD.parquet.
#
#Para a entrega, envie:
#
#O código Python completo que realiza a ingestão e a paginação.
#
#O nome do arquivo Parquet que você gerou.

import pandas as pd
import time
import requests
from datetime import datetime, timedelta

timestamp_ingestao = datetime.now()
pd.set_option('display.max_columns', None)

url = 'https://rickandmortyapi.com/api/character/'

todos_os_dados = []
while url:
    try:
        response = requests.get(url)
        response.raise_for_status()
        dados = response.json()
        todos_os_dados.extend(dados['results'])
        url = dados['info']['next']
        print(f"Página coletada. Próxima URL: {url}")

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            print('limite de requisição excedido, aguarde 10 segundos')
            time.sleep(10)
            continue
        else:
            print(f'Erro inesperado: {e}')
            break

df = pd.DataFrame(todos_os_dados)
df['data_ingestao'] = timestamp_ingestao

print(df.head())

nome_parquet = f'raw_mkt_ads_{timestamp_ingestao.strftime('%Y%m%d')}.parquet'



try:
    df.to_parquet(nome_parquet, index=False)
    print(f"\n[SUCESSO] DataFrame salvo como '{nome_parquet}'")

except Exception as e:
    print(f"\n[ERRO] Ocorreu um erro inesperado: {e}")