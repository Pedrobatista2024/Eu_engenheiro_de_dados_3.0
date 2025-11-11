#Tarefa:
#
#Conexão DW: Crie um novo banco de dados SQLite chamado aurora_dw.db.
#
#Carregamento Inicial: Carregue os três DataFrames (dim_vendedor, dim_agente_suporte e ft_negocio) como tabelas separadas no banco de dados. Use if_exists='replace' para esta primeira execução de setup.
#
#Integridade Referencial (Simulação SQL): Em um DW real, as chaves estrangeiras (id_vendedor_fk, id_agente_fk) garantem que não haja IDs na Tabela de Fato que não existam na tabela Dimensão.
#
#Escreva uma consulta SQL que verifique se há algum id_vendedor_fk na tabela ft_negocio que seja NULL ou que não exista na tabela dim_vendedor. Esta é uma verificação de Qualidade de Dados pós-carregamento.

import pandas as pd
from sqlalchemy import create_engine
import time

dim_vendedor = pd.read_csv('dim_vendedor.csv')
dim_agente_suporte = pd.read_csv('dim_agente_suporte.csv')
ft_negocio = pd.read_csv('ft_negocio.csv')

db_name = 'aurora_dw.db'

try:
    engine = create_engine(f'sqlite:///{db_name}')
    print(f'conexão com dw "{db_name}" criada com secesso.')
except Exception as e:
    print(f"[ERRO] Não foi possível criar a engine de conexão: {e}")
    exit()

try:
    print("Iniciando carregamento das Dimensões...")

    dim_vendedor.to_sql(
        'dim_vendedor',
        con=engine,
        if_exists='replace',
        index=False
    )
    print(" -> Tabela 'dim_vendedor' carregada.")

    dim_agente_suporte.to_sql(
        'dim_agente_suporte',
        con=engine,
        if_exists='replace',
        index=False
    )
    print(" -> Tabela 'dim_agente_suporte' carregada.")
    
    print("\nIniciando carregamento da Tabela Fato...")

    ft_negocio.to_sql(
        'ft_negocio',
        con=engine,
        if_exists='replace',
        index=False
    )
    print(" -> Tabela 'ft_negocio' carregada.")
    
    print("\n[SUCESSO] Carga no Data Warehouse concluída!")
except ImportError:
    print("\n[ERRO] A biblioteca 'sqlalchemy' não está instalada.")
    print("Por favor, instale-a primeiro: pip install sqlalchemy")
except Exception as e:
    print(f"\n[ERRO] Ocorreu um erro durante o carregamento: {e}")

query_sql = """
SELECT
    ft.lead_id,
    ft.id_vendedor_fk
FROM
    ft_negocio AS ft
LEFT JOIN
    dim_vendedor AS dim
ON
    ft.id_vendedor_fk = dim.id_vendedor
WHERE
    ft.id_vendedor_fk IS NULL
    OR dim.id_vendedor IS NULL;
"""
print("\n--- 3. Executando Verificação de Integridade (Qualidade de Dados) ---")
try:
    df_erros = pd.read_sql_query(query_sql, con=engine)

    if df_erros.empty:
        print("✅ [SUCESSO] Verificação de integridade aprovada!")
        print("   Nenhum 'id_vendedor_fk' órfão ou NULO foi encontrado.")
    else:
        print("🚨 [FALHA] Verificação de integridade falhou!")
        print("   As seguintes linhas da 'ft_negocio' têm IDs inválidos:")
        print(df_erros)
except Exception as e:
    print(f"\n[ERRO] Ocorreu um erro ao executar a consulta: {e}")