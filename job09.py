#Atividade 9/10: SQL Avançado - Criação de Visão Analítica (VIEW)
#O time de Análise de Negócios da Aurora Tech precisa de uma forma simples e segura de consultar o LTV sem ter que reescrever os JOINs complexos a cada vez.
#
#O Desafio: Criar uma Visão (VIEW) no Data Warehouse que pré-une a Tabela de Fato (ft_negocio) com todas as Dimensões relevantes (dim_vendedor, dim_agente_suporte).
#
#Tarefa:
#
#Criação da VIEW: Escreva um comando SQL para criar uma Visão chamada vw_ltv_base.
#
#Conteúdo da VIEW: A visão deve conter todas as colunas da ft_negocio mais o nome_vendedor e o nome_agente (vindos das dimensões).
#
#Para a entrega, envie:
#
#O código Python que se conecta ao aurora_dw.db e executa o comando SQL para criar a VIEW.
#
#O comando SQL completo para criar a Visão.

import time
import pandas as pd
from sqlalchemy import create_engine, text

DB_NAME = 'aurora_dw.db'

SQL_CREATE_VIEW ="""CREATE VIEW IF NOT EXISTS vw_ltv_base AS
SELECT
    ft.*,
    dv.nome_vendedor,
    da.nome_agente
FROM
    ft_negocio AS ft
    
LEFT JOIN
    dim_vendedor AS dv
ON
    ft.id_vendedor_fk = dv.id_vendedor

LEFT JOIN
    dim_agente_suporte AS da
ON
    ft.id_agente_fk = da.id_agente;
"""

print("🚀 Iniciando Atividade 9: Criação da Visão Analítica (VIEW)")

#SQL_QUERY = "SELECT * FROM vw_ltv_base;"

try:
    engine = create_engine(f'sqlite:///{DB_NAME}')

    with engine.connect() as conn:
        print(f"Conexão com '{DB_NAME}' estabelecida.")
        
        print("Criando a VIEW 'vw_ltv_base'...")
        
        conn.execute(text(SQL_CREATE_VIEW))
        
        conn.commit()
        
        print("\n[SUCESSO] VIEW 'vw_ltv_base' criada.")
        print("Agora os analistas podem fazer: SELECT * FROM vw_ltv_base")
        #df_analise = pd.read_sql_query(SQL_QUERY, con=engine)
        #print("\n--- 📊 Resultado da Consulta (vw_ltv_base) ---")
    
    if df_analise.empty:
        print("A VIEW foi consultada com sucesso, mas não retornou dados.")
    else:
        print(df_analise)

    print("\n[SUCESSO] Análise concluída.")

except ImportError:
    print("\n[ERRO] A biblioteca 'sqlalchemy' não está instalada.")
    print("Por favor, instale-a primeiro: pip install sqlalchemy")
except Exception as e:
    print(f"\n[ERRO] Ocorreu um erro ao criar a VIEW: {e}")

