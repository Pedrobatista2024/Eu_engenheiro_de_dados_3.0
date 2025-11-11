#Tarefa:
#
#Criação de Índices: Execute dois comandos SQL para criar índices nas seguintes colunas:
#
#valor_contrato na tabela ft_negocio (para acelerar as agregações financeiras como SUM e AVG).
#
#data_conversao na tabela ft_negocio (para acelerar filtros de tempo, ex: WHERE data_conversao > '2024-01-01').
#
#Justificativa: Explique em texto, de forma concisa, por que a criação desses dois índices específicos (métricas e tempo) é a chave para otimizar as consultas de LTV.
#
#Para a entrega, envie:
#
#O código Python que se conecta ao aurora_dw.db e executa os comandos SQL para criar os índices.
#
#A justificativa concisa.

import time
from sqlalchemy import create_engine, text


DB_NAME = 'aurora_dw.db'

SQL_IDX_VALOR = """
CREATE INDEX IF NOT EXISTS idx_valor_contrato
ON ft_negocio (valor_contrato);
"""

SQL_IDX_DATA = """
CREATE INDEX IF NOT EXISTS idx_data_conversao
ON ft_negocio (data_conversao);
"""

print("🚀 Iniciando Atividade 8: Otimização do DW (Índices)")

try:
    engine = create_engine(f'sqlite:///{DB_NAME}')

    with engine.connect() as conn:
        print(f"Conexão com '{DB_NAME}' estabelecida.")
        
        print("Aplicando índices na tabela 'ft_negocio'...")
        
        conn.execute(text(SQL_IDX_VALOR))
        print(" -> Índice 'idx_valor_contrato' (para finanças) criado com sucesso.")
        
        conn.execute(text(SQL_IDX_DATA))
        print(" -> Índice 'idx_data_conversao' (para tempo) criado com sucesso.")

        conn.commit()

    print("\n[SUCESSO] Otimização concluída. O DW está indexado.")

except ImportError:
    print("\n[ERRO] A biblioteca 'sqlalchemy' não está instalada.")
    print("Por favor, instale-a primeiro: pip install sqlalchemy")
except Exception as e:
    print(f"\n[ERRO] Ocorreu um erro ao aplicar os índices: {e}")