import pandas as pd
import json
import os
from datetime import datetime, timedelta

# --- 1. FONTE CRM (Vendas - Para Atividade 1) ---
# Simulação de exportação CSV diária
crm_data = {
    'lead_id': [1001, 1002, 1003, 1004, 1005, 1006, 1007],
    'data_criacao': ['2025-10-15', '2025-10-16', '2025-10-17', '2025-10-18', '2025-10-19', '2025-10-20', '2025-10-21'],
    'data_conversao': ['2025-10-20', '2025-10-23', None, '2025-10-24', None, '2025-10-24', '2025-10-24'],
    'valor_contrato': [5500.00, 12000.00, None, 8000.00, None, 3200.00, 9800.00],
    'vendedor': ['Ana B.', 'Carlos D.', 'Ana B.', 'Erica F.', 'Erica F.', 'Gustavo H.', 'Ana B.'],
    'status_lead': ['Fechado', 'Fechado', 'Em Negociação', 'Fechado', 'Perdido', 'Fechado', 'Fechado']
}
df_crm = pd.DataFrame(crm_data)

# Cria o arquivo CSV que você irá ingerir na Atividade 1
NOME_ARQUIVO_CRM = 'crm_vendas_export.csv'
df_crm.to_csv(NOME_ARQUIVO_CRM, index=False, sep=';') 
print(f"[SETUP] Criado arquivo: {NOME_ARQUIVO_CRM}")

# --- 2. FONTE MARKETING (Ads - Para Atividade 2) ---
# A API de Marketing não será simulada por arquivo; você fará a requisição real
# (usaremos uma API pública real que você definirá para simular a necessidade de paginação).
print("[SETUP] Fonte Marketing: Utilizará API pública real com necessidade de paginação (simulação de API de Ads).")

# --- 3. FONTE SUPORTE (Logs - Para Atividade 3) ---
# Simulação de logs JSON aninhados do sistema de tickets
support_logs = {
    "tickets": [
        {
            "ticket_id": 5001,
            "status": "Resolvido",
            "tempo_resposta_min": 15,
            "cliente": "Alpha Corp",
            "detalhes_suporte": {
                "agente_id": "T001",
                "categoria": "Software",
                "prioridade": "Alta"
            }
        },
        {
            "ticket_id": 5002,
            "status": "Aberto",
            "tempo_resposta_min": None,
            "cliente": "Beta Ltda",
            "detalhes_suporte": {
                "agente_id": "T002",
                "categoria": "Hardware",
                "prioridade": "Média"
            }
        },
        {
            "ticket_id": 5003,
            "status": "Resolvido",
            "tempo_resposta_min": 5,
            "cliente": "Gama Consultoria",
            "detalhes_suporte": {
                "agente_id": "T001",
                "categoria": "Rede",
                "prioridade": "Baixa"
            }
        }
    ]
}

# Cria o arquivo JSON que você irá ingerir na Atividade 3
NOME_ARQUIVO_SUPORTE = 'suporte_logs_brutos.json'
with open(NOME_ARQUIVO_SUPORTE, 'w') as f:
    json.dump(support_logs, f, indent=4)
print(f"[SETUP] Criado arquivo: {NOME_ARQUIVO_SUPORTE}")

print("\n[SETUP CONCLUÍDO] Você pode iniciar o PROJETO DATA STREAM com a Atividade 1.")