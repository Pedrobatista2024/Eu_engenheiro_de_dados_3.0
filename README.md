# 🚀 PROJETO DATA STREAM: Implantação do Data Lake e Data Warehouse (Aurora Tech Solutions)

Este repositório documenta a implementação de um **Núcleo Zero de Dados** para a **Aurora Tech Solutions**, simulando o desafio real de unificar **dados em silos** e estabelecer uma cultura *data-driven* do zero. O projeto foca na construção de um *pipeline* robusto que permite o cálculo das métricas financeiras críticas: **CAC (Custo de Aquisição do Cliente)** e **LTV (Valor do Tempo de Vida do Cliente)**.

O trabalho abrangeu todo o ciclo de vida do dado, desde a ingestão de fontes complexas até a otimização de consultas no Data Warehouse.

---

## 🎯 Objetivo e Contexto

O objetivo principal foi unificar as três fontes críticas da Aurora (Vendas, Marketing e Suporte) para criar uma **Fonte Única da Verdade** analítica.

### 📊 Fontes de Dados Ingeridas

| Fonte | Complexidade | Pilares Cobertos |
| :--- | :--- | :--- |
| **Vendas (CRM)** | CSV/Parquet, Tratamento de Nulos. | Qualidade, Ingestão. |
| **Marketing (Ads)** | API com **Paginação Dinâmica**. | Ingestão, Lógica ETL/ELT. |
| **Suporte (Logs)** | **JSON Aninhado** (Requer *Flattening*). | Programação Python (Pandas). |

---

## 🛠️ Pilares Avançados da Engenharia de Dados Implementados

O projeto solidifica a experiência em todas as áreas necessárias para uma arquitetura moderna de dados:

1.  **Modelagem Dimensional:** Criação de um esquema Dimensão-Fato: `dim_vendedor`, `dim_agente_suporte` e `ft_negocio` (Fato Unificado).
2.  **Qualidade de Dados:** Implementação de regras específicas de negócio (Ex: Substituição de `NaN` por `0.00` em Finanças e uso de **Valores Sentinela** para datas ausentes).
3.  **SQL Avançado e Otimização:**
    * Criação de **Índices** em colunas de tempo e métricas (`data_conversao`, `valor_contrato`) para acelerar as consultas de LTV.
    * Criação de **Views** (`vw_ltv_base`) para simplificar o acesso analítico (Usabilidade).
    * Verificação de **Integridade Referencial** (Chaves Órfãs).
4.  **Data Lake & Staging:** Implementação de um fluxo ELT (Extrair, Carregar, Transformar) com separação clara das camadas **Raw** e **Staging** (limpeza e unificação de esquema).

---

## 📝 Entrega Final: O Data Warehouse Analítico

A entrega final para a gestão da Aurora Tech Solutions é o Data Warehouse **`aurora_dw.db`**, que contém:

| Tabela/View | Tipo | Função Analítica |
| :--- | :--- | :--- |
| **`ft_negocio`** | Tabela de Fato | Contém todas as métricas (Valor, Tempo de Resposta) e Chaves Estrangeiras (FKs). |
| **`dim_vendedor` / `dim_agente_suporte`** | Dimensões | Tabelas de Descrição, garantindo a normalização e a facilidade de segmentação. |
| **`vw_ltv_base`** | **VIEW Analítica** | Fonte única de verdade, unindo Fato e Dimensões. Permite consultas simples (`SELECT *`) para o cálculo de CAC/LTV. |

---