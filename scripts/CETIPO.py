from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import json
import os
# ----- Credenciais direto no script -----
service_account_info = json.loads(os.environ['GCP_KEY'])
# ----- Cria credenciais direto do dict -----
credentials = service_account.Credentials.from_service_account_info(service_account_info)
# ----- Client BigQuery -----
client = bigquery.Client(credentials=credentials, project=credentials.project_id)
# ----- Período da competência -----
competencia_de = '2018-01-01'
competencia_ate = '2025-12-01'
# Armazena a consulta de inserção em uma variável
insert_query = f"""
   update `assertiv.business_analytics_gold.gold_utilizacao`
   set CLASSIFICACAO_ASSERTIV = NULL
   where CNPJ_OPERADORA = '29309127000179'
  -- -- and data_competencia between '{competencia_de}' and '{competencia_ate}';
   """
# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result() # Espera pela conclusão da consulta
# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Marcando registro como NULL em: {num_rows} registros.")
# Armazena a consulta de inserção em uma variável
