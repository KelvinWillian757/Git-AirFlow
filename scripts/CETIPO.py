from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import json
import os
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    try:
        # ----- Credenciais - com melhor tratamento de erro -----
        gcp_key = os.environ.get('GCP_KEY')
        if not gcp_key:
            raise ValueError("Variável de ambiente GCP_KEY não encontrada ou vazia")
        
        # Parse do JSON
        try:
            service_account_info = json.loads(gcp_key)
            logger.info("Credenciais carregadas com sucesso")
        except json.JSONDecodeError as e:
            logger.error(f"Erro ao fazer parse do JSON: {e}")
            logger.error(f"Primeiros 100 caracteres da chave: {gcp_key[:100]}...")
            raise
        
        # ----- Cria credenciais -----
        credentials = service_account.Credentials.from_service_account_info(service_account_info)
        
        # ----- Client BigQuery -----
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        logger.info(f"Conectado ao projeto: {credentials.project_id}")
        
        # ----- Período da competência -----
        competencia_de = '2018-01-01'
        competencia_ate = '2025-12-01'
        
        # Armazena a consulta de inserção em uma variável
        insert_query = f"""
        update `assertiv.business_analytics_gold.gold_utilizacao`
        set CLASSIFICACAO_ASSERTIV = NULL
        where CNPJ_OPERADORA = '29309127000179'
        and data_competencia between '{competencia_de}' and '{competencia_ate}';
        """
        
        # Execute a consulta de inserção
        logger.info("Executando query...")
        insert_job = client.query(insert_query)
        insert_job.result()  # Espera pela conclusão da consulta
        
        # Obtenha o número de linhas afetadas
        num_rows = insert_job.num_dml_affected_rows
        logger.info(f"Marcando registro como NULL em: {num_rows} registros.")
        
        return num_rows
        
    except Exception as e:
        logger.error(f"Erro na execução do ETL: {e}")
        raise

if __name__ == "__main__":
    main()
