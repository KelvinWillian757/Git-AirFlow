from google.oauth2 import service_account

import os
from google.cloud import bigquery
from datetime import datetime


def main():

    client = bigquery.Client()

    competencia_de = '2018-01-01'
    competencia_ate = '2025-12-01'

    query = f"""
    UPDATE `assertiv.business_analytics_gold.gold_utilizacao`
    SET CLASSIFICACAO_ASSERTIV = NULL
    WHERE CNPJ_OPERADORA = '29309127000179'
    """

    job = client.query(query)
    job.result()

    print(f"Linhas afetadas: {job.num_dml_affected_rows}")
