# -*- coding: utf-8 -*-
"""
Created on Mon Mar  2 17:55:26 2026

@author: kelvin.umbelino
"""

import os
import json
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime

def get_credentials():
    """
    Função para obter credenciais de diferentes fontes:
    1. Variável de ambiente GOOGLE_APPLICATION_CREDENTIALS
    2. Arquivo JSON em caminho específico
    3. Conexão do Airflow
    """
    
    # Opção 1: Via variável de ambiente (recomendado para Docker)
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    if credentials_path and os.path.exists(credentials_path):
        print(f"Usando credenciais do arquivo: {credentials_path}")
        return service_account.Credentials.from_service_account_file(credentials_path)
    
    # Opção 2: Via caminho absoluto (fallback para desenvolvimento local)
    local_paths = [
        "C:/Users/kelvin.umbelino.ASSERTIVDC/Documents/airflow-docker/key/gcp.json",
        "/opt/airflow/credentials/gcp.json",  # Caminho no container
        "./key/gcp.json",  # Caminho relativo
    ]
    
    for path in local_paths:
        if os.path.exists(path):
            print(f"Usando credenciais do arquivo: {path}")
            return service_account.Credentials.from_service_account_file(path)
    
    # Opção 3: Via Airflow Connection (se estiver no Airflow)
    try:
        from airflow.hooks.base import BaseHook
        connection = BaseHook.get_connection('google_cloud_default')
        if connection.extra_dejson.get('keyfile_dict'):
            print("Usando credenciais da conexão do Airflow")
            keyfile_dict = json.loads(connection.extra_dejson['keyfile_dict'])
            return service_account.Credentials.from_service_account_info(keyfile_dict)
    except:
        pass
    
    raise Exception("Não foi possível encontrar as credenciais do Google Cloud!")

def main():
    try:
        # ----- Obtém credenciais de forma segura -----
        credentials = get_credentials()
        
        # ----- Client BigQuery -----
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        print(f"Conectado ao projeto: {credentials.project_id}")
        
        # ----- Período da competência -----
        competencia_de = '2018-01-01'
        competencia_ate = '2025-12-01'
        
        # Lista para armazenar resultados de cada query
        resultados = []
        
        # Query 1: NULL
        insert_query = f"""
        update `assertiv.business_analytics_gold.gold_utilizacao` 
        set CLASSIFICACAO_ASSERTIV = NULL
        where CNPJ_OPERADORA = '29309127000179'
        """
        resultados.append(executar_query(client, insert_query, "Marcando registro como NULL"))
        
        # Query 2: PEONA (CNPJ_PRESTADOR)
        insert_query = f"""
        update `assertiv.business_analytics_gold.gold_utilizacao` 
        set CLASSIFICACAO_ASSERTIV = 'PEONA'
        where CNPJ_PRESTADOR = '00000000000000' and COD_PROCEDIMENTO = ''
        and CNPJ_OPERADORA = '29309127000179'
        """
        resultados.append(executar_query(client, insert_query, "Update PEONA (CNPJ_PRESTADOR)"))
        
        # Query 3: PEONA (NOME_PLANO)
        insert_query = f"""
        update `assertiv.business_analytics_gold.gold_utilizacao` 
        set CLASSIFICACAO_ASSERTIV = 'PEONA'
        where NOME_PLANO is null
        and CNPJ_OPERADORA = '29309127000179'
        """
        resultados.append(executar_query(client, insert_query, "Update PEONA (NOME_PLANO)"))
        
        # Query 4: PRONTO SOCORRO
        insert_query = f"""
        update `assertiv.business_analytics_gold.gold_utilizacao` u set CLASSIFICACAO_ASSERTIV = 'PRONTO SOCORRO'
        from
        (select distinct u.COD_AUTORIZACAO
        from `assertiv.business_analytics_gold.gold_utilizacao` u
        left join `assertiv.business_analytics_gold.gold_operadora_prestadores` prest 
          on (prest.CNPJ_PRESTADOR = u.CNPJ_PRESTADOR and prest.CNPJ_OPERADORA = u.CNPJ_OPERADORA)
        left join `assertiv.business_analytics_gold.gold_operadora_procedimentos` proc
          on (proc.COD_PROCEDIMENTO = u.COD_PROCEDIMENTO and proc.CNPJ_OPERADORA = u.CNPJ_OPERADORA)
        where 
        u.CNPJ_OPERADORA = '29309127000179'
        and (LEFT(u.COD_DOCUMENTO, 2) = 'CO' OR LEFT(u.COD_AUTORIZACAO, 2) = 'CO') 
        and (upper(proc.NOME_PROCEDIMENTO) like '%PRONTO%' OR upper(proc.NOME_PROCEDIMENTO) like '%PS%')) a
        where 
        u.CNPJ_OPERADORA = '29309127000179'
        and a.COD_AUTORIZACAO = u.COD_AUTORIZACAO;
        """
        resultados.append(executar_query(client, insert_query, "Update PRONTO SOCORRO"))
        
        # Query 5: CONSULTA
        insert_query = f"""
        update `assertiv.business_analytics_gold.gold_utilizacao` u set CLASSIFICACAO_ASSERTIV = 'CONSULTA'
        from
        (select distinct u.COD_AUTORIZACAO
        from `assertiv.business_analytics_gold.gold_utilizacao` u
        left join `assertiv.business_analytics_gold.gold_operadora_prestadores` prest 
          on (prest.CNPJ_PRESTADOR = u.CNPJ_PRESTADOR and prest.CNPJ_OPERADORA = u.CNPJ_OPERADORA)
        left join `assertiv.business_analytics_gold.gold_operadora_procedimentos` proc
          on (proc.COD_PROCEDIMENTO = u.COD_PROCEDIMENTO and proc.CNPJ_OPERADORA = u.CNPJ_OPERADORA)
        where 
        u.CNPJ_OPERADORA = '29309127000179'
        and (LEFT(u.COD_DOCUMENTO, 2) = 'CO' OR LEFT(u.COD_AUTORIZACAO, 2) = 'CO') 
        and (upper(proc.NOME_PROCEDIMENTO) not like '%PRONTO%' and upper(proc.NOME_PROCEDIMENTO) not like '%PS%')
        and u.CLASSIFICACAO_ASSERTIV is null) a
        where 
        u.CNPJ_OPERADORA = '29309127000179'
        and a.COD_AUTORIZACAO = u.COD_AUTORIZACAO;
        """
        resultados.append(executar_query(client, insert_query, "Update CONSULTA"))
        
        # Query 6: EXAME
        insert_query = f"""
        update `assertiv.business_analytics_gold.gold_utilizacao` u set CLASSIFICACAO_ASSERTIV = 'EXAME'
        from
        (select distinct u.COD_AUTORIZACAO
        from `assertiv.business_analytics_gold.gold_utilizacao` u
        left join `assertiv.business_analytics_gold.gold_operadora_prestadores` prest 
          on (prest.CNPJ_PRESTADOR = u.CNPJ_PRESTADOR and prest.CNPJ_OPERADORA = u.CNPJ_OPERADORA)
        left join `assertiv.business_analytics_gold.gold_operadora_procedimentos` proc
          on (proc.COD_PROCEDIMENTO = u.COD_PROCEDIMENTO and proc.CNPJ_OPERADORA = u.CNPJ_OPERADORA)
        where 
        u.CNPJ_OPERADORA = '29309127000179'
        and LEFT(u.COD_DOCUMENTO, 2) = 'EX'
        and u.CLASSIFICACAO_ASSERTIV is null) a
        where 
        u.CNPJ_OPERADORA = '29309127000179'
        and a.COD_AUTORIZACAO = u.COD_AUTORIZACAO;
        """
        resultados.append(executar_query(client, insert_query, "Update EXAME"))
        
        # Query 7: TERAPIA
        insert_query = f"""
        update `assertiv.business_analytics_gold.gold_utilizacao` u set CLASSIFICACAO_ASSERTIV = 'TERAPIA'
        from
        (select distinct u.COD_AUTORIZACAO
        from `assertiv.business_analytics_gold.gold_utilizacao` u
        left join `assertiv.business_analytics_gold.gold_operadora_prestadores` prest 
          on (prest.CNPJ_PRESTADOR = u.CNPJ_PRESTADOR and prest.CNPJ_OPERADORA = u.CNPJ_OPERADORA)
        left join `assertiv.business_analytics_gold.gold_operadora_procedimentos` proc
          on (proc.COD_PROCEDIMENTO = u.COD_PROCEDIMENTO and proc.CNPJ_OPERADORA = u.CNPJ_OPERADORA)
        where 
        u.CNPJ_OPERADORA = '29309127000179'
        and LEFT(u.COD_DOCUMENTO, 2) = 'TR'
        and u.CLASSIFICACAO_ASSERTIV is null) a
        where
        u.CNPJ_OPERADORA = '29309127000179'
        and a.COD_AUTORIZACAO = u.COD_AUTORIZACAO;
        """
        resultados.append(executar_query(client, insert_query, "Update TERAPIA"))
        
        # Query 8: INTERNACAO
        insert_query = f"""
        UPDATE assertiv.business_analytics_gold.gold_utilizacao u
        SET CLASSIFICACAO_ASSERTIV = 'INTERNACAO'
        FROM
            (SELECT DISTINCT u.COD_AUTORIZACAO
             FROM assertiv.business_analytics_gold.gold_utilizacao u
             LEFT JOIN assertiv.business_analytics_gold.gold_operadora_prestadores prest
               ON prest.CNPJ_PRESTADOR = u.CNPJ_PRESTADOR
               AND prest.CNPJ_OPERADORA = u.CNPJ_OPERADORA
             LEFT JOIN assertiv.business_analytics_gold.gold_operadora_procedimentos proc
               ON proc.COD_PROCEDIMENTO = u.COD_PROCEDIMENTO
               AND proc.CNPJ_OPERADORA = u.CNPJ_OPERADORA
             WHERE u.CNPJ_OPERADORA = '29309127000179'
               AND u.CLASSIFICACAO_OPERADORA LIKE 'I.%'
               AND u.CLASSIFICACAO_ASSERTIV IS NULL
            ) a
        WHERE u.CNPJ_OPERADORA = '29309127000179'
          AND a.COD_AUTORIZACAO = u.COD_AUTORIZACAO;
        """
        resultados.append(executar_query(client, insert_query, "Update INTERNACAO"))
        
        # Query 9: OUTROS
        insert_query = f"""
        update `assertiv.business_analytics_gold.gold_utilizacao` u set CLASSIFICACAO_ASSERTIV = 'OUTROS'
        from
        (select distinct u.COD_AUTORIZACAO
        from `assertiv.business_analytics_gold.gold_utilizacao` u
        left join `assertiv.business_analytics_gold.gold_operadora_prestadores` prest 
          on (prest.CNPJ_PRESTADOR = u.CNPJ_PRESTADOR and prest.CNPJ_OPERADORA = u.CNPJ_OPERADORA)
        left join `assertiv.business_analytics_gold.gold_operadora_procedimentos` proc
          on (proc.COD_PROCEDIMENTO = u.COD_PROCEDIMENTO and proc.CNPJ_OPERADORA = u.CNPJ_OPERADORA)
        where 
        u.CNPJ_OPERADORA = '29309127000179'
        and u.CLASSIFICACAO_ASSERTIV is null) a
        where 
        u.CNPJ_OPERADORA = '29309127000179'
        and a.COD_AUTORIZACAO = u.COD_AUTORIZACAO;
        """
        resultados.append(executar_query(client, insert_query, "Update OUTROS"))
        
        # Resume resultados
        print("\n" + "="*50)
        print("RESUMO DAS ATUALIZAÇÕES:")
        print("="*50)
        for resultado in resultados:
            print(f"{resultado['descricao']}: {resultado['linhas']} registros")
        
    except Exception as e:
        print(f"ERRO na execução: {str(e)}")
        raise

def executar_query(client, query, descricao):
    """Função auxiliar para executar queries e retornar resultados"""
    try:
        print(f"\nExecutando: {descricao}...")
        job = client.query(query)
        job.result()  # Aguarda conclusão
        linhas = job.num_dml_affected_rows
        print(f"✓ {descricao}: {linhas} registros afetados")
        return {"descricao": descricao, "linhas": linhas}
    except Exception as e:
        print(f"✗ Erro em {descricao}: {str(e)}")
        return {"descricao": descricao, "linhas": 0, "erro": str(e)}

if __name__ == "__main__":
    main()
