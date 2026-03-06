# -*- coding: utf-8 -*-
"""
Created on Fri Mar  6 12:40:31 2026

@author: kelvin.umbelino
"""

# -*- coding: utf-8 -*-
"""
Script de importação de arquivos do bucket de origem para bucket de destino
Adaptado para usar autenticação via variável de ambiente GCP_KEY
"""

import math
import re
import os
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
from fastavro import writer, parse_schema
from io import BytesIO
import time
import json
import logging
from datetime import datetime

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def authenticate_cloud() -> storage.Client:
    """
    Autentica usando a variável de ambiente GCP_KEY (mesmo padrão do CETIPO)
    """
    try:
        # Pegar a chave da variável de ambiente
        gcp_key = os.environ.get('GCP_KEY')
        if not gcp_key:
            raise ValueError("Variável de ambiente GCP_KEY não encontrada")
        
        # Parse do JSON
        service_account_info = json.loads(gcp_key)
        
        # Criar credenciais
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info
        )
        
        # Criar cliente do Storage
        client = storage.Client(
            credentials=credentials,
            project=credentials.project_id
        )
        
        logger.info("Autenticado com sucesso no GCS")
        return client
        
    except Exception as e:
        logger.error(f"Erro na autenticação: {e}")
        raise


def list_blobs(bucket_name: str, client: storage.Client) -> list:
    """Lista todos os blobs no bucket e retorna metadados"""
    try:
        listArchive = []
        blobs = client.list_blobs(bucket_name)
        
        for blob in blobs:
            tupla = blob.name.split('/')
            
            if len(tupla) == 6:
                file_ext = tupla[5].split('.')[-1].lower() if '.' in tupla[5] else ''
                
                if file_ext in ['txt', 'csv', 'xls', 'xlsx', 'pdf']:
                    listArchive.append(
                        criar_metadado(tupla, blob, file_ext)
                    )
        
        logger.info(f"Encontrados {len(listArchive)} arquivos no bucket {bucket_name}")
        return listArchive
        
    except Exception as e:
        logger.error(f"Erro ao listar blobs: {e}")
        return []


def criar_metadado(tupla: list, blob: storage.Blob, typeArchive: str) -> dict:
    """Cria dicionário com metadados do arquivo"""
    return {
        'Tipo': typeArchive,
        'Cliente': tupla[0],
        'Operadora': tupla[1],
        'Assunto': tupla[2],
        'Ano': tupla[3],
        'Mes': tupla[4],
        'Arquivo': tupla[5],
        'Caminho': blob.name
    }


def download_blob_into_memory(bucket_name: str, blob_name: str, 
                              file_type: str, assunto: str, 
                              archive_name: str, codificacao: int, 
                              grupo: str) -> list:
    """Download e parse de arquivos TXT"""
    
    client = authenticate_cloud()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    encoding = 'utf-8' if codificacao == 1 else 'ISO-8859-1'

    try:
        with blob.open("r", encoding=encoding) as files:
            fileArchive = []
            
            if assunto == 'mensalidades':
                if file_type == 'txt':
                    contrato = fatura = empresa = ''
                    
                    for file in files:
                        if file[0:9] == 'Contrato:':
                            header = file.split(':')
                            contrato = header[1].split('-')[0].strip()
                            fatura = header[2].split('#')[0].strip()
                            
                        elif file[0:8] == 'Empresa:':
                            header = file.split('-')
                            empresa = header[0].split(':')[1].strip() + '-' + header[1]
                            
                        else:
                            if file[0:1].isdigit():
                                dict_line = file.split('#')
                                fileArchive.append({
                                    'Codigo': dict_line[0],
                                    'Beneficiario': dict_line[1],
                                    'Matricula': dict_line[2],
                                    'CPF': dict_line[3],
                                    'Plano': dict_line[4],
                                    'Tipo': dict_line[5],
                                    'Idade': dict_line[6],
                                    'Dependencia': dict_line[7],
                                    'DataLimite': dict_line[8],
                                    'DataInclusao': dict_line[9],
                                    'DataExclusao': dict_line[10],
                                    'Lotacao': dict_line[11],
                                    'Rubrica': dict_line[12],
                                    'CoParticipacao': dict_line[13],
                                    'Outros': dict_line[14],
                                    'Mensalidade': dict_line[15],
                                    'TotalFamilia': dict_line[16],
                                    'Contrato': contrato,
                                    'Fatura': fatura,
                                    'Empresa': empresa,
                                    'Origem': archive_name
                                })
                                
            elif assunto == 'eventos':
                if file_type == 'txt':
                    for file in files:
                        dict_line = file.split('\t')
                        if len(dict_line) > 50:  # Validação básica
                            fileArchive.append({
                                'Operadora': dict_line[0],
                                'Contrato': dict_line[1],
                                'EmpresaNome': dict_line[2],
                                'Subfatura': dict_line[3],
                                'SubfaturaNome': dict_line[4],
                                'Uk1': dict_line[5],
                                'Uk2': dict_line[6],
                                'CodFamilia': dict_line[7],
                                'CarteirinhaTitular': dict_line[8],
                                'NomeTitular': dict_line[9],
                                'Cpf': dict_line[10],
                                'Uk3': dict_line[11],
                                'Carteirinha': dict_line[12],
                                'Nome': dict_line[13],
                                'DtNascimento': dict_line[14],
                                'Sexo': dict_line[15],
                                'Titular': dict_line[16],
                                'CodDependente': dict_line[17],
                                'GrauParentesco': dict_line[18],
                                'EstadoCivil': dict_line[19],
                                'PlanoCodigo': dict_line[20],
                                'PlanoNome': dict_line[21],
                                'Status': dict_line[22],
                                'Cidade': dict_line[23],
                                'Estado': dict_line[24],
                                'ProcedimentoCod': dict_line[25],
                                'ProcedimentoNome': dict_line[26],
                                'ProcedimentoTabela': dict_line[27],
                                'ProcedimentoC1': dict_line[28],
                                'ProcedimentoC2': dict_line[29],
                                'ProcedimentoC3': dict_line[30],
                                'ProcedimentoClassificacao': dict_line[31],
                                'ProcedimentoCodClass': dict_line[32],
                                'ProcedimentoEspecialidade': dict_line[33],
                                'Uk4': dict_line[34],
                                'Uk5': dict_line[35],
                                'GuiaCod': dict_line[36],
                                'Uk6': dict_line[37],
                                'RedeReembolso': dict_line[38],
                                'DtUtilizacao': dict_line[39],
                                'Uk7': dict_line[40],
                                'DtCompetencia': dict_line[41],
                                'CodAutorizacao': dict_line[42],
                                'PrestadorNome': dict_line[43],
                                'Cod1': dict_line[44],
                                'Valor1': dict_line[45],
                                'ValorSinistro': dict_line[46],
                                'Valor2': dict_line[47],
                                'Valor3': dict_line[48],
                                'Conselho': dict_line[49],
                                'ConselhoEstado': dict_line[50],
                                'PrestadorCodClass': dict_line[51],
                                'PrestadorClassificacao': dict_line[52],
                                'Cod5': dict_line[53],
                                'Uk8': dict_line[54],
                                'Cod6': dict_line[55],
                                'Uk9': dict_line[56],
                                'Cod7': dict_line[57],
                                'Grupo': grupo,
                                'Origem': archive_name
                            })
                            
            elif assunto == 'beneficiarios':
                if file_type == 'txt':
                    for file in files:
                        dict_line = file.split('\t')
                        if len(dict_line) > 25:
                            fileArchive.append({
                                'Operadora': dict_line[0],
                                'Contrato': dict_line[1],
                                'Subfatura': dict_line[2],
                                'EmpresaNome': dict_line[3],
                                'Uk1': dict_line[4],
                                'Uk2': dict_line[5],
                                'CodFamilia': dict_line[6],
                                'Carteirinha': dict_line[7],
                                'Nome': dict_line[8],
                                'Cpf': dict_line[9],
                                'Uk3': dict_line[10],
                                'DtNascimento': dict_line[11],
                                'Sexo': dict_line[12],
                                'Titular': dict_line[13],
                                'CodDependente': dict_line[14],
                                'GrauParentesco': dict_line[15],
                                'EstadoCivil': dict_line[16],
                                'PlanoCodigo': dict_line[17],
                                'PlanoNome': dict_line[18],
                                'Uk4': dict_line[19],
                                'Cidade': dict_line[20],
                                'Estado': dict_line[21],
                                'DtCompetencia': dict_line[22],
                                'DtInicioVigencia': dict_line[23],
                                'DtCancelamento': dict_line[24],
                                'CarteirinhaTitular': dict_line[25],
                                'Grupo': grupo,
                                'Origem': archive_name
                            })
            
            logger.info(f"Processados {len(fileArchive)} registros do arquivo {archive_name}")
            return fileArchive
            
    except Exception as e:
        logger.error(f"Erro ao processar arquivo {blob_name}: {e}")
        return []


def downloadExcel(archive: str, path: str, grupo: str, mes: str, ano_desejado: str) -> list:
    """Processa arquivo Excel de RG"""
    try:
        dt = mes + "/" + ano_desejado

        excel = pd.read_excel(
            path, sheet_name="rpt_AnaliseCustosReceitasSaudeC"
        )

        df = pd.DataFrame(excel)
        
        # Verifica se "Cód. Analisados:" está na planilha
        contains_text = df.apply(
            lambda row: row.astype(str).str.contains("Cód. Analisados:", na=False).any(), 
            axis=1
        ).any()
        
        df = df.to_dict('tight')
        df = pd.DataFrame(df['data']).drop(columns=[0, 1, 2, 3, 5, 6, 7, 8])
        df = df[df.iloc[:, 2] == dt]
        
        # Higieniza o dataframe
        df.fillna(0, inplace=True)
        
        result = trataDataFrame(df, archive, grupo)
        return result
        
    except Exception as e:
        logger.error(f"Erro ao processar Excel {archive}: {e}")
        return []


def trataDataFrame(dtFrame: pd.DataFrame, archive: str, grupo: str) -> list:
    """Trata o DataFrame extraído do Excel"""
    try:
        dicionary = {
            'cliente': grupo, 
            'meses': [dtFrame.iloc[0, 10]], 
            'receita': [dtFrame.iloc[0, 11]],
            'custoTotal': [dtFrame.iloc[0, 12]], 
            'sinistralidade': [dtFrame.iloc[0, 13]],
            'beneficiarioAtendido': [dtFrame.iloc[0, 14]], 
            'custoPerCapita': [dtFrame.iloc[0, 15]],
            'vidas': [dtFrame.iloc[0, 16]]
        }

        df = pd.DataFrame(dicionary)
        result = []
        
        for i in df.itertuples(index=False):
            result.append({
                'mes': str(i.meses),
                'receita': str(trataNaN(i.receita)),
                'custoTotal': str(trataNaN(i.custoTotal)),
                'sinistralidade': str(i.sinistralidade),
                'beneficiarioAtendido': str(i.beneficiarioAtendido),
                'custoPerCapita': str(i.custoPerCapita),
                'vidas': str(trataNaN(i.vidas)),
                'cliente': grupo,
                'grupo': grupo,
                'origem': archive
            })

        return result
        
    except Exception as e:
        logger.error(f"Erro no trataDataFrame: {e}")
        return []


def trataNaN(valor):
    """Trata valores NaN"""
    try:
        valor = float(valor)
        if math.isnan(valor):
            return 'NaN'
        return valor
    except (ValueError, TypeError):
        return valor


def normalize_to_string(records: list) -> list:
    """Normaliza todos os valores para string"""
    normalized = []
    for row in records:
        new_row = {}
        for k, v in row.items():
            if v is None:
                new_row[k] = ''
            else:
                new_row[k] = str(v)
        normalized.append(new_row)
    return normalized


def schemaAvro(listData: list, file_name: str, assunto: str) -> str:
    """Cria schema Avro baseado no tipo de dado"""
    
    listData = normalize_to_string(listData)
    
    schemas = {
        'mensalidades': {
            'doc': 'assertiv business inteligence',
            'name': 'raw',
            'namespace': 'Mensalidades',
            'type': 'record',
            'fields': [
                {"name": "Codigo", "type": "string"},
                {"name": "Beneficiario", "type": "string"},
                {"name": "Matricula", "type": "string"},
                {"name": "CPF", "type": "string"},
                {"name": "Plano", "type": "string"},
                {"name": "Tipo", "type": "string"},
                {"name": "Idade", "type": "string"},
                {"name": "Dependencia", "type": "string"},
                {"name": "DataLimite", "type": "string"},
                {"name": "DataInclusao", "type": "string"},
                {"name": "DataExclusao", "type": "string"},
                {"name": "Lotacao", "type": "string"},
                {"name": "Rubrica", "type": "string"},
                {"name": "CoParticipacao", "type": "string"},
                {"name": "Outros", "type": "string"},
                {"name": "Mensalidade", "type": "string"},
                {"name": "TotalFamilia", "type": "string"},
                {"name": "Contrato", "type": "string"},
                {"name": "Fatura", "type": "string"},
                {"name": "Empresa", "type": "string"},
                {"name": "Origem", "type": "string"},
            ]
        },
        'eventos': {
            'doc': 'assertiv business inteligence',
            'name': 'raw',
            'namespace': 'Eventos',
            'type': 'record',
            'fields': [
                {'name': 'Operadora', 'type': 'string'},
                {'name': 'Contrato', 'type': 'string'},
                {'name': 'EmpresaNome', 'type': 'string'},
                {'name': 'Subfatura', 'type': 'string'},
                {'name': 'SubfaturaNome', 'type': 'string'},
                {'name': 'Uk1', 'type': 'string'},
                {'name': 'Uk2', 'type': 'string'},
                {'name': 'CodFamilia', 'type': 'string'},
                {'name': 'CarteirinhaTitular', 'type': 'string'},
                {'name': 'NomeTitular', 'type': 'string'},
                {'name': 'Cpf', 'type': 'string'},
                {'name': 'Uk3', 'type': 'string'},
                {'name': 'Carteirinha', 'type': 'string'},
                {'name': 'Nome', 'type': 'string'},
                {'name': 'DtNascimento', 'type': 'string'},
                {'name': 'Sexo', 'type': 'string'},
                {'name': 'Titular', 'type': 'string'},
                {'name': 'CodDependente', 'type': 'string'},
                {'name': 'GrauParentesco', 'type': 'string'},
                {'name': 'EstadoCivil', 'type': 'string'},
                {'name': 'PlanoCodigo', 'type': 'string'},
                {'name': 'PlanoNome', 'type': 'string'},
                {'name': 'Status', 'type': 'string'},
                {'name': 'Cidade', 'type': 'string'},
                {'name': 'Estado', 'type': 'string'},
                {'name': 'ProcedimentoCod', 'type': 'string'},
                {'name': 'ProcedimentoNome', 'type': 'string'},
                {'name': 'ProcedimentoTabela', 'type': 'string'},
                {'name': 'ProcedimentoC1', 'type': 'string'},
                {'name': 'ProcedimentoC2', 'type': 'string'},
                {'name': 'ProcedimentoC3', 'type': 'string'},
                {'name': 'ProcedimentoClassificacao', 'type': 'string'},
                {'name': 'ProcedimentoCodClass', 'type': 'string'},
                {'name': 'ProcedimentoEspecialidade', 'type': 'string'},
                {'name': 'Uk4', 'type': 'string'},
                {'name': 'Uk5', 'type': 'string'},
                {'name': 'GuiaCod', 'type': 'string'},
                {'name': 'Uk6', 'type': 'string'},
                {'name': 'RedeReembolso', 'type': 'string'},
                {'name': 'DtUtilizacao', 'type': 'string'},
                {'name': 'Uk7', 'type': 'string'},
                {'name': 'DtCompetencia', 'type': 'string'},
                {'name': 'CodAutorizacao', 'type': 'string'},
                {'name': 'PrestadorNome', 'type': 'string'},
                {'name': 'Cod1', 'type': 'string'},
                {'name': 'Valor1', 'type': 'string'},
                {'name': 'ValorSinistro', 'type': 'string'},
                {'name': 'Valor2', 'type': 'string'},
                {'name': 'Valor3', 'type': 'string'},
                {'name': 'Conselho', 'type': 'string'},
                {'name': 'ConselhoEstado', 'type': 'string'},
                {'name': 'PrestadorCodClass', 'type': 'string'},
                {'name': 'PrestadorClassificacao', 'type': 'string'},
                {'name': 'Cod5', 'type': 'string'},
                {'name': 'Uk8', 'type': 'string'},
                {'name': 'Cod6', 'type': 'string'},
                {'name': 'Uk9', 'type': 'string'},
                {'name': 'Cod7', 'type': 'string'},
                {'name': 'Grupo', 'type': 'string'},
                {"name": "Origem", "type": "string"},
            ]
        },
        'beneficiarios': {
            'doc': 'assertiv business inteligence',
            'name': 'raw',
            'namespace': 'Beneficiarios',
            'type': 'record',
            'fields': [
                {'name': 'Operadora', 'type': 'string'},
                {'name': 'Contrato', 'type': 'string'},
                {'name': 'Subfatura', 'type': 'string'},
                {'name': 'EmpresaNome', 'type': 'string'},
                {'name': 'Uk1', 'type': 'string'},
                {'name': 'Uk2', 'type': 'string'},
                {'name': 'CodFamilia', 'type': 'string'},
                {'name': 'Carteirinha', 'type': 'string'},
                {'name': 'Nome', 'type': 'string'},
                {'name': 'Cpf', 'type': 'string'},
                {'name': 'Uk3', 'type': 'string'},
                {'name': 'DtNascimento', 'type': 'string'},
                {'name': 'Sexo', 'type': 'string'},
                {'name': 'Titular', 'type': 'string'},
                {'name': 'CodDependente', 'type': 'string'},
                {'name': 'GrauParentesco', 'type': 'string'},
                {'name': 'EstadoCivil', 'type': 'string'},
                {'name': 'PlanoCodigo', 'type': 'string'},
                {'name': 'PlanoNome', 'type': 'string'},
                {'name': 'Uk4', 'type': 'string'},
                {'name': 'Cidade', 'type': 'string'},
                {'name': 'Estado', 'type': 'string'},
                {'name': 'DtCompetencia', 'type': 'string'},
                {'name': 'DtInicioVigencia', 'type': 'string'},
                {'name': 'DtCancelamento', 'type': 'string'},
                {'name': 'CarteirinhaTitular', 'type': 'string'},
                {'name': 'Grupo', 'type': 'string'},
                {"name": "Origem", "type": "string"},
            ]
        },
        'RG': {
            'doc': 'assertiv business inteligence',
            'name': 'raw',
            'namespace': 'sinistralidade',
            'type': 'record',
            'fields': [
                {'name': 'mes', 'type': 'string'},
                {'name': 'receita', 'type': 'string'},
                {'name': 'custoTotal', 'type': 'string'},
                {'name': 'sinistralidade', 'type': 'string'},
                {'name': 'beneficiarioAtendido', 'type': 'string'},
                {'name': 'custoPerCapita', 'type': 'string'},
                {'name': 'vidas', 'type': 'string'},
                {'name': 'cliente', 'type': 'string'},
                {'name': 'grupo', 'type': 'string'},
                {'name': 'origem', 'type': 'string'},
            ]
        }
    }

    parsed_schema = parse_schema(schemas.get(assunto, schemas['RG']))

    # Cria o arquivo avro
    avro_filename = f'{file_name}.avro'
    with open(avro_filename, 'wb') as file:
        try:
            writer(file, parsed_schema, listData)
            logger.info(f"Arquivo Avro criado: {avro_filename} com {len(listData)} registros")
        except Exception as e:
            logger.error(f"Erro ao criar arquivo Avro {file_name}: {e}")
            
    return avro_filename


def upload_bucket(bucket_name: str, file_name: str, destination_name: str) -> bool:
    """Upload de arquivo para o bucket GCS"""
    try:
        client = authenticate_cloud()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_name)
        
        # Upload do arquivo
        blob.upload_from_filename(file_name)
        
        logger.info(f'Arquivo: {file_name} enviado para gs://{bucket_name}/{destination_name}')
        return True
        
    except Exception as e:
        logger.error(f'Erro ao enviar {file_name}: {e}')
        return False


def filterData(line: str) -> str:
    """Tratamento de caracteres especiais"""
    padrao = r'[^a-zA-Z0-9áéíóúÁÉÍÓÚâêîôûÂÊÎÔÛàèìòùÀÈÌÒÙãõÃÕçÇ/\s().:]'
    return re.sub(padrao, '', line)


def deleteArchive(file: str):
    """Remove arquivo local"""
    try:
        os.remove(file)
        logger.info(f"Arquivo removido: {file}")
    except Exception as e:
        logger.error(f"Erro ao remover arquivo {file}: {e}")


def processar_importacao(ano: str, mes: str, mes_extenso: str):
    """
    Função principal para processar importação
    Args:
        ano: Ano desejado (ex: '2025')
        mes: Mês numérico (ex: '12')
        mes_extenso: Mês por extenso (ex: 'Dezembro')
    """
    try:
        # Buckets
        bucket_origem = 'assertiv-amil'
        bucket_destino = 'assertiv_analytics'
        
        logger.info(f"Iniciando importação para {mes_extenso}/{ano}")
        
        # Autenticar e listar arquivos
        client = authenticate_cloud()
        listArchives = list_blobs(bucket_origem, client)
        
        # Filtrar por ano e mês
        listArchives = [
            item for item in listArchives 
            if item['Ano'] == ano and item['Mes'] == mes
        ]
        
        logger.info(f"Encontrados {len(listArchives)} arquivos para processar")
        
        resultados = {
            'processados': 0,
            'erros': 0,
            'detalhes': []
        }

        for listArchive in listArchives:
            nameArchive = f"{listArchive['Ano']}{listArchive['Mes']}_{listArchive['Arquivo'].split('.')[0]}"
            
            try:
                cliente = listArchive['Cliente'].split(' ')[0]
                
                # Processar baseado no tipo
                if listArchive['Operadora'].lower() == 'amil' and listArchive['Assunto'] == 'RG':
                    caminho = f"gs://{bucket_origem}/{listArchive['Caminho']}"
                    arquivo = listArchive['Arquivo']
                    
                    # Processar Excel
                    dados = downloadExcel(arquivo, caminho, cliente, mes_extenso, ano)
                    
                    if dados:
                        # Criar Avro
                        avroFile = schemaAvro(dados, nameArchive, listArchive['Assunto'])
                        
                        # Upload
                        destination_name = f"Amil/RG/raw/{avroFile}"
                        if upload_bucket(bucket_destino, avroFile, destination_name):
                            deleteArchive(avroFile)
                            resultados['processados'] += 1
                            resultados['detalhes'].append({
                                'arquivo': nameArchive,
                                'tipo': 'RG',
                                'status': 'sucesso'
                            })
                    
                elif listArchive['Operadora'].lower() == 'amil' and listArchive['Assunto'] == 'beneficiarios':
                    dados = download_blob_into_memory(
                        bucket_origem, listArchive['Caminho'], 
                        listArchive['Tipo'], listArchive['Assunto'], 
                        nameArchive, 2, cliente
                    )
                    
                    if dados:
                        avroFile = schemaAvro(dados, nameArchive, listArchive['Assunto'])
                        destination_name = f"Amil/beneficiarios/raw/{avroFile}"
                        
                        if upload_bucket(bucket_destino, avroFile, destination_name):
                            deleteArchive(avroFile)
                            resultados['processados'] += 1
                            resultados['detalhes'].append({
                                'arquivo': nameArchive,
                                'tipo': 'beneficiarios',
                                'status': 'sucesso'
                            })
                    
                elif listArchive['Operadora'].lower() == 'amil' and listArchive['Assunto'] == 'eventos':
                    dados = download_blob_into_memory(
                        bucket_origem, listArchive['Caminho'], 
                        listArchive['Tipo'], listArchive['Assunto'], 
                        nameArchive, 1, cliente
                    )
                    
                    if dados:
                        avroFile = schemaAvro(dados, nameArchive, listArchive['Assunto'])
                        destination_name = f"Amil/eventos/raw/{avroFile}"
                        
                        if upload_bucket(bucket_destino, avroFile, destination_name):
                            deleteArchive(avroFile)
                            resultados['processados'] += 1
                            resultados['detalhes'].append({
                                'arquivo': nameArchive,
                                'tipo': 'eventos',
                                'status': 'sucesso'
                            })
                            
            except Exception as e:
                logger.error(f"Erro processando {nameArchive}: {e}")
                resultados['erros'] += 1
                resultados['detalhes'].append({
                    'arquivo': nameArchive,
                    'erro': str(e),
                    'status': 'erro'
                })
        
        logger.info(f"Importação concluída: {resultados['processados']} sucessos, {resultados['erros']} erros")
        return resultados
        
    except Exception as e:
        logger.error(f"Erro na importação: {e}")
        raise


if __name__ == '__main__':
    # Exemplo de execução standalone
    processar_importacao('2025', '12', 'Dezembro')