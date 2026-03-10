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

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def authenticate_cloud() -> storage.Client:
    """Autentica usando credenciais da variável de ambiente GCP_KEY"""
    try:
        # Buscar credenciais da variável de ambiente
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
        
        # Criar credenciais e cliente
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=['https://www.googleapis.com/auth/cloud-platform']
        )
        
        # Criar cliente storage com as credenciais
        client = storage.Client(
            credentials=credentials,
            project=credentials.project_id
        )
        
        logger.info(f"Conectado ao projeto: {credentials.project_id}")
        return client
        
    except Exception as e:
        logger.error(f"Erro na autenticação: {e}")
        raise


def instance_bucket(client: storage.Client) -> storage.Client:
    """Retorna o cliente já autenticado"""
    return client


def list_blobs(bucket_name: str, client: storage.Client) -> list:
    try:
        listArchive = []
        blobs = client.list_blobs(bucket_name)
        for blob in blobs:
            tupla = blob.name.split('/')
            if len(tupla) == 6:
                if tupla[5].split('.')[1] in ['txt', 'csv', 'xls', 'xlsx', 'pdf']:
                    listArchive.append(
                        create_list_entry(tupla, blob, tupla[5].split('.')[1])
                    )
        logger.info(f"Total de blobs encontrados: {len(listArchive)}")
        return listArchive
    except Exception as e:
        logger.error(f"Erro ao listar blobs: {e}")
        return []
    finally:
        return listArchive


def create_list_entry(tupla: list, blob: storage.Blob, typeArchive: str) -> dict:
    createLine = {
        'Tipo': typeArchive,
        'Cliente': tupla[0],
        'Operadora': tupla[1],
        'Assunto': tupla[2],
        'Ano': tupla[3],
        'Mes': tupla[4],
        'Arquivo': tupla[5],
        'Caminho': blob.name
    }
    return createLine


def download_blob_into_memory(bucket_name: str, blob_name: str, type: str, assunt: str, archive: str, codificacao: int, grupo: str) -> list:
    
    client = authenticate_cloud()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    encoding = 'utf-8' if codificacao == 1 else 'ISO-8859-1'

    fileArchive = []
    
    try:
        with blob.open("r", encoding=encoding) as files:
            if assunt == 'mensalidades':
                if type == 'txt':
                    contrato = ''
                    fatura = ''
                    empresa = ''
                    
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
                                dict_items = file.split('#')
                                fileArchive.append({
                                    'Codigo': dict_items[0],
                                    'Beneficiario': dict_items[1],
                                    'Matricula': dict_items[2],
                                    'CPF': dict_items[3],
                                    'Plano': dict_items[4],
                                    'Tipo': dict_items[5],
                                    'Idade': dict_items[6],
                                    'Dependencia': dict_items[7],
                                    'DataLimite': dict_items[8],
                                    'DataInclusao': dict_items[9],
                                    'DataExclusao': dict_items[10],
                                    'Lotacao': dict_items[11],
                                    'Rubrica': dict_items[12],
                                    'CoParticipacao': dict_items[13],
                                    'Outros': dict_items[14],
                                    'Mensalidade': dict_items[15],
                                    'TotalFamilia': dict_items[16],
                                    'Contrato': contrato,
                                    'Fatura': fatura,
                                    'Empresa': empresa,
                                    'Origem': archive
                                })
                                
            elif assunt == 'eventos':
                if type == 'txt':
                    for file in files:
                        dict_items = file.split('\t')
                        if len(dict_items) >= 58:  # Verifica se tem colunas suficientes
                            fileArchive.append({
                                'Operadora': dict_items[0],
                                'Contrato': dict_items[1],
                                'EmpresaNome': dict_items[2],
                                'Subfatura': dict_items[3],
                                'SubfaturaNome': dict_items[4],
                                'Uk1': dict_items[5],
                                'Uk2': dict_items[6],
                                'CodFamilia': dict_items[7],
                                'CarteirinhaTitular': dict_items[8],
                                'NomeTitular': dict_items[9],
                                'Cpf': dict_items[10],
                                'Uk3': dict_items[11],
                                'Carteirinha': dict_items[12],
                                'Nome': dict_items[13],
                                'DtNascimento': dict_items[14],
                                'Sexo': dict_items[15],
                                'Titular': dict_items[16],
                                'CodDependente': dict_items[17],
                                'GrauParentesco': dict_items[18],
                                'EstadoCivil': dict_items[19],
                                'PlanoCodigo': dict_items[20],
                                'PlanoNome': dict_items[21],
                                'Status': dict_items[22],
                                'Cidade': dict_items[23],
                                'Estado': dict_items[24],
                                'ProcedimentoCod': dict_items[25],
                                'ProcedimentoNome': dict_items[26],
                                'ProcedimentoTabela': dict_items[27],
                                'ProcedimentoC1': dict_items[28],
                                'ProcedimentoC2': dict_items[29],
                                'ProcedimentoC3': dict_items[30],
                                'ProcedimentoClassificacao': dict_items[31],
                                'ProcedimentoCodClass': dict_items[32],
                                'ProcedimentoEspecialidade': dict_items[33],
                                'Uk4': dict_items[34],
                                'Uk5': dict_items[35],
                                'GuiaCod': dict_items[36],
                                'Uk6': dict_items[37],
                                'RedeReembolso': dict_items[38],
                                'DtUtilizacao': dict_items[39],
                                'Uk7': dict_items[40],
                                'DtCompetencia': dict_items[41],
                                'CodAutorizacao': dict_items[42],
                                'PrestadorNome': dict_items[43],
                                'Cod1': dict_items[44],
                                'Valor1': dict_items[45],
                                'ValorSinistro': dict_items[46],
                                'Valor2': dict_items[47],
                                'Valor3': dict_items[48],
                                'Conselho': dict_items[49],
                                'ConselhoEstado': dict_items[50],
                                'PrestadorCodClass': dict_items[51],
                                'PrestadorClassificacao': dict_items[52],
                                'Cod5': dict_items[53],
                                'Uk8': dict_items[54],
                                'Cod6': dict_items[55],
                                'Uk9': dict_items[56],
                                'Cod7': dict_items[57],
                                'Grupo': grupo,
                                'Origem': archive
                            })
                            
            elif assunt == 'beneficiarios':
                if type == 'txt':
                    for file in files:
                        dict_items = file.split('\t')
                        if len(dict_items) >= 26:  # Verifica se tem colunas suficientes
                            fileArchive.append({
                                'Operadora': dict_items[0],
                                'Contrato': dict_items[1],
                                'Subfatura': dict_items[2],
                                'EmpresaNome': dict_items[3],
                                'Uk1': dict_items[4],
                                'Uk2': dict_items[5],
                                'CodFamilia': dict_items[6],
                                'Carteirinha': dict_items[7],
                                'Nome': dict_items[8],
                                'Cpf': dict_items[9],
                                'Uk3': dict_items[10],
                                'DtNascimento': dict_items[11],
                                'Sexo': dict_items[12],
                                'Titular': dict_items[13],
                                'CodDependente': dict_items[14],
                                'GrauParentesco': dict_items[15],
                                'EstadoCivil': dict_items[16],
                                'PlanoCodigo': dict_items[17],
                                'PlanoNome': dict_items[18],
                                'Uk4': dict_items[19],
                                'Cidade': dict_items[20],
                                'Estado': dict_items[21],
                                'DtCompetencia': dict_items[22],
                                'DtInicioVigencia': dict_items[23],
                                'DtCancelamento': dict_items[24],
                                'CarteirinhaTitular': dict_items[25],
                                'Grupo': grupo,
                                'Origem': archive
                            })
        
        logger.info(f"Download concluído: {len(fileArchive)} registros de {assunt}")
        return fileArchive
        
    except Exception as e:
        logger.error(f"Erro ao baixar blob {blob_name}: {e}")
        return []


def downloadExcel(archive: str, path: str, grupo: str, mes: str, ano_desejado: str) -> list:
    """Baixa e processa arquivo Excel"""
    try:
        dt = mes + "/" + ano_desejado

        excel = pd.read_excel(path, sheet_name="rpt_AnaliseCustosReceitasSaudeC")
        df = pd.DataFrame(excel)
        
        # Converte para dicionário e processa
        df = df.to_dict('tight')
        df = pd.DataFrame(df['data']).drop(columns=[0, 1, 2, 3, 5, 6, 7, 8])
        
        df = df[df.iloc[:, 2] == dt]
        
        # Higieniza o dataframe
        df.fillna(0, inplace=True)
        
        df = trataDataFrame(df, archive, grupo)
        
        logger.info(f"Excel processado: {len(df)} registros")
        return df
        
    except Exception as e:
        logger.error(f"Erro ao processar Excel {archive}: {e}")
        return []


def trataDataFrame(dtFrame: pd.DataFrame, archive: str, grupo: str) -> list:
    """Trata o DataFrame para o formato desejado"""
    
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
    
    def extrai_valor(v):
        if isinstance(v, pd.Series):
            return v.iloc[0] if not v.empty else None
        return v
    
    for i in df.itertuples(index=False):
        mes = extrai_valor(i.meses)
        receita = extrai_valor(i.receita)
        custoTotal = extrai_valor(i.custoTotal)
        sinistralidade = extrai_valor(i.sinistralidade)
        beneficiarioAtendido = extrai_valor(i.beneficiarioAtendido)
        custoPerCapita = extrai_valor(i.custoPerCapita)
        vidas = extrai_valor(i.vidas)
    
        result.append({
            'mes': str(mes) if mes else '',
            'receita': trataNaN(receita),
            'custoTotal': trataNaN(custoTotal),
            'sinistralidade': trataNaN(sinistralidade),
            'beneficiarioAtendido': trataNaN(beneficiarioAtendido),
            'custoPerCapita': trataNaN(custoPerCapita),
            'vidas': trataNaN(vidas),
            'cliente': grupo,
            'grupo': grupo,
            'origem': archive
        })

    return result


def trataNaN(valor):
    """Trata valores NaN"""
    try:
        if pd.isna(valor):
            return 'NaN'
        valor = float(valor)
        if math.isnan(valor):
            return 'NaN'
        return str(valor)
    except (ValueError, TypeError):
        return str(valor) if valor is not None else ''


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


def schemaAvro(listData: list, file_name: str, assunt: str) -> str:
    """Cria schema Avro baseado no tipo de assunto"""
    
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
            ],
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
            ],
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
            ],
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
            ],
        }
    }
    
    schema = schemas.get(assunt)
    if not schema:
        logger.error(f"Assunto desconhecido: {assunt}")
        return None
    
    parsed_schema = parse_schema(schema)
    
    # Cria o arquivo avro
    avro_filename = f'{file_name}.avro'
    try:
        with open(avro_filename, 'wb') as file:
            writer(file, parsed_schema, listData)
        logger.info(f"Arquivo Avro criado: {avro_filename}")
        return avro_filename
    except Exception as e:
        logger.error(f"Erro ao criar arquivo Avro {file_name}: {e}")
        return None


def upload_bucket(bucket_name: str, file_name: str, destination_name: str, client: storage.Client) -> bool:
    """Upload de arquivo para o bucket"""
    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_name)
        blob.upload_from_filename(file_name)
        logger.info(f'Arquivo: {file_name} enviado para {destination_name}')
        return True
    except Exception as e:
        logger.error(f'Erro ao enviar arquivo {file_name}: {e}')
        return False


def filterData(line: str) -> str:
    """Função auxiliar para tratamento de caracteres especiais"""
    padrao = r'[^a-zA-Z0-9áéíóúÁÉÍÓÚâêîôûÂÊÎÔÛàèìòùÀÈÌÒÙãõÃÕçÇ/\s().:]'
    return re.sub(padrao, '', line)


def deleteArchive(file: str):
    """Remove arquivo local"""
    try:
        os.remove(file)
        logger.info(f"Arquivo removido: {file}")
    except Exception as e:
        logger.error(f"Erro ao remover arquivo {file}: {e}")


def main():
    """Função principal"""
    logger.info("=" * 50)
    logger.info("INICIANDO IMPORTAÇÃO DE ARQUIVOS")
    logger.info("=" * 50)
    
    try:
        # Configurações
        bucket_destiny = 'assertiv_analytics'
        bucket_name = 'assertiv-amil'
        
        # Parâmetros de filtro
        mes_desejado = '12'
        mes = 'Dezembro'
        ano_desejado = '2025'
        cliente_filtro = 'ASSERTIV'
        
        # Autenticar
        client = authenticate_cloud()
        
        # Listar arquivos
        logger.info(f"Listando blobs do bucket: {bucket_name}")
        listArchives = list_blobs(bucket_name, client)
        
        # Filtrar por cliente
        listArchives = [item for item in listArchives if item['Cliente'] == cliente_filtro]
        logger.info(f"Arquivos filtrados para {cliente_filtro}: {len(listArchives)}")
        
        # Processar cada arquivo
        for listArchive in listArchives:
            nameArchive = f"{listArchive['Ano']}{listArchive['Mes']}_{listArchive['Arquivo'].split('.')[0]}"
            
            # Processar RG
            if listArchive['Operadora'] == 'amil' and listArchive['Assunto'] == 'RG':
                try:
                    caminho = f"gs://{bucket_name}/{listArchive['Caminho']}"
                    cliente = listArchive['Cliente'].split(' ')[0]
                    
                    dow = downloadExcel(listArchive['Arquivo'], caminho, cliente, mes, ano_desejado)
                    
                    if dow:
                        avroFile = schemaAvro(dow, nameArchive, listArchive['Assunto'])
                        if avroFile:
                            destination_name = f"Amil/RG/raw/{avroFile}"
                            if upload_bucket(bucket_destiny, avroFile, destination_name, client):
                                deleteArchive(avroFile)
                except Exception as e:
                    logger.error(f"Erro ao processar RG {nameArchive}: {e}")
            
            # Processar beneficiarios
            elif listArchive['Operadora'] == 'amil' and listArchive['Assunto'] == 'beneficiarios':
                try:
                    data = download_blob_into_memory(
                        bucket_name, 
                        listArchive['Caminho'], 
                        listArchive['Tipo'], 
                        listArchive['Assunto'], 
                        nameArchive, 
                        2, 
                        listArchive['Cliente'].split(' ')[0]
                    )
                    
                    if data:
                        avroFile = schemaAvro(data, nameArchive, listArchive['Assunto'])
                        if avroFile:
                            destination_name = f"Amil/beneficiarios/raw/{avroFile}"
                            if upload_bucket(bucket_destiny, avroFile, destination_name, client):
                                deleteArchive(avroFile)
                except Exception as e:
                    logger.error(f"Erro ao processar Beneficiarios {nameArchive}: {e}")
            
            # Processar eventos
            elif listArchive['Operadora'] == 'amil' and listArchive['Assunto'] == 'eventos':
                try:
                    data = download_blob_into_memory(
                        bucket_name, 
                        listArchive['Caminho'], 
                        listArchive['Tipo'], 
                        listArchive['Assunto'], 
                        nameArchive, 
                        1, 
                        listArchive['Cliente'].split(' ')[0]
                    )
                    
                    if data:
                        avroFile = schemaAvro(data, nameArchive, listArchive['Assunto'])
                        if avroFile:
                            destination_name = f"Amil/eventos/raw/{avroFile}"
                            if upload_bucket(bucket_destiny, avroFile, destination_name, client):
                                deleteArchive(avroFile)
                except Exception as e:
                    logger.error(f"Erro ao processar Eventos {nameArchive}: {e}")
        
        logger.info("=" * 50)
        logger.info("IMPORTAÇÃO DE ARQUIVOS FINALIZADA COM SUCESSO!")
        logger.info("=" * 50)
        
        return 0
        
    except Exception as e:
        logger.error(f"Erro fatal na importação: {e}")
        raise


if __name__ == '__main__':
    main()
