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


def authenticate_cloud() -> object:
    """Autentica usando credenciais da variável de ambiente GCP_KEY"""
    try:
        gcp_key = os.environ.get('GCP_KEY')
        if not gcp_key:
            raise ValueError("Variável de ambiente GCP_KEY não encontrada ou vazia")
        
        # Parse do JSON
        try:
            service_account_info = json.loads(gcp_key)
            logger.info("Credenciais carregadas com sucesso")
        except json.JSONDecodeError as e:
            logger.error(f"Erro ao fazer parse do JSON: {e}")
            raise
        
        # Cria credenciais
        credentials = service_account.Credentials.from_service_account_info(service_account_info)
        
        return credentials
    except Exception as e:
        logger.error(f"Erro na autenticação: {e}")
        raise


def instance_bucket() -> object:
    """cria instancia de objeto do client"""
    try:
        credentials = authenticate_cloud()
        client = storage.Client(credentials=credentials, project=credentials.project_id)
        logger.info(f"Conectado ao Storage - Projeto: {credentials.project_id}")
        return client
    except Exception as e:
        logger.error(f"Erro ao instanciar bucket: {e}")
        raise


def list_blobs(bucket_name: str, client: object) -> list:
    try:
        listArchive = []
        blobs = client.list_blobs(bucket_name)
        for blob in blobs:
            tupla = blob.name.split('/')
            if len(tupla) == 6:
                if tupla[5].split('.')[1] in ['txt', 'csv', 'xls', 'xlsx', 'pdf']:
                    listArchive.append(
                        List(tupla, blob, tupla[5].split('.')[1])
                    )
        logger.info(f"Total de arquivos encontrados: {len(listArchive)}")
        return listArchive
    except Exception as e:
        logger.error(f"Erro ao listar blobs: {e}")
        return []
    finally:
        return listArchive


def List(tupla: str, blob: str, typeArchive: str) -> dict:
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


def download_blob_into_memory(bucket_name: str, blob_name: str, type: str, assunt: str, archive: str, codificacao: int, grupo: str):
    try:
        client = instance_bucket()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        if codificacao == 1:
            Encoding = 'utf-8'
        else:
            Encoding = 'ISO-8859-1'

        with blob.open("r", encoding=Encoding) as files:
            fileArchive = []
            linha = 1
            for file in files:
                if assunt == 'mensalidades':
                    if type == 'txt':
                        if file[0:9] == 'Contrato:':
                            header = file.split(':')
                            contrato = header[1].split('-')[0].strip()
                            fatura = header[2].split('#')[0].strip()
                        elif file[0:8] == 'Empresa:':
                            header = file.split('-')
                            empresa = header[0].split(':')[
                                1].strip() + '-' + header[1]
                        else:
                            if (file[0:1].isdigit()):
                                dict = file.split('#')
                                fileArchive.append(
                                    {
                                        'Codigo': dict[0],
                                        'Beneficiario': dict[1],
                                        'Matricula': dict[2],
                                        'CPF': dict[3],
                                        'Plano': dict[4],
                                        'Tipo': dict[5],
                                        'Idade': dict[6],
                                        'Dependencia': dict[7],
                                        'DataLimite': dict[8],
                                        'DataInclusao': dict[9],
                                        'DataExclusao': dict[10],
                                        'Lotacao': dict[11],
                                        'Rubrica': dict[12],
                                        'CoParticipacao': dict[13],
                                        'Outros': dict[14],
                                        'Mensalidade': dict[15],
                                        'TotalFamilia': dict[16],
                                        'Contrato': contrato,
                                        'Fatura': fatura,
                                        'Empresa': empresa,
                                        'Origem': archive
                                    }
                                )
                elif assunt == 'eventos':
                    if type == 'txt':
                        dict = file.split('\t')
                        fileArchive.append({
                            'Operadora': dict[0],
                            'Contrato': dict[1],
                            'EmpresaNome': dict[2],
                            'Subfatura': dict[3],
                            'SubfaturaNome': dict[4],
                            'Uk1': dict[5],
                            'Uk2': dict[6],
                            'CodFamilia': dict[7],
                            'CarteirinhaTitular': dict[8],
                            'NomeTitular': dict[9],
                            'Cpf': dict[10],
                            'Uk3': dict[11],
                            'Carteirinha': dict[12],
                            'Nome': dict[13],
                            'DtNascimento': dict[14],
                            'Sexo': dict[15],
                            'Titular': dict[16],
                            'CodDependente': dict[17],
                            'GrauParentesco': dict[18],
                            'EstadoCivil': dict[19],
                            'PlanoCodigo': dict[20],
                            'PlanoNome': dict[21],
                            'Status': dict[22],
                            'Cidade': dict[23],
                            'Estado': dict[24],
                            'ProcedimentoCod': dict[25],
                            'ProcedimentoNome': dict[26],
                            'ProcedimentoTabela': dict[27],
                            'ProcedimentoC1': dict[28],
                            'ProcedimentoC2': dict[29],
                            'ProcedimentoC3': dict[30],
                            'ProcedimentoClassificacao': dict[31],
                            'ProcedimentoCodClass': dict[32],
                            'ProcedimentoEspecialidade': dict[33],
                            'Uk4': dict[34],
                            'Uk5': dict[35],
                            'GuiaCod': dict[36],
                            'Uk6': dict[37],
                            'RedeReembolso': dict[38],
                            'DtUtilizacao': dict[39],
                            'Uk7': dict[40],
                            'DtCompetencia': dict[41],
                            'CodAutorizacao': dict[42],
                            'PrestadorNome': dict[43],
                            'Cod1': dict[44],
                            'Valor1': dict[45],
                            'ValorSinistro': dict[46],
                            'Valor2': dict[47],
                            'Valor3': dict[48],
                            'Conselho': dict[49],
                            'ConselhoEstado': dict[50],
                            'PrestadorCodClass': dict[51],
                            'PrestadorClassificacao': dict[52],
                            'Cod5': dict[53],
                            'Uk8': dict[54],
                            'Cod6': dict[55],
                            'Uk9': dict[56],
                            'Cod7': dict[57],
                            'Grupo': grupo,
                            'Origem': archive
                        })
                elif assunt == 'beneficiarios':
                    if type == 'txt':
                        dict = file.split('\t')
                        fileArchive.append({
                            'Operadora': dict[0],
                            'Contrato': dict[1],
                            'Subfatura': dict[2],
                            'EmpresaNome': dict[3],
                            'Uk1': dict[4],
                            'Uk2': dict[5],
                            'CodFamilia': dict[6],
                            'Carteirinha': dict[7],
                            'Nome': dict[8],
                            'Cpf': dict[9],
                            'Uk3': dict[10],
                            'DtNascimento': dict[11],
                            'Sexo': dict[12],
                            'Titular': dict[13],
                            'CodDependente': dict[14],
                            'GrauParentesco': dict[15],
                            'EstadoCivil': dict[16],
                            'PlanoCodigo': dict[17],
                            'PlanoNome': dict[18],
                            'Uk4': dict[19],
                            'Cidade': dict[20],
                            'Estado': dict[21],
                            'DtCompetencia': dict[22],
                            'DtInicioVigencia': dict[23],
                            'DtCancelamento': dict[24],
                            'CarteirinhaTitular': dict[25],
                            'Grupo': grupo,
                            'Origem': archive
                        })
        return fileArchive
    except Exception as e:
        logger.error(f"Erro ao fazer download do blob {blob_name}: {e}")
        return []


def downloadExcel(archive: str, path: str, grupo: str, mes: str, ano_desejado: str):
    try:
        dt = mes + "/" + ano_desejado

        excel = pd.read_excel(
            path, sheet_name="rpt_AnaliseCustosReceitasSaudeC")

        df = pd.DataFrame(excel)
        
        # Verifica se "Cód. Analisados:" está na planilha
        contains_text = df.apply(lambda row: row.astype(str).str.contains("Cód. Analisados:", na=False).any(), axis=1).any()
        
        df = df.to_dict('tight')
        df = pd.DataFrame(df['data']).drop(columns=[0, 1, 2, 3, 5, 6, 7, 8])
        df = df[df.iloc[:, 2] == dt]
        
        # higieniza o dataframe
        df.fillna(0, inplace=True)
        
        df = trataDataFrame(df, archive, grupo)
        
        return df
    except Exception as e:
        logger.error(f"Erro ao processar Excel {archive}: {e}")
        return []


def trataDataFrame(dtFrame: str, archive: str, grupo: str):
    try:
        dicionary = {'cliente': grupo, 'meses': [dtFrame[10]], 'receita': [
            dtFrame[11]], 'custoTotal': [dtFrame[12]], 'sinistralidade': [dtFrame[13]],
            'beneficiarioAtendido': [dtFrame[14]], 'custoPerCapita': [dtFrame[15]],
            'vidas': [dtFrame[16]]}

        df = pd.DataFrame(dicionary)
        result = []
        
        def extrai_valor(v):
            if isinstance(v, pd.Series):
                return v.iloc[0]
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
                'mes': mes,
                'receita': trataNaN(receita),
                'custoTotal': trataNaN(custoTotal),
                'sinistralidade': sinistralidade,
                'beneficiarioAtendido': beneficiarioAtendido,
                'custoPerCapita': custoPerCapita,
                'vidas': trataNaN(vidas),
                'cliente': grupo,
                'grupo': grupo,
                'origem': archive
            })

        return result
    except Exception as e:
        logger.error(f"Erro ao tratar DataFrame: {e}")
        return []


def trataNaN(valor):
    try:
        valor = float(valor)
        if math.isnan(valor):
            return 'NaN'
        return valor
    except (ValueError, TypeError):
        return valor


def normalize_to_string(records):
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


def schemaAvro(listData: object, file_name: str, assunt: str) -> str:
    try:
        listData = normalize_to_string(listData)

        if assunt == 'mensalidades':
            schemaMensalidade = {
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
            }
            parsed_schema = parse_schema(schemaMensalidade)
            
        elif assunt == 'eventos':
            schemaEventos = {
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
            }
            parsed_schema = parse_schema(schemaEventos)
            
        elif assunt == 'beneficiarios':
            schemaBeneficiarios = {
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
            }
            parsed_schema = parse_schema(schemaBeneficiarios)
            
        elif assunt == 'RG':
            schemaSinistralidade = {
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
            parsed_schema = parse_schema(schemaSinistralidade)
        else:
            logger.error(f"Tipo de assunto não reconhecido: {assunt}")
            return ""

        # cria o arquivo avro
        with open(f'{file_name}.avro', 'wb') as file:
            try:
                writer(file, parsed_schema, listData)
                logger.info(f"Arquivo AVRO gerado: {file_name}.avro")
            except Exception as e:
                logger.error(f"Não gerou Arquivo: {file_name} - Erro: {e}")
            return file.name
    except Exception as e:
        logger.error(f"Erro no schemaAvro: {e}")
        return ""


def upload_bucket(bucket_name: str, file_name: str, destination_name: str) -> str:
    try:
        client = instance_bucket()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_name)
        blob.upload_from_filename(file_name)

        logger.info(f'Arquivo: {file_name} enviado para {destination_name}')
    except Exception as e:
        logger.error(f'Não enviou Arquivo: {file_name} - Erro:{e}')


def filterData(line):
    padrao = r'[^a-zA-Z0-9áéíóúÁÉÍÓÚâêîôûÂÊÎÔÛàèìòùÀÈÌÒÙãõÃÕçÇ/\s().:]'
    return re.sub(padrao, '', line)


def deleteArchive(file: str):
    try:
        os.remove(file)
        logger.info(f"Arquivo {file} deletado")
    except Exception as e:
        logger.error(f"Erro ao deletar {file}: {e}")


def main():
    try:
        logger.info("=" * 50)
        logger.info("INICIANDO PROCESSO DE IMPORTAÇÃO AMIL")
        logger.info("=" * 50)
        
        # nome do repositorio
        bucket_destiny = 'assertiv_analytics'
        bucket_name = 'assertiv-amil'
        
        # Instancia cliente
        client = instance_bucket()
        
        # Lista arquivos
        listArchives = list_blobs(bucket_name, client)
        
        # Defina o mês e o ano desejados
        mes_desejado = '12'
        mes = 'Dezembro'
        ano_desejado = '2025'
        Cliente = 'ASSERTIV'
        
        # Filtrar a lista
        listArchives = [item for item in listArchives if item['Cliente'] == Cliente]
        logger.info(f"Total de arquivos do cliente {Cliente}: {len(listArchives)}")

        for listArchive in listArchives:
            nameArchive = fr"{listArchive['Ano']}{listArchive['Mes']}_{listArchive['Arquivo'].split('.')[0]}"
            
            if (listArchive['Operadora'] == 'amil' and listArchive['Assunto'] == 'RG'):
                try:
                    caminho = fr"gs://{bucket_name}/{listArchive['Caminho']}"
                    cliente = listArchive['Cliente'].split(' ')[0]
                    arquivo = fr"{listArchive['Arquivo']}"
                    
                    dow = downloadExcel(arquivo, caminho, cliente, mes, ano_desejado)
                    
                    if dow:
                        avroFile = schemaAvro(dow, nameArchive, listArchive['Assunto'])
                        if avroFile:
                            destination_name = fr"Amil/RG/raw/{avroFile}"
                            upload_bucket(bucket_destiny, avroFile, destination_name)
                            deleteArchive(avroFile)
                except Exception as e:
                    logger.error(f'Não gerou Arquivo RG: {nameArchive} - Erro: {e}')

            elif (listArchive['Operadora'] == 'amil' and listArchive['Assunto'] == 'beneficiarios'):
                try:
                    dados = download_blob_into_memory(
                        bucket_name, listArchive['Caminho'], listArchive['Tipo'], 
                        listArchive['Assunto'], nameArchive, 2, 
                        listArchive['Cliente'].split(' ')[0]
                    )
                    
                    if dados:
                        avroFile = schemaAvro(dados, nameArchive, listArchive['Assunto'])
                        if avroFile:
                            destination_name = fr"Amil/beneficiarios/raw/{avroFile}"
                            upload_bucket(bucket_destiny, avroFile, destination_name)
                            deleteArchive(avroFile)
                except Exception as e:
                    logger.error(f'Não gerou Arquivo Beneficiarios: {nameArchive} - Erro: {e}')

            elif (listArchive['Operadora'] == 'amil' and listArchive['Assunto'] == 'eventos'):
                try:
                    dados = download_blob_into_memory(
                        bucket_name, listArchive['Caminho'], listArchive['Tipo'], 
                        listArchive['Assunto'], nameArchive, 1, 
                        listArchive['Cliente'].split(' ')[0]
                    )
                    
                    if dados:
                        avroFile = schemaAvro(dados, nameArchive, listArchive['Assunto'])
                        if avroFile:
                            destination_name = fr"Amil/eventos/raw/{avroFile}"
                            upload_bucket(bucket_destiny, avroFile, destination_name)
                            deleteArchive(avroFile)
                except Exception as e:
                    logger.error(f'Não gerou Arquivo Eventos: {nameArchive} - Erro: {e}')

        logger.info("=" * 50)
        logger.info("IMPORTAÇÃO FINALIZADA COM SUCESSO!")
        logger.info("=" * 50)
        
        return 0
        
    except Exception as e:
        logger.error(f"ERRO FATAL NO PROCESSO: {e}")
        return 1


if __name__ == '__main__':
    exit(main())
