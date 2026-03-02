# -*- coding: utf-8 -*-
"""
Created on Mon Aug 26 09:41:51 2024

@author: kelvin.umbelino
"""

from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import os

# Caminho para o seu arquivo de chave JSON
key_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'key', 'assertiv-edcf4791a41c.json')
# key_path = r"C:\Users\gabriel.mendes\Assertiv Corretora e Administradora de Seguro\AssertivDesenvolvimento - Documentos\Scripts\assertiv-edcf4791a41c.json"

# Crie credenciais a partir do arquivo de chave JSON
credentials = service_account.Credentials.from_service_account_file(key_path)

# Crie uma instância do cliente BigQuery com as credenciais fornecidas
client = bigquery.Client(credentials=credentials, project=credentials.project_id)
  
competencia_de = '2018-01-01'
competencia_ate = '2025-12-01'


# Armazena a consulta de inserção em uma variável
insert_query = f"""
   update `assertiv.business_analytics_gold.gold_utilizacao` 
   set CLASSIFICACAO_ASSERTIV = NULL
   where CNPJ_OPERADORA = '29309127000179'
  -- --  and data_competencia between '{competencia_de}' and '{competencia_ate}';

   """

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Marcando registro como NULL em: {num_rows} registros.")



# Armazena a consulta de inserção em uma variável
insert_query = f"""
   update `assertiv.business_analytics_gold.gold_utilizacao` 
   set CLASSIFICACAO_ASSERTIV = 'PEONA'
   where CNPJ_PRESTADOR = '00000000000000' and COD_PROCEDIMENTO = ''
   --Filtro por grupo, operadora e competência
   and CNPJ_OPERADORA = '29309127000179'
  --  and data_competencia between '{competencia_de}' and '{competencia_ate}';
   """

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Update feito em: {num_rows} registros.")
# Armazena a consulta de inserção em uma variável
insert_query = f"""
   update `assertiv.business_analytics_gold.gold_utilizacao` 
   set CLASSIFICACAO_ASSERTIV = 'PEONA'
   where NOME_PLANO is null
   --Filtro por grupo, operadora e competência
   and CNPJ_OPERADORA = '29309127000179'
  --  and data_competencia between '{competencia_de}' and '{competencia_ate}';
   """

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Update feito em: {num_rows} registros.")
# NOME_PLANO

# Armazena a consulta de inserção em uma variável
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
--Filtro por grupo, operadora e competência
--u.grupo = 'ATIVY'
u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
and (LEFT(u.COD_DOCUMENTO, 2) = 'CO' OR LEFT(u.COD_AUTORIZACAO, 2) = 'CO') and (upper(proc.NOME_PROCEDIMENTO) like '%PRONTO%' OR upper(proc.NOME_PROCEDIMENTO) like '%PS%')) a
where 
--u.grupo = 'ATIVY'
u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
and a.COD_AUTORIZACAO = u.COD_AUTORIZACAO;
   """

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Update feito em: {num_rows} registros.")



# Armazena a consulta de inserção em uma variável
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
--Filtro por grupo, operadora e competência
--u.grupo = 'ATIVY'
 u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
and (LEFT(u.COD_DOCUMENTO, 2) = 'CO' OR LEFT(u.COD_AUTORIZACAO, 2) = 'CO') and (upper(proc.NOME_PROCEDIMENTO) like '%PRONTO%' OR upper(proc.NOME_PROCEDIMENTO) like '%PS%')
and u.CLASSIFICACAO_ASSERTIV is null) a
where 
--Filtro por grupo, operadora e competência
--u.grupo = 'ATIVY'
u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
and a.COD_AUTORIZACAO = u.COD_AUTORIZACAO;


   """

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Update feito em: {num_rows} registros.")






# Armazena a consulta de inserção em uma variável
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
--Filtro por grupo, operadora e competência
--u.grupo = 'ATIVY'
u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
and LEFT(u.COD_DOCUMENTO, 2) = 'EX'
and u.CLASSIFICACAO_ASSERTIV is null) a
where 
--Filtro por grupo, operadora e competência
--u.grupo = 'ATIVY'
u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
and a.COD_AUTORIZACAO = u.COD_AUTORIZACAO;


   """

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Update feito em: {num_rows} registros.")




# Armazena a consulta de inserção em uma variável
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
--Filtro por grupo, operadora e competência
--u.grupo = 'ATIVY'
u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
and LEFT(u.COD_DOCUMENTO, 2) = 'TR'
and u.CLASSIFICACAO_ASSERTIV is null) a
where
--Filtro por grupo, operadora e competência
--u.grupo = 'ATIVY'
u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
and a.COD_AUTORIZACAO = u.COD_AUTORIZACAO;
"""

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Update feito em: {num_rows} registros.")






# Armazena a consulta de inserção em uma variável
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

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Update feito em: {num_rows} registros.")



# Armazena a consulta de inserção em uma variável
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
--Filtro por grupo, operadora e competência
--u.grupo = 'ATIVY'
u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
and u.CLASSIFICACAO_ASSERTIV is null) a
where 
--Filtro por grupo, operadora e competência
--u.grupo = 'ATIVY'
u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
and a.COD_AUTORIZACAO = u.COD_AUTORIZACAO;
"""

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Update feito em: {num_rows} registros.")




# Armazena a consulta de inserção em uma variável
insert_query = f"""
select u.CHAVE_BENEFICIARIO, u.cnpj_prestador, prest.NOME_PRESTADOR, u.GRUPO_ESTATISTICO, u.categoria_operadora, u.cod_procedimento, proc.NOME_PROCEDIMENTO, 
u.CLASSIFICACAO_OPERADORA, u.NOME_ESPECIALIDADE_PRESTADOR,
u.valor_pago, u.DATA_ATENDIMENTO, u.is_internado, u.QDE_EVENTOS_PAGO, u.REDE_REEMBOLSO, u.COD_DOCUMENTO, u.no_lote, u.guia_tiss, u.CLASSIFICACAO_ASSERTIV,
u.EVENTO_ASSERTIV
 from `assertiv.business_analytics_gold.gold_utilizacao` u
 left join `assertiv.business_analytics_gold.gold_operadora_procedimentos` proc on (proc.COD_PROCEDIMENTO = u.COD_PROCEDIMENTO and proc.CNPJ_operadora = u.CNPJ_OPERADORA)
 left join `assertiv.business_analytics_gold.gold_operadora_prestadores` prest on (prest.CNPJ_PRESTADOR = u.CNPJ_PRESTADOR and prest.CNPJ_operadora = u.CNPJ_OPERADORA)
where 
--Filtro por grupo, operadora e competência
--u.grupo = 'ATIVY'
u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
order by u.CHAVE_BENEFICIARIO, u.DATA_ATENDIMENTO;

"""

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Update feito em: {num_rows} registros.")




# Armazena a consulta de inserção em uma variável
insert_query = f"""
--Se todos estes estiverem em internação, e só tiver UM destes no código, migrar
update `assertiv.business_analytics_gold.gold_utilizacao` u set CLASSIFICACAO_ASSERTIV = 'CONSULTA'
where u.CNPJ_OPERADORA = '29309127000179' and u.COD_PROCEDIMENTO in ('10101012', '00010014', '90108733', '90119244', '90131700', 
'90125311', '90016513', '90114599', '90109899', '90126000', '90122400', '90124755', '90003438', '90123288', '90120666', '90125055', 
'91000184', '90108544', '91000262', '90120600', '90115222', '91000225', '91000185', '91000499', '90003446', '10106146', '20101074', 
'10106014', '60101229', '60101601', '90104366') and
--Filtro por grupo, operadora e competência
--and u.grupo = 'ATIVY'
u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
and u.CLASSIFICACAO_ASSERTIV <> 'CONSULTA';

"""

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Update feito em: {num_rows} registros.")




# Armazena a consulta de inserção em uma variável
insert_query = f"""

update `assertiv.business_analytics_gold.gold_utilizacao` u set CLASSIFICACAO_ASSERTIV = 'PRONTO SOCORRO'
where u.CNPJ_OPERADORA = '29309127000179' and u.COD_PROCEDIMENTO in ('90103588', '90103599', '90103444', '10101039', 
'91000229', '90103422', '91000227', '90017641', '91000736', '90017668', '90103611', '90014642', '90116644', '90103477', 
'90103455', '80123066', '91000231', '90119299', '80123074', '90119333', '90103433','90114588',	'12345678',	'80044042',	'40301397')
--Filtro por grupo, operadora e competência
--and u.grupo = 'ATIVY'
and u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
and u.CLASSIFICACAO_ASSERTIV <> 'PRONTO SOCORRO';



"""

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Update feito em: {num_rows} registros.")




# Armazena a consulta de inserção em uma variável
insert_query = f"""

update `assertiv.business_analytics_gold.gold_utilizacao` u set CLASSIFICACAO_ASSERTIV = 'TERAPIA'
where u.CNPJ_OPERADORA = '29309127000179' and u.COD_PROCEDIMENTO in 
('50000160', '50000144', '90124155', '90123355', '20101090', '90104366', '50000462', '50001183', '90131733',
'91000201', '50000560','50000470',	'50001221',	'50000080',	'50000616',	'20103220',	'80122264',	
'50000470',	'50000462',	'20104430',	'20104294',	'20104308',	'90104366',	'91000499',	'60101083',	'90131722',	
'90132199',	'90132188',	'41401026',	'20101090',	'41301048',	'20101104',	'60101229')
--Filtro por grupo, operadora e competência
--and u.grupo = 'ATIVY'
and u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
and u.CLASSIFICACAO_ASSERTIV <> 'TERAPIA';




"""

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Update feito em: {num_rows} registros.")


# Armazena a consulta de inserção em uma variável
insert_query = f"""
update `assertiv.business_analytics_gold.gold_utilizacao` u set CLASSIFICACAO_ASSERTIV = 'EXAME'
where u.CNPJ_OPERADORA = '29309127000179' and LEFT(u.COD_PROCEDIMENTO, 2) in ('40', '41')
--Filtro por grupo, operadora e competência
--and u.grupo = 'ATIVY'
and u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
and u.CLASSIFICACAO_ASSERTIV <> 'EXAME';

"""

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Update feito em: {num_rows} registros.")


# Armazena a consulta de inserção em uma variável
insert_query = f"""
update `assertiv.business_analytics_gold.gold_utilizacao` u set CLASSIFICACAO_ASSERTIV = 'EXAME'
where u.CNPJ_OPERADORA = '29309127000179' and u.COD_PROCEDIMENTO in ('62501038', '62501020', '62501054', 
'62501046', '40103099', '40103072', '40103102', '40103064', '40103110', '40103528', '40302075', '41401409', 
'41401360', '41401387', '40302733', '40309312', '41401379', '41401441', '41401395', '41401450', '40101045', 
'40101037', '41301048', '40103137', '41401182', '20101104', '40103889', '40101061', '41501012', '41401425', 
'41401174', '41401026', '41401204', '41401433', '41301307', '41301170', '41401646', '41301072', '41301200',
'40601137', '41501128', '41301420', '41301080', '40103439','90131933',	'41301323',	'41301200',	'41301323',	
'41301323',	'41301323',	'41301323',	'41301323',	'41301250',	'41401026',	'20101104',	'20101104',	'41401026',	
'41401026',	'20101104',	'41401026',	'20101104',	'41301250',	'41301323',	'41301323',	'41301250',	'41301323',	
'41301323',	'41301323',	'41301323')
--Filtro por grupo, operadora e competência
--and u.grupo = 'ATIVY'
and u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
and u.CLASSIFICACAO_ASSERTIV <> 'EXAME'
and u.CLASSIFICACAO_ASSERTIV <> 'INTERNACAO'
and u.CLASSIFICACAO_ASSERTIV <> 'PRONTO SOCORRO';


"""

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Update feito em: {num_rows} registros.")






# Armazena a consulta de inserção em uma variável
insert_query = f"""
update `assertiv.business_analytics_gold.gold_utilizacao` u set CLASSIFICACAO_ASSERTIV = 'OUTROS'
where u.CNPJ_OPERADORA = '29309127000179' and u.COD_PROCEDIMENTO in ('30711010', '20104065', '30101921', 
'30101468', '62504452', '62504339', '62504142', '62504290', '62504371', '62504401', '62504355', '62504541', 
'62504428', '62504410', '60020857', '60021098', '62504177', '62504460', '62504258', '62504436', '30101107', 
'30101450', '60018917', '90116266', '60018992', '60019050', '30101948', '30101204', '60034807', '60034777', 
'31303196', '60019026', '60020946', '90112044', '91000857',	'91000857',	'31205070',	'-1')
--Filtro por grupo, operadora e competência
--and u.grupo = 'ATIVY'
and u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
and u.CLASSIFICACAO_ASSERTIV <> 'OUTROS';

"""

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Update feito em: {num_rows} registros.")



# Armazena a consulta de inserção em uma variável
insert_query = f"""
update `assertiv.business_analytics_gold.gold_utilizacao` u set CLASSIFICACAO_ASSERTIV = 'OUTROS'
where u.CNPJ_OPERADORA = '29309127000179' and LEFT(u.COD_PROCEDIMENTO, 2) in ('62')
--Filtro por grupo, operadora e competência
--and u.grupo = 'ATIVY'
and u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
and u.CLASSIFICACAO_ASSERTIV <> 'OUTROS';



"""

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Update feito em: {num_rows} registros.")



# Armazena a consulta de inserção em uma variável
insert_query = f"""
update `assertiv.business_analytics_gold.gold_utilizacao` u set CLASSIFICACAO_ASSERTIV = 'INTERNACAO'
where u.CNPJ_OPERADORA = '29309127000179' and u.COD_PROCEDIMENTO in ('31002390', '70903140', '60000805', 
'30721237', '30720095', '22222222', '31303250', '60000511', '60000775', '90009240', '60000929', '60000651', 
'30602050', '31309097', '30733057', '60000783', '31309054', '10103023', '90103755', '10103015', '60000686', 
'60001054', '10104020', '10104011', '30911079', '20103450', '60001038', '30502080', '31305032', '30715369', 
'31003583', '60000171', '60000023', '90103766', '30208084', '90016726', '30728142', '50000365', '50000829', 
'31009166', '31009050', '31009336', '30403154', '60000554', '31009344', '60000619', '31403204', '30728070', 
'30728126', '74306260', '31303234', '31009174', '30206120', '31403220', '76420655', '30733065', '30733090', 
'31009263', '30602173', '90103822', '30914159', '31307248', '30914060', '31303226', '90131666', '30904080', 
'30906164', '30915023', '31303285', '30726158', '30602076', '40814092', '30209021', '30208033', '30713153', 
'70903646', '30729190', '75334038', '75334020', '30726182', '30502314', '31401155', '31401260', '31401309', 
'30726212', '30726166', '31303153', '30208017', '31009352', '31206182','80044425',	'80044131',	'30303060',	
'80044484',	'90107577','90106155')
--Filtro por grupo, operadora e competência
--and u.grupo = 'ATIVY'
and u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
and u.CLASSIFICACAO_ASSERTIV <> 'INTERNACAO';


"""

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Update feito em: {num_rows} registros.")





# Armazena a consulta de inserção em uma variável
insert_query = f"""
update `assertiv.business_analytics_gold.gold_utilizacao` u set CLASSIFICACAO_ASSERTIV = 'INTERNACAO'
where u.COD_DOCUMENTO in
(select distinct COD_DOCUMENTO
from `assertiv.business_analytics_gold.gold_utilizacao` u
where COD_DOCUMENTO in
(select distinct COD_DOCUMENTO from
(select *, row_number() over (partition by a.COD_DOCUMENTO) as rn from 
(select distinct u.COD_DOCUMENTO, u.CLASSIFICACAO_ASSERTIV
 from `assertiv.business_analytics_gold.gold_utilizacao` u
 where 
 --Filtro por grupo, operadora e competência
 --u.grupo = 'ATIVY'
u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}' 
 and u.CLASSIFICACAO_ASSERTIV not in ('CONSULTA', 'PEONA', 'OUTROS')) a) b
 where b.rn > 1)
and u.CLASSIFICACAO_ASSERTIV = 'INTERNACAO')
--Filtro por grupo, operadora e competência
--and u.grupo = 'ATIVY'
and u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}';

"""

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Update feito em: {num_rows} registros.")



# Armazena a consulta de inserção em uma variável
insert_query = f"""
update `assertiv.business_analytics_gold.gold_utilizacao` u set CLASSIFICACAO_ASSERTIV = 'PRONTO SOCORRO'
where u.COD_DOCUMENTO in
(
 
 select distinct COD_DOCUMENTO
from `assertiv.business_analytics_gold.gold_utilizacao` u
where COD_DOCUMENTO in
(select distinct COD_DOCUMENTO from
(select *, row_number() over (partition by a.COD_DOCUMENTO) as rn from 
(select distinct u.COD_DOCUMENTO, u.CLASSIFICACAO_ASSERTIV
 from `assertiv.business_analytics_gold.gold_utilizacao` u
 where 
 --Filtro por grupo, operadora e competência
--u.grupo = 'ATIVY'
u.CNPJ_OPERADORA = '29309127000179'
 --and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
and u.CLASSIFICACAO_ASSERTIV not in ('CONSULTA', 'PEONA', 'OUTROS')) a) b
 where b.rn > 1)
and u.CLASSIFICACAO_ASSERTIV = 'PRONTO SOCORRO')


--Filtro por grupo, operadora e competência
--and u.grupo = 'ATIVY'
and u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}';
"""

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Update feito em: {num_rows} registros.")





# Armazena a consulta de inserção em uma variável
insert_query = f"""
update `assertiv.business_analytics_gold.gold_utilizacao` u set CLASSIFICACAO_ASSERTIV = 'CONSULTA'
where u.COD_DOCUMENTO in
(select distinct COD_DOCUMENTO
from `assertiv.business_analytics_gold.gold_utilizacao` u
where COD_DOCUMENTO in
(select distinct COD_DOCUMENTO from
(select *, row_number() over (partition by a.COD_DOCUMENTO) as rn from 
(select distinct u.COD_DOCUMENTO, u.CLASSIFICACAO_ASSERTIV
 from `assertiv.business_analytics_gold.gold_utilizacao` u
 where 
 --Filtro por grupo, operadora e competência
 --u.grupo = 'ATIVY'
u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
 and u.CLASSIFICACAO_ASSERTIV not in ('PEONA', 'OUTROS')) a) b
 where b.rn > 1)
and u.CLASSIFICACAO_ASSERTIV = 'CONSULTA')
--Filtro por grupo, operadora e competência
--and u.grupo = 'ATIVY'
and u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}';
"""

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Update feito em: {num_rows} registros.")






# Armazena a consulta de inserção em uma variável
insert_query = f"""
update `assertiv.business_analytics_gold.gold_utilizacao` u set CLASSIFICACAO_ASSERTIV = 'TERAPIA'
where u.COD_DOCUMENTO in
(select distinct COD_DOCUMENTO
from `assertiv.business_analytics_gold.gold_utilizacao` u
where COD_DOCUMENTO in
(select distinct COD_DOCUMENTO from
(select *, row_number() over (partition by a.COD_DOCUMENTO) as rn from 
(select distinct u.COD_DOCUMENTO, u.CLASSIFICACAO_ASSERTIV
 from `assertiv.business_analytics_gold.gold_utilizacao` u
 where 
 --Filtro por grupo, operadora e competência
 --u.grupo = 'ATIVY'
u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
 and u.CLASSIFICACAO_ASSERTIV not in ('CONSULTA', 'PEONA', 'OUTROS')) a) b
 where b.rn > 1)
and u.CLASSIFICACAO_ASSERTIV = 'TERAPIA')
--Filtro por grupo, operadora e competência
--and u.grupo = 'ATIVY'
and u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}';
"""

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Update feito em: {num_rows} registros.")



# Armazena a consulta de inserção em uma variável
insert_query = f"""
update `assertiv.business_analytics_gold.gold_utilizacao` u set CLASSIFICACAO_ASSERTIV = 'OUTROS'
where u.CNPJ_OPERADORA = '29309127000179' and LEFT(u.COD_AUTORIZACAO, 2) = 'CO'
and u.COD_PROCEDIMENTO in ('0000277387', '0000049050', '0000289448', '0000285693', '0000266647', '0000017904', 
'0000076047', '0000061005', '0000150972', '60015292', '0000091561', '0000025904', '0000240781', '0000120916', 
'0000160128', '0000770041', '0000770249', '0000253907', '0000061007', '0000015797', '0000059900')
--Filtro por grupo, operadora e competência
--and u.grupo = 'ATIVY'
and u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
and u.CLASSIFICACAO_ASSERTIV <> 'OUTROS';
"""

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Update feito em: {num_rows} registros.")





# Armazena a consulta de inserção em uma variável
insert_query = f"""
   update `assertiv.business_analytics_gold.gold_utilizacao` 
   set EVENTO_ASSERTIV = NULL
   where CNPJ_OPERADORA = '29309127000179'
  --  and data_competencia between '{competencia_de}' and '{competencia_ate}';

   """

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Marcando registro como NULL em: {num_rows} registros.")






# Armazena a consulta de inserção em uma variável
insert_query = f"""
--EVENTO CONSULTA - para contabilizar para frequência. Cada consulta é um evento independente
update `assertiv.business_analytics_gold.gold_utilizacao` u set EVENTO_ASSERTIV = GENERATE_UUID()
where u.classificacao_assertiv = 'CONSULTA' and u.CNPJ_OPERADORA = '29309127000179'
--Filtro por grupo, operadora e competência
--and u.grupo = 'ATIVY'
and u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
and u.evento_assertiv is null;

"""

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Update feito em: {num_rows} registros.")




# Armazena a consulta de inserção em uma variável
insert_query = f"""
--EVENTO EXAME - para contabilizar para frequência. Cada evento de exame é um evento independente, DESDE que esteja na tabela HM/SADT
--Assim excluímos taxas e alugueis da contabilização
update `assertiv.business_analytics_gold.gold_utilizacao` u set EVENTO_ASSERTIV = GENERATE_UUID()
from 
(select distinct CHAVE_BENEFICIARIO, DATA_ATENDIMENTO, COD_PROCEDIMENTO from `assertiv.business_analytics_gold.gold_utilizacao` u
left join `assertiv.business_analytics_raw.raw_assertiv_hmsadt` hm on hm.codigoTuss = u.COD_PROCEDIMENTO
where u.classificacao_assertiv = 'EXAME' 
--Filtro por grupo, operadora e competência
--and u.grupo = 'ATIVY'
and u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
and hm.codigoTuss is not null) a
where u.classificacao_assertiv = 'EXAME'
and a.COD_PROCEDIMENTO = u.COD_PROCEDIMENTO
and a.CHAVE_BENEFICIARIO = u.CHAVE_BENEFICIARIO
and a.DATA_ATENDIMENTO = u.DATA_ATENDIMENTO 
--Filtro por grupo, operadora e competência
--and u.grupo = 'ATIVY'
and u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
and u.evento_assertiv is null;


"""

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Update feito em: {num_rows} registros.")







# Armazena a consulta de inserção em uma variável
insert_query = f"""
--EVENTO TERAPIA - para contabilizar para frequência. Cada consulta é um evento independente
update `assertiv.business_analytics_gold.gold_utilizacao` u set EVENTO_ASSERTIV = GENERATE_UUID()
where u.classificacao_assertiv = 'TERAPIA' 
--Filtro por grupo, operadora e competência
--and u.grupo = 'ATIVY'
and u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
and u.evento_assertiv is null;


"""

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Update feito em: {num_rows} registros.")




# Armazena a consulta de inserção em uma variável
insert_query = f"""
--EVENTO INTERNACAO
update `assertiv.business_analytics_gold.gold_utilizacao` u set EVENTO_ASSERTIV = a.uuid
from
(select CHAVE_BENEFICIARIO, COD_DOCUMENTO, GENERATE_UUID() as uuid from
(select distinct u.CHAVE_BENEFICIARIO, u.COD_DOCUMENTO 
from `assertiv.business_analytics_gold.gold_utilizacao` u
where u.CLASSIFICACAO_ASSERTIV = 'INTERNACAO' 
and u.COD_DOCUMENTO is not null
--Filtro por grupo, operadora e competência
--and u.grupo = 'ATIVY'
and u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
) b) a
where u.classificacao_assertiv = 'INTERNACAO'
and a.CHAVE_BENEFICIARIO = u.CHAVE_BENEFICIARIO
and a.COD_DOCUMENTO = u.COD_DOCUMENTO
--Filtro por grupo, operadora e competência
--and u.grupo = 'ATIVY'
and u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
and u.evento_assertiv is null;



"""

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Update feito em: {num_rows} registros.")



# Armazena a consulta de inserção em uma variável
insert_query = f"""
--EVENTO PRONTO SOCORRO - para contabilizar para frequência. Cada dia que existe um evento de pronto socorro é contabilizado como um evento independente
update `assertiv.business_analytics_gold.gold_utilizacao` u set EVENTO_ASSERTIV = a.uuid
from
(select CHAVE_BENEFICIARIO, DATA_ATENDIMENTO, GENERATE_UUID() as uuid from
(select distinct u.CHAVE_BENEFICIARIO, u.DATA_ATENDIMENTO
 from `assertiv.business_analytics_gold.gold_utilizacao` u
where u.classificacao_assertiv = 'PRONTO SOCORRO'
--Filtro por grupo, operadora e competência
--and u.grupo = 'ATIVY'
and u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
) b) a
where u.classificacao_assertiv = 'PRONTO SOCORRO'
and a.CHAVE_BENEFICIARIO = u.CHAVE_BENEFICIARIO
and a.DATA_ATENDIMENTO = u.DATA_ATENDIMENTO 
--Filtro por grupo, operadora e competência
--and u.grupo = 'ATIVY'
and u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
and u.evento_assertiv is null;


"""

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Update feito em: {num_rows} registros.")





# Armazena a consulta de inserção em uma variável
insert_query = f"""
--EVENTO OUTROS - para contabilizar para frequência. Cada dia que existe um evento outros é contabilizado como um evento independente
update `assertiv.business_analytics_gold.gold_utilizacao` u set EVENTO_ASSERTIV = a.uuid
from
(select CHAVE_BENEFICIARIO, DATA_ATENDIMENTO, GENERATE_UUID() as uuid from
(select distinct u.CHAVE_BENEFICIARIO, u.DATA_ATENDIMENTO
 from `assertiv.business_analytics_gold.gold_utilizacao` u
where u.classificacao_assertiv = 'OUTROS'
--Filtro por grupo, operadora e competência
--and u.grupo = 'ATIVY'
and u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
) b) a
where u.classificacao_assertiv = 'OUTROS'
and a.CHAVE_BENEFICIARIO = u.CHAVE_BENEFICIARIO
and a.DATA_ATENDIMENTO = u.DATA_ATENDIMENTO 
--Filtro por grupo, operadora e competência
--and u.grupo = 'ATIVY'
and u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
and u.evento_assertiv is null;
"""

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Update feito em: {num_rows} registros.")































# Armazena a consulta de inserção em uma variável
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
--Filtro por grupo, operadora e competência
--u.grupo = 'ATIVY'
u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
and LEFT(u.COD_DOCUMENTO, 2) = 'CO' and upper(u.CLASSIFICACAO_ASSERTIV) like '%PRONTO%') a
where 
--u.grupo = 'ATIVY'
u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
and a.COD_AUTORIZACAO = u.COD_AUTORIZACAO;
   """

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Update feito em: {num_rows} registros.")



# Armazena a consulta de inserção em uma variável
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
--Filtro por grupo, operadora e competência
--u.grupo = 'ATIVY'
 u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
and LEFT(u.COD_DOCUMENTO, 2) = 'CO' and upper(u.CLASSIFICACAO_ASSERTIV) not like '%PRONTO SOCORRO%'
and u.CLASSIFICACAO_ASSERTIV is null) a
where 
--Filtro por grupo, operadora e competência
--u.grupo = 'ATIVY'
u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
and a.COD_AUTORIZACAO = u.COD_AUTORIZACAO;


   """

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Update feito em: {num_rows} registros.")



# Armazena a consulta de inserção em uma variável
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
--Filtro por grupo, operadora e competência
--u.grupo = 'ATIVY'
u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
and LEFT(u.COD_DOCUMENTO, 2) = 'TR'
and u.CLASSIFICACAO_ASSERTIV is null) a
where
--Filtro por grupo, operadora e competência
--u.grupo = 'ATIVY'
u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
and a.COD_AUTORIZACAO = u.COD_AUTORIZACAO;
"""

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Update feito em: {num_rows} registros.")






# # Armazena a consulta de inserção em uma variável
# insert_query = f"""
# update `assertiv.business_analytics_gold.gold_utilizacao` u set CLASSIFICACAO_ASSERTIV = 'INTERNACAO'
# from
# (select distinct u.COD_AUTORIZACAO
# from `assertiv.business_analytics_gold.gold_utilizacao` u
# left join `assertiv.business_analytics_gold.gold_operadora_prestadores` prest 
#   on (prest.CNPJ_PRESTADOR = u.CNPJ_PRESTADOR and prest.CNPJ_OPERADORA = u.CNPJ_OPERADORA)
# left join `assertiv.business_analytics_gold.gold_operadora_procedimentos` proc
#   on (proc.COD_PROCEDIMENTO = u.COD_PROCEDIMENTO and proc.CNPJ_OPERADORA = u.CNPJ_OPERADORA)
# where 
# --Filtro por grupo, operadora e competência
# --u.grupo = 'ATIVY'
# u.CNPJ_OPERADORA = '29309127000179'
# and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
# and SAFE_CAST(u.COD_DOCUMENTO as INTEGER) is not null
# and u.CLASSIFICACAO_ASSERTIV is null) a
# where 
# --Filtro por grupo, operadora e competência
# --.grupo = 'ATIVY'
# u.CNPJ_OPERADORA = '29309127000179'
# and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
# and a.COD_AUTORIZACAO = u.COD_AUTORIZACAO;
# """

# # Execute a consulta de inserção
# insert_job = client.query(insert_query)
# insert_job.result()  # Espera pela conclusão da consulta

# # Obtenha o número de linhas afetadas
# num_rows = insert_job.num_dml_affected_rows
# print(f"Update feito em: {num_rows} registros.")



# Armazena a consulta de inserção em uma variável
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
--Filtro por grupo, operadora e competência
--u.grupo = 'ATIVY'
u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
and u.CLASSIFICACAO_ASSERTIV is null) a
where 
--Filtro por grupo, operadora e competência
--u.grupo = 'ATIVY'
u.CNPJ_OPERADORA = '29309127000179'
--and u.data_competencia between '{competencia_de}' and '{competencia_ate}'
and a.COD_AUTORIZACAO = u.COD_AUTORIZACAO;
"""

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Update feito em: {num_rows} registros.")



# Armazena a consulta de inserção em uma variável
insert_query = f"""

   update `assertiv.business_analytics_gold.gold_utilizacao` 
   set CLASSIFICACAO_ASSERTIV = 'PEONA'
   where CNPJ_OPERADORA = '29309127000179'
   and (CLASSIFICACAO_ASSERTIV = '' OR CLASSIFICACAO_ASSERTIV = 'NULL' OR CLASSIFICACAO_ASSERTIV = 'null');
   --and data_competencia between '{competencia_de}' and '{competencia_ate}';

   """

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Marcando registro como NULL em: {num_rows} registros.")








# Armazena a consulta de inserção em uma variável
insert_query = f"""
   update `assertiv.business_analytics_gold.gold_utilizacao` 
   set CLASSIFICACAO_ASSERTIV = 'TERAPIA'
   where CNPJ_OPERADORA = '29309127000179'
   and CLASSIFICACAO_ASSERTIV = 'TERAPIA SIMPLES';
   --and data_competencia between '{competencia_de}' and '{competencia_ate}';
   
   update `assertiv.business_analytics_gold.gold_utilizacao` 
   set CLASSIFICACAO_ASSERTIV = 'INTERNACAO'
   where CNPJ_OPERADORA = '29309127000179'
   and CLASSIFICACAO_ASSERTIV = 'INTERNAÇÃO HOSPITALAR';
   --and data_competencia between '{competencia_de}' and '{competencia_ate}';

   
   """

# Execute a consulta de inserção
insert_job = client.query(insert_query)
insert_job.result()  # Espera pela conclusão da consulta

# Obtenha o número de linhas afetadas
num_rows = insert_job.num_dml_affected_rows
print(f"Marcando registro como NULL em: {num_rows} registros.")
