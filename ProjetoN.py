#pip install pandas
#pip install oauth2client
#pip install google-cloud-bigquery
#pip install -- update google-cloud-bigquery
#pip install pytest-shutil
#pip install DateTime
#pip install glob2
#pip install pyinstaller

import pandas as pd
from google.oauth2 import service_account
import shutil
import os
from datetime import date,datetime
import pathlib

# ---- Conecão com big query ----
credencial = service_account.Credentials.from_service_account_file(r"C:/Projeto_Neoway/Projeto/GBQ.json",scopes=['https://www.googleapis.com/auth/bigquery'])

# ---- Variaveis ----
dataAtual = date.today()
dataHoraAtual = datetime.today()
origem = ""
arquivo = ""
arquivoJson = ""

# ---- Criando pasta de data ----
origem = 'C:/Projeto_Neoway/Arquivos/Pendente/'
destinoPadrao = f"C:/Projeto_Neoway/Arquivos/Historico/{dataAtual}/"

if not os.path.exists(destinoPadrao):
    os.mkdir(destinoPadrao)

# ---- Função salvar log ----
def salvalog(linha):
    file = open(destinoPadrao + 'arquivo_log.txt', 'a')
    file.write( str(dataHoraAtual) + ' ........ ' + linha + '\n')
    file.close()

# ---- Função para arquivos CSV ----
def ler_arquivo_csv (nomearquivo,separador,tabeladestino):
    try: 
        arquivo = pd.read_csv(origem+nomearquivo, sep = separador,dtype=str)
        salvalog('Arquivo lido: ' + origem+nomearquivo )
        arquivo.to_gbq(destination_table = 'poised-lens-425012-f5.ProjetoN.' + tabeladestino,project_id='poised-lens-425012-f5',if_exists='replace',credentials=credencial)
        salvalog('Dados salvos na tabela: ' + tabeladestino )
        shutil.move(origem+nomearquivo, destinoPadrao+nomearquivo) 
        salvalog('Arquivo enviado para a pasta ' + destinoPadrao)
    except FileNotFoundError:
        salvalog('Arquivo nao encontrado: ' + origem+nomearquivo)
    except IOError:
        salvalog('Erro desconhecido, por favor, contatar administrador.')

# ---- Função para arquivos JSON ----
def ler_arquivo_json (tabeladestino):
    try: 
        caminhoJson = pathlib.Path(origem)
        lista = list(caminhoJson.glob('*.json'))

        if len(lista) == 0:
           salvalog('Nao ha arquivos JSON para processar: ' + origem)  

        for i in range(len(lista)):
            arquivoJson = pd.read_json(lista[i],dtype=str, lines=True)
            arquivoLido = str(lista[i])
            nomeArquivoJson = arquivoLido[len(origem):len(arquivoLido)]
            print(nomeArquivoJson)
            salvalog('Arquivo lido: ' + arquivoLido )
            if i == 0:
                existeTab = 'replace'
            else:
                existeTab = 'append'
            arquivoJson.to_gbq(destination_table = 'poised-lens-425012-f5.ProjetoN.' + tabeladestino,project_id='poised-lens-425012-f5',if_exists=existeTab,credentials=credencial)
            salvalog('Dados salvos na tabela: ' + tabeladestino )
            shutil.move(arquivoLido, destinoPadrao+nomeArquivoJson) 
            salvalog('Arquivo enviado para a pasta ' + destinoPadrao)
    except IOError:
        salvalog('Erro desconhecido, por favor, contatar administrador.')

salvalog('Processo iniciado!')

# ---- Lendo arquivos CSV ----
ler_arquivo_csv('df_empresas.csv',',','EMP_DF')
ler_arquivo_csv('empresas_simples.csv',';','EMP_SIMPLES')
ler_arquivo_csv('empresas_nivel_atividade.csv',';','EMP_NIV_ATIVIDADE') 
ler_arquivo_csv('empresas_saude_tributaria.csv',';','EMP_SAU_TRIBUTARIA') 
ler_arquivo_csv('empresas_porte.csv',';','EMP_PORTE') 

# ---- Lendo arquivos JSON ----
ler_arquivo_json('PROCESSOS')

# ---- FATO_PROCESSOS
query = '''
    SELECT 
        row_number() OVER (partition by null) as codProcesso
        ,CONCAT(REPEAT('0',14 - CAST(LENGTH(TRIM(CAST(cnpj AS STRING))) AS INT64)),TRIM(CAST(cnpj AS STRING))) AS cnpj 
        ,area
        ,grauProcesso
        ,SPLIT(comarca,'-') [safe_ordinal(1)] AS comarcaCidade
        ,SPLIT(comarca,'-') [safe_ordinal(2)] AS comarcaUF
        ,julgamento
        ,IF(dataDecisao = 'nan',NULL,SUBSTR(dataDecisao,0,10)) AS dataDecisao
        ,IF(dataEncerramento = 'nan',NULL,SUBSTR(dataEncerramento,0,10)) AS dataEncerramento
        ,uf
        ,tribunal
        ,ultimoEstado
        ,SPLIT(orgaoJulgador,'-') [safe_ordinal(1)] AS orgaoJulgador
        ,SPLIT(orgaoJulgador,'-') [safe_ordinal(2)] AS orgaoJulgadorCidade
        ,SPLIT(orgaoJulgador,'-') [safe_ordinal(3)] AS orgaoJulgadorUF
        ,citacaoTipo
        ,unidadeOrigem
        ,juiz
        ,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(assuntosCNJ,'[{',""),'}]',""),"}, {","|"), "', '",";"), "': '",":"),"'","") AS assuntosCNJ
        ,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(partes,'[{',""),'}]',""),"}, {","|"), "', '",";"), "': '",":"),"'","") AS partes  
        ,valorCausa
        ,valorPredicaoCondenacao
    from `ProjetoN.PROCESSOS`
    '''
resProcessos = pd.read_gbq(credentials=credencial,query=query)
salvalog('Tabela PROCESSOS lida')
resProcessos.to_gbq(destination_table = 'poised-lens-425012-f5.ProjetoN.FATO_PROCESSOS',project_id='poised-lens-425012-f5',if_exists='replace',credentials=credencial)
salvalog('Tabela FATO_PROCESSOS atualizada')


# ---- FATO_PROCESSOS
query2 = '''
    SELECT 
    CONCAT(REPEAT('0',14 - CAST(LENGTH(TRIM(CAST(DF.cnpj AS STRING))) AS INT64)),TRIM(CAST(DF.cnpj AS STRING))) AS cnpj 
    ,DF.dt_abertura
    ,DF.empresa_Matriz
    ,DF.cd_cnae_principal
    ,DF.de_cnae_principal
    ,DF.de_ramo_atividade
    ,DF.de_setor
    ,DF.endereco_cep
    ,DF.endereco_municipio
    ,DF.endereco_uf
    ,DF.endereco_regiao
    ,DF.endereco_mesorregiao
    ,DF.situacao_cadastral
    ,NA.nivel_atividade
    ,NP.empresa_porte
    ,ST.saude_tributaria
    ,SI.optante_simples
    ,SI.optante_simei
    FROM `ProjetoN.EMP_DF` DF
    LEFT JOIN `ProjetoN.EMP_NIV_ATIVIDADE` NA
    ON CONCAT(REPEAT('0',14 - CAST(LENGTH(TRIM(CAST(DF.cnpj AS STRING))) AS INT64)),TRIM(CAST(DF.cnpj AS STRING))) = 
    CONCAT(REPEAT('0',14 - CAST(LENGTH(TRIM(CAST(NA.cnpj AS STRING))) AS INT64)),TRIM(CAST(NA.cnpj AS STRING)))
    LEFT JOIN `ProjetoN.EMP_PORTE` NP
    ON CONCAT(REPEAT('0',14 - CAST(LENGTH(TRIM(CAST(DF.cnpj AS STRING))) AS INT64)),TRIM(CAST(DF.cnpj AS STRING))) = 
    CONCAT(REPEAT('0',14 - CAST(LENGTH(TRIM(CAST(NP.cnpj AS STRING))) AS INT64)),TRIM(CAST(NP.cnpj AS STRING)))
    LEFT JOIN `ProjetoN.EMP_SAU_TRIBUTARIA` ST
    ON CONCAT(REPEAT('0',14 - CAST(LENGTH(TRIM(CAST(DF.cnpj AS STRING))) AS INT64)),TRIM(CAST(DF.cnpj AS STRING))) = 
    CONCAT(REPEAT('0',14 - CAST(LENGTH(TRIM(CAST(ST.cnpj AS STRING))) AS INT64)),TRIM(CAST(ST.cnpj AS STRING)))
    LEFT JOIN `ProjetoN.EMP_SIMPLES` SI
    ON CONCAT(REPEAT('0',14 - CAST(LENGTH(TRIM(CAST(DF.cnpj AS STRING))) AS INT64)),TRIM(CAST(DF.cnpj AS STRING))) = 
    CONCAT(REPEAT('0',14 - CAST(LENGTH(TRIM(CAST(SI.cnpj AS STRING))) AS INT64)),TRIM(CAST(SI.cnpj AS STRING)))
    '''
resEmpresas = pd.read_gbq(credentials=credencial,query=query2)
salvalog('Tabelas de EMPRESAS lidas')
resEmpresas.to_gbq(destination_table = 'poised-lens-425012-f5.ProjetoN.DIM_EMPRESAS',project_id='poised-lens-425012-f5',if_exists='replace',credentials=credencial)
salvalog('Tabela DIM_EMPRESAS atualizada')

salvalog('Processo finalizado!\n')
print('Processo finalizado!')