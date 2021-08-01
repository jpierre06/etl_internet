#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import time
import json
import requests
import pandas as pd
from pandas.io.json import json_normalize
import numpy as np
import os


# In[ ]:


from datetime import datetime


# In[ ]:


from pandas.io import sql
from sqlalchemy import create_engine


# In[ ]:


path_notebook = os.getcwd()


# In[ ]:


#Inicio da execução do notebook
start_general = datetime.now()


# In[ ]:





# ## Definir funções

# In[ ]:


def cadastrar_fonte_dados (nome_site, site_fonte_dados, nome_fonte_dados):
    
    fonte_dados = [[nome_site, site_fonte_dados, nome_fonte_dados]]

    colunas = ['Nome_Site', 'Site_Fonte_Dados', 'Nome_Fonte_Dados']

    fonte_dados = pd.DataFrame(fonte_dados, columns = colunas)

    with engine.connect() as conn, conn.begin():
        fonte_dados.to_sql('stg_Fonte_Dados', conn, if_exists='replace', index=False)


    ## Executar procedure versionamento

    conexao = engine.connect()

    consulta_sql = "CALL Stage.Versiona_stg_Fonte_Dados_Internet();"

    with engine.begin() as conn:
        conn.execute(consulta_sql)


    ## Deletar dados tabela stage

    consulta_sql = """
        DELETE FROM Stage.stg_Fonte_Dados
        WHERE TRUE;
    """

    with engine.begin() as conn:
        conn.execute(consulta_sql)

    conexao.close()

    ### Identificar ID Fonte de dados

    conn = engine.connect()

    consulta_sql = """
        SELECT id_Fonte_Dados
        FROM Internet_db.Fonte_Dados_Internet
        WHERE Site_Fonte_Dados = '""" +  url  + """'

    """

    with engine.begin() as conn:
        result_query = conn.execute(consulta_sql).fetchall()

    return result_query[0][0]


# In[ ]:


def consultar_fonte_dados(nome_site, site_fonte_dados, nome_fonte_dados):
    
    conn = engine.connect()

    consulta_sql = """
        SELECT id_Fonte_Dados
        FROM Internet_db.Fonte_Dados_Internet
        WHERE Site_Fonte_Dados = '""" +  site_fonte_dados  + """'

    """

    with engine.begin() as conn:
        result_query = conn.execute(consulta_sql).fetchall()


    if len(result_query) != 0:
        return result_query[0][0]

    else:
        print('Fonte de dados não está previamente cadastrada')
        print('Fonte de dados será cadastrada agora')

        return cadastrar_fonte_dados (nome_site, site_fonte_dados, nome_fonte_dados)


# In[ ]:


def inserir_dados_stg(nome_data_frame, nome_banco_tabela, nome_tabela, nome_banco_procedure, nome_procedure):

    # Inserir dados tabela
    with engine.connect() as conn, conn.begin():
        nome_data_frame.to_sql(nome_tabela, conn, if_exists='replace', index=False)



    ## Executar procedure versionamento
    conexao = engine.connect()

    consulta_sql = "CALL "  + nome_banco_procedure + "." + nome_procedure + "();"

    with engine.begin() as conn:
        conn.execute(consulta_sql)



    ## Deletar dados tabela

    consulta_sql = """
        DELETE FROM """ + nome_banco_tabela + """.""" + nome_tabela + """
        WHERE TRUE;
    """

    with engine.begin() as conn:
        conn.execute(consulta_sql)

    conexao.close()


# In[ ]:





# In[ ]:





# ## Criar conexão banco dados

# In[ ]:


#texto = open('/root/airflow/scripts/config/conexao_db.txt')
texto = open('conexao_db.txt')
conexao_db = texto.read()
texto.close()


# In[ ]:


engine = create_engine(conexao_db, encoding='utf-8')


# In[ ]:


print(engine.table_names())


# In[ ]:





# ### Datas de vigências

# In[ ]:


data_inicio_vigencia = datetime.now().strftime('%Y-%m-%d')


# In[ ]:


data_fim_vigencia = '9999-12-31'


# In[ ]:





# # Importar base de dados de países

# In[ ]:


#Realizar getURL no arquivo json
url = 'https://servicodados.ibge.gov.br/api/v1/localidades/paises'
r = requests.get(url)
print (len(r.content))


# In[ ]:


#Importar arquivos
paises = json.loads(r.content)
data_extracao = datetime.now()


# In[ ]:


#converter json para data frame
paises = json_normalize(paises)


# In[ ]:





# ### Identificar ID Fonte de dados

# In[ ]:


nome_site = 'IBGE'
site_fonte_dados = url
nome_fonte_dados = 'paises'


# In[ ]:


id_fonte_dados = consultar_fonte_dados(nome_site, site_fonte_dados, nome_fonte_dados)


# In[ ]:


print('id_fonte_dados:' , id_fonte_dados)


# In[ ]:





# ## Conversões

# In[ ]:


#renomear colunas
paises = paises.rename(columns={
    'id.M49':'id_Pais'
    ,'nome':'nome_Pais'
    ,'sigla':'sigla_Regiao'
    ,'id.ISO-ALPHA-2' : 'ISO_Alpha_2'
    ,'id.ISO-ALPHA-3' : 'ISO_Alpha_3'
    ,'sub-regiao.id.M49' : 'id_Subregiao_Mundo'
    ,'sub-regiao.nome' : 'nome_Subregiao_Mundo'
    ,'sub-regiao.regiao.id.M49' : 'id_Regiao_Mundo'
    ,'sub-regiao.regiao.nome' : 'nome_Regiao_Mundo'
})


# In[ ]:


paises['Data_Extracao'] = data_extracao


# In[ ]:


paises['id_Fonte_Dados'] = id_fonte_dados


# In[ ]:


paises['Data_Inicio_Vigencia'] = data_inicio_vigencia


# In[ ]:


paises['Data_Fim_Vigencia'] = data_fim_vigencia


# In[ ]:





# ### Gerar informação de Região país

# In[ ]:


regiao_paises = paises.groupby([
    'id_Fonte_Dados'
    ,'id_Regiao_Mundo'
    ,'nome_Regiao_Mundo'
    ,'Data_Inicio_Vigencia'
    ,'Data_Fim_Vigencia'
]).size().reset_index()


# In[ ]:


regiao_paises['Data_Extracao'] = data_extracao


# In[ ]:


# Selecionar colunas
regiao_paises = regiao_paises[[
    'id_Fonte_Dados'
    ,'id_Regiao_Mundo'
    ,'nome_Regiao_Mundo'
    ,'Data_Extracao'
    ,'Data_Inicio_Vigencia'
    ,'Data_Fim_Vigencia'    
]]


# In[ ]:





# ### Inserir dados

# In[ ]:


inserir_dados_stg(regiao_paises, 'Stage', 'stg_Regiao_Mundo_IBGE', 'Stage', 'Versiona_stg_Regiao_Mundo_IBGE')


# In[ ]:





# ### Gerar informação de Subregião país

# In[ ]:


subregiao_paises = paises.groupby([
    'id_Fonte_Dados'
    ,'id_Subregiao_Mundo'
    ,'nome_Subregiao_Mundo'
    ,'Data_Inicio_Vigencia'
    ,'Data_Fim_Vigencia'
]).size().reset_index()


# In[ ]:


subregiao_paises['Data_Extracao'] = data_extracao


# In[ ]:


# Selecionar colunas
subregiao_paises = subregiao_paises[[
    'id_Fonte_Dados'
    ,'id_Subregiao_Mundo'
    ,'nome_Subregiao_Mundo'
    ,'Data_Extracao'
    ,'Data_Inicio_Vigencia'
    ,'Data_Fim_Vigencia'    
]]


# In[ ]:





# ### Inserir dados

# In[ ]:


inserir_dados_stg(subregiao_paises, 'Stage', 'stg_Subregiao_Mundo_IBGE', 'Stage', 'Versiona_stg_Subregiao_Mundo_IBGE')


# In[ ]:





# ### Gerar informação de País

# In[ ]:


# Selecionar colunas
paises = paises[[
    'id_Fonte_Dados'
    ,'id_Regiao_Mundo'
    ,'id_Subregiao_Mundo'
    ,'id_Pais'
    ,'nome_Pais'
    ,'ISO_Alpha_2'
    ,'ISO_Alpha_3'
    ,'Data_Extracao'
    ,'Data_Inicio_Vigencia'
    ,'Data_Fim_Vigencia'
]]


# In[ ]:





# ### Inserir dados

# In[ ]:


inserir_dados_stg(paises, 'Stage', 'stg_Pais_IBGE', 'Stage', 'Versiona_stg_Pais_IBGE')


# In[ ]:





# # Importar base de dados de regiões

# In[ ]:


#Realizar getURL no arquivo json
url = 'https://servicodados.ibge.gov.br/api/v1/localidades/regioes'
r = requests.get(url)
print (len(r.content))


# In[ ]:


#Importar arquivos
regioes = json.loads(r.content)
data_extracao = datetime.now()


# In[ ]:


#converter json para data frame
regioes = json_normalize(regioes)


# In[ ]:





# ### Identificar ID Fonte de dados

# In[ ]:


nome_site = 'IBGE'
site_fonte_dados = url
nome_fonte_dados = 'regioes'


# In[ ]:


id_fonte_dados = consultar_fonte_dados(nome_site, site_fonte_dados, nome_fonte_dados)


# In[ ]:


print('id_fonte_dados:' , id_fonte_dados)


# In[ ]:





# In[ ]:


#renomear colunas
regioes = regioes.rename(columns={'id':'id_Regiao', 'nome':'nome_Regiao', 'sigla':'sigla_Regiao'})


# In[ ]:


regioes['Data_Extracao'] = data_extracao


# In[ ]:


regioes['id_Fonte_Dados'] = id_fonte_dados


# In[ ]:


regioes['Data_Inicio_Vigencia'] = data_inicio_vigencia


# In[ ]:


regioes['Data_Fim_Vigencia'] = data_fim_vigencia


# In[ ]:


regioes['Data_Extracao'] = data_extracao


# In[ ]:


# Selecionar colunas
regioes = regioes[[
    'id_Fonte_Dados'
    ,'id_Regiao'
    ,'sigla_Regiao'
    ,'nome_Regiao'
    ,'Data_Extracao'
    ,'Data_Inicio_Vigencia'
    ,'Data_Fim_Vigencia'
]]


# In[ ]:





# ### Inserir dados

# In[ ]:


inserir_dados_stg(regioes, 'Stage', 'stg_Regiao_IBGE', 'Stage', 'Versiona_stg_Regiao_IBGE')


# In[ ]:





# # Importar base de dados de estados

# In[ ]:


#Realizar getURL no arquivo json
url = 'https://servicodados.ibge.gov.br/api/v1/localidades/estados'
r = requests.get(url)
print (len(r.content))


# In[ ]:


#Importar arquivos
estados = json.loads(r.content)
data_extracao = datetime.now()


# In[ ]:


#converter json para data frame
estados = json_normalize(estados)


# ### Identificar ID Fonte de dados

# In[ ]:


nome_site = 'IBGE'
site_fonte_dados = url
nome_fonte_dados = 'estados'


# In[ ]:


id_fonte_dados = consultar_fonte_dados(nome_site, site_fonte_dados, nome_fonte_dados)


# In[ ]:


print('id_fonte_dados:' , id_fonte_dados)


# In[ ]:





# In[ ]:


#renomear colunas
estados = estados.rename(columns={'id':'id_Estado', 'nome':'nome_Estado', 'sigla':'sigla_Estado', 'regiao.id':'id_Regiao'})


# In[ ]:


estados['id_Fonte_Dados'] = id_fonte_dados


# In[ ]:


estados['Data_Extracao'] = data_extracao


# In[ ]:


estados['Data_Inicio_Vigencia'] = data_inicio_vigencia


# In[ ]:


estados['Data_Fim_Vigencia'] = data_fim_vigencia


# In[ ]:


# Selecionar colunas
estados = estados[[
    'id_Fonte_Dados'
    ,'id_Estado'
    ,'id_Regiao'
    ,'sigla_Estado'
    ,'nome_Estado'
    ,'Data_Extracao'
    ,'Data_Inicio_Vigencia'
    ,'Data_Fim_Vigencia'
]]


# In[ ]:





# ### Inserir dados

# In[ ]:


inserir_dados_stg(estados, 'Stage', 'stg_Estado_IBGE', 'Stage', 'Versiona_stg_Estado_IBGE')


# In[ ]:





# # Importar base de dados de Mesoregião

# In[ ]:


#Realizar getURL no arquivo json
url = 'https://servicodados.ibge.gov.br/api/v1/localidades/mesorregioes'
r = requests.get(url)
print (len(r.content))


# In[ ]:


#Importar arquivos
mesoregicao = json.loads(r.content)
data_extracao = datetime.now()


# In[ ]:


#converter json para data frame
mesoregicao = json_normalize(mesoregicao)


# In[ ]:





# ### Identificar ID Fonte de dados

# In[ ]:


nome_site = 'IBGE'
site_fonte_dados = url
nome_fonte_dados = 'mesorregioes'


# In[ ]:


id_fonte_dados = consultar_fonte_dados(nome_site, site_fonte_dados, nome_fonte_dados)


# In[ ]:


print('id_fonte_dados:' , id_fonte_dados)


# In[ ]:





# In[ ]:


#renomear colunas
mesoregicao = mesoregicao.rename(columns={'id':'id_Mesoregiao', 'nome':'nome_Mesoregiao', 'UF.id':'id_Estado', 'UF.regiao.id':'id_Regiao'})


# In[ ]:


mesoregicao['id_Fonte_Dados'] = id_fonte_dados


# In[ ]:


mesoregicao['Data_Extracao'] = data_extracao


# In[ ]:


mesoregicao['Data_Inicio_Vigencia'] = data_inicio_vigencia


# In[ ]:


mesoregicao['Data_Fim_Vigencia'] = data_fim_vigencia


# In[ ]:


# Selecionar colunas
mesoregicao = mesoregicao[[
    'id_Fonte_Dados'
    ,'id_Mesoregiao'
    ,'id_Estado'
    ,'id_Regiao'
    ,'nome_Mesoregiao'
    ,'Data_Extracao'
    ,'Data_Inicio_Vigencia'
    ,'Data_Fim_Vigencia'
]]


# In[ ]:





# ### Inserir dados

# In[ ]:


inserir_dados_stg(mesoregicao, 'Stage', 'stg_Mesoregiao_IBGE', 'Stage', 'Versiona_stg_Mesoregiao_IBGE')


# In[ ]:





# # Importar base de dados de Microregião

# In[ ]:


#Realizar getURL no arquivo json
url = 'https://servicodados.ibge.gov.br/api/v1/localidades/microrregioes'
r = requests.get(url)
print (len(r.content))


# In[ ]:


#Importar arquivos
microregiao = json.loads(r.content)
data_extracao = datetime.now()


# In[ ]:


#converter json para data frame
microregiao = json_normalize(microregiao)


# In[ ]:





# ### Identificar ID Fonte de dados

# In[ ]:


nome_site = 'IBGE'
site_fonte_dados = url
nome_fonte_dados = 'microrregioes'


# In[ ]:


id_fonte_dados = consultar_fonte_dados(nome_site, site_fonte_dados, nome_fonte_dados)


# In[ ]:


print('id_fonte_dados:' , id_fonte_dados)


# In[ ]:





# In[ ]:


#renomear colunas
microregiao = microregiao.rename(columns={
    'id':'id_Microregiao'
    , 'nome':'nome_Microregiao'
    , 'mesorregiao.id':'id_Mesoregiao'
    , 'mesorregiao.UF.id':'id_Estado'
    , 'mesorregiao.UF.regiao.id':'id_Regiao'
})


# In[ ]:


microregiao['id_Fonte_Dados'] = id_fonte_dados


# In[ ]:


microregiao['Data_Extracao'] = data_extracao


# In[ ]:


microregiao['Data_Inicio_Vigencia'] = data_inicio_vigencia


# In[ ]:


microregiao['Data_Fim_Vigencia'] = data_fim_vigencia


# In[ ]:


# Selecionar colunas
microregiao = microregiao[[
    'id_Fonte_Dados'
    ,'id_Microregiao'
    ,'id_Mesoregiao'
    ,'id_Estado'
    ,'id_Regiao'
    ,'nome_Microregiao'
    ,'Data_Extracao'
    ,'Data_Inicio_Vigencia'
    ,'Data_Fim_Vigencia'
]]


# In[ ]:





# ### Inserir dados

# In[ ]:


inserir_dados_stg(microregiao, 'Stage', 'stg_Microregicao_IBGE', 'Stage', 'Versiona_stg_Microregicao_IBGE')


# In[ ]:





# # Importar base de dados de Região intermediaria

# In[ ]:


#Realizar getURL no arquivo json
url = 'https://servicodados.ibge.gov.br/api/v1/localidades/regioes-intermediarias'
r = requests.get(url)
print (len(r.content))


# In[ ]:


#Importar arquivos
regiao_intermediaria = json.loads(r.content)
data_extracao = datetime.now()


# In[ ]:


#converter json para data frame
regiao_intermediaria = json_normalize(regiao_intermediaria)


# In[ ]:





# ### Identificar ID Fonte de dados

# In[ ]:


nome_site = 'IBGE'
site_fonte_dados = url
nome_fonte_dados = 'regioes-intermediarias'


# In[ ]:


id_fonte_dados = consultar_fonte_dados(nome_site, site_fonte_dados, nome_fonte_dados)


# In[ ]:


print('id_fonte_dados:' , id_fonte_dados)


# In[ ]:





# In[ ]:


#renomear colunas
regiao_intermediaria = regiao_intermediaria.rename(columns={
    'id':'id_Regiao_Intermediaria'
    , 'nome':'nome_Regiao_Intermediaria'
    , 'UF.id':'id_Estado'
    , 'UF.regiao.id':'id_Regiao'
})


# In[ ]:


regiao_intermediaria['id_Fonte_Dados'] = id_fonte_dados


# In[ ]:


regiao_intermediaria['Data_Extracao'] = data_extracao


# In[ ]:


regiao_intermediaria['Data_Inicio_Vigencia'] = data_inicio_vigencia


# In[ ]:


regiao_intermediaria['Data_Fim_Vigencia'] = data_fim_vigencia


# In[ ]:


# Selecionar colunas
regiao_intermediaria = regiao_intermediaria[[
    'id_Fonte_Dados'
    ,'id_Regiao_Intermediaria'
    ,'id_Estado'
    ,'id_Regiao'
    ,'nome_Regiao_Intermediaria'
    ,'Data_Extracao'
    ,'Data_Inicio_Vigencia'
    ,'Data_Fim_Vigencia'
]]


# In[ ]:





# ### Inserir dados

# In[ ]:


inserir_dados_stg(regiao_intermediaria, 'Stage', 'stg_Regiao_Intermediaria_IBGE', 'Stage', 'Versiona_stg_Regiao_Intermediaria_IBGE')


# In[ ]:





# # Importar base de dados de Região imediata

# In[ ]:


#Realizar getURL no arquivo json
url = 'https://servicodados.ibge.gov.br/api/v1/localidades/regioes-imediatas'
r = requests.get(url)
print (len(r.content))


# In[ ]:


#Importar arquivos
regiao_imediatas = json.loads(r.content)
data_extracao = datetime.now()


# In[ ]:


#converter json para data frame
regiao_imediatas = json_normalize(regiao_imediatas)


# In[ ]:





# ### Identificar ID Fonte de dados

# In[ ]:


nome_site = 'IBGE'
site_fonte_dados = url
nome_fonte_dados = 'regioes-imediatas'


# In[ ]:


id_fonte_dados = consultar_fonte_dados(nome_site, site_fonte_dados, nome_fonte_dados)


# In[ ]:


print('id_fonte_dados:' , id_fonte_dados)


# In[ ]:





# In[ ]:


#renomear colunas
regiao_imediatas = regiao_imediatas.rename(columns={
    'id':'id_Regiao_Imediata'
    , 'nome':'nome_Regiao_Imediata'
    , 'regiao-intermediaria.id':'id_Regiao_Intermediaria'
    , 'regiao-intermediaria.UF.id':'id_Estado'
    , 'regiao-intermediaria.UF.regiao.id':'id_Regiao'
})


# In[ ]:


regiao_imediatas['id_Fonte_Dados'] = id_fonte_dados


# In[ ]:


regiao_imediatas['Data_Extracao'] = data_extracao


# In[ ]:


regiao_imediatas['Data_Inicio_Vigencia'] = data_inicio_vigencia


# In[ ]:


regiao_imediatas['Data_Fim_Vigencia'] = data_fim_vigencia


# In[ ]:


# Selecionar colunas
regiao_imediatas = regiao_imediatas[[
    'id_Fonte_Dados'
    ,'id_Regiao_Imediata'
    ,'id_Regiao_Intermediaria'
    ,'id_Estado'
    ,'id_Regiao'
    ,'nome_Regiao_Imediata'
    ,'Data_Extracao'
    ,'Data_Inicio_Vigencia'
    ,'Data_Fim_Vigencia'
]]


# In[ ]:





# ### Inserir dados

# In[ ]:


inserir_dados_stg(regiao_imediatas, 'Stage', 'stg_Regiao_Imediata_IBGE', 'Stage', 'Versiona_stg_Regiao_Imediata_IBGE')


# In[ ]:





# # Importar base de dados de municípios

# In[ ]:


#Realizar getURL no arquivo json
url = 'https://servicodados.ibge.gov.br/api/v1/localidades/municipios'
r = requests.get(url)
print (len(r.content))


# In[ ]:


#Importar arquivos
municipios = json.loads(r.content)
data_extracao = datetime.now()


# In[ ]:


#converter json para data frame
municipios = json_normalize(municipios)


# In[ ]:





# ### Identificar ID Fonte de dados

# In[ ]:


nome_site = 'IBGE'
site_fonte_dados = url
nome_fonte_dados = 'municipios'


# In[ ]:


id_fonte_dados = consultar_fonte_dados(nome_site, site_fonte_dados, nome_fonte_dados)


# In[ ]:


print('id_fonte_dados:' , id_fonte_dados)


# In[ ]:





# In[ ]:


#renomear colunas
municipios = municipios.rename(columns={
    'id':'id_Municipio'
    ,'nome':'nome_Municipio'
    ,'microrregiao.mesorregiao.UF.id':'id_Estado'
    ,'microrregiao.mesorregiao.UF.regiao.id':'id_Regiao'
    ,'microrregiao.mesorregiao.id' : 'id_Mesoregiao'
    ,'regiao-imediata.id' : 'id_Regiao_Imediata'
    ,'regiao-imediata.regiao-intermediaria.id' : 'id_Regiao_Intermediaria'
    ,'microrregiao.id' : 'id_Microregiao'
})


# In[ ]:


municipios['id_Fonte_Dados'] = id_fonte_dados


# In[ ]:


municipios['Data_Extracao'] = data_extracao


# In[ ]:


municipios['Data_Inicio_Vigencia'] = data_inicio_vigencia


# In[ ]:


municipios['Data_Fim_Vigencia'] = data_fim_vigencia


# In[ ]:


# Selecionar colunas
municipios = municipios[[
    'id_Fonte_Dados'
    ,'id_Regiao'
    ,'id_Estado'
    ,'id_Mesoregiao'
    ,'id_Microregiao'
    ,'id_Regiao_Imediata'
    ,'id_Regiao_Intermediaria'
    ,'id_Municipio'
    ,'nome_Municipio'
    ,'Data_Extracao'
    ,'Data_Inicio_Vigencia'
    ,'Data_Fim_Vigencia'
]]


# In[ ]:





# ### Inserir dados

# In[ ]:


inserir_dados_stg(municipios, 'Stage', 'stg_Municipio_IBGE', 'Stage', 'Versiona_stg_Municipio_IBGE')


# In[ ]:





# # Importar base de dados de distrito

# In[ ]:


#Realizar getURL no arquivo json
url = 'https://servicodados.ibge.gov.br/api/v1/localidades/distritos'
r = requests.get(url)
print (len(r.content))


# In[ ]:


#Importar arquivos
distritos = json.loads(r.content)
data_extracao = datetime.now()


# In[ ]:


#converter json para data frame
distritos = json_normalize(distritos)


# In[ ]:





# ### Identificar ID Fonte de dados

# In[ ]:


nome_site = 'IBGE'
site_fonte_dados = url
nome_fonte_dados = 'distritos'


# In[ ]:


id_fonte_dados = consultar_fonte_dados(nome_site, site_fonte_dados, nome_fonte_dados)


# In[ ]:


print('id_fonte_dados:' , id_fonte_dados)


# In[ ]:





# In[ ]:


#renomear colunas
distritos = distritos.rename(columns={
    'id':'id_Distrito'
    ,'municipio.id':'id_Municipio'
    ,'municipio.regiao-imediata.id' : 'id_Regiao_Imediata'
    ,'municipio.regiao-imediata.regiao-intermediaria.id' : 'id_Regiao_Intermediaria'
    ,'municipio.microrregiao.id' : 'id_Microregiao'
    ,'municipio.microrregiao.mesorregiao.id' : 'id_Mesoregiao'
    ,'municipio.microrregiao.mesorregiao.UF.id' : 'id_Estado'
    ,'municipio.regiao-imediata.regiao-intermediaria.UF.regiao.id' : 'id_Regiao'
    ,'nome':'nome_Distrito'
})


# In[ ]:


distritos['id_Fonte_Dados'] = id_fonte_dados


# In[ ]:


distritos['Data_Extracao'] = data_extracao


# In[ ]:


distritos['Data_Inicio_Vigencia'] = data_inicio_vigencia


# In[ ]:


distritos['Data_Fim_Vigencia'] = data_fim_vigencia


# In[ ]:


# Selecionar colunas
distritos = distritos[[
    'id_Fonte_Dados'
    ,'id_Distrito'
    ,'id_Municipio'
    ,'id_Regiao_Imediata'
    ,'id_Regiao_Intermediaria'
    ,'id_Microregiao'
    ,'id_Mesoregiao'
    ,'id_Estado'
    ,'id_Regiao'
    ,'nome_Distrito'
    ,'Data_Extracao'
    ,'Data_Inicio_Vigencia'
    ,'Data_Fim_Vigencia'
]]


# In[ ]:





# ### Inserir dados

# In[ ]:


inserir_dados_stg(distritos, 'Stage', 'stg_Distrito_IBGE', 'Stage', 'Versiona_stg_Distrito_IBGE')


# In[ ]:





# # Importar base de dados de subdistrito

# In[ ]:


#Realizar getURL no arquivo json
url = 'https://servicodados.ibge.gov.br/api/v1/localidades/subdistritos'
r = requests.get(url)
print (len(r.content))


# In[ ]:


#Importar arquivos
sub_distritos = json.loads(r.content)
data_extracao = datetime.now()


# In[ ]:


#converter json para data frame
sub_distritos = json_normalize(sub_distritos)


# In[ ]:





# ### Identificar ID Fonte de dados

# In[ ]:


nome_site = 'IBGE'
site_fonte_dados = url
nome_fonte_dados = 'subdistritos'


# In[ ]:


id_fonte_dados = consultar_fonte_dados(nome_site, site_fonte_dados, nome_fonte_dados)


# In[ ]:


print('id_fonte_dados:' , id_fonte_dados)


# In[ ]:





# In[ ]:


#renomear colunas
sub_distritos = sub_distritos.rename(columns={
    'id' : 'id_Sub_Distrito'    
    ,'distrito.id':'id_Distrito'
    ,'distrito.municipio.id':'id_Municipio'
    ,'distrito.municipio.regiao-imediata.id' : 'id_Regiao_Imediata'
    ,'distrito.municipio.regiao-imediata.regiao-intermediaria.id' : 'id_Regiao_Intermediaria'
    ,'distrito.municipio.microrregiao.id' : 'id_Microregiao'
    ,'distrito.municipio.microrregiao.mesorregiao.id' : 'id_Mesoregiao'
    ,'distrito.municipio.microrregiao.mesorregiao.UF.id' : 'id_Estado'
    ,'distrito.municipio.regiao-imediata.regiao-intermediaria.UF.regiao.id' : 'id_Regiao'
    ,'nome':'nome_Sub_Distrito'
})


# In[ ]:


sub_distritos['id_Fonte_Dados'] = id_fonte_dados


# In[ ]:


sub_distritos['Data_Extracao'] = data_extracao


# In[ ]:


sub_distritos['Data_Inicio_Vigencia'] = data_inicio_vigencia


# In[ ]:


sub_distritos['Data_Fim_Vigencia'] = data_fim_vigencia


# In[ ]:


# Selecionar colunas
sub_distritos = sub_distritos[[
    'id_Fonte_Dados'
    ,'id_Sub_Distrito'
    ,'id_Distrito'
    ,'id_Municipio'
    ,'id_Regiao_Imediata'
    ,'id_Regiao_Intermediaria'
    ,'id_Microregiao'
    ,'id_Mesoregiao'
    ,'id_Estado'
    ,'id_Regiao'
    ,'nome_Sub_Distrito'
    ,'Data_Extracao'
    ,'Data_Inicio_Vigencia'
    ,'Data_Fim_Vigencia'
]]


# In[ ]:





# ### Inserir dados

# In[ ]:


inserir_dados_stg(sub_distritos, 'Stage', 'stg_Sub_Distrito_IBGE', 'Stage', 'Versiona_stg_Sub_Distrito_IBGE')


# In[ ]:





# In[ ]:


#Fim da execução do notebook
end_general = datetime.now()


# In[ ]:


# Write the DataFrame to a BigQuery table

print("Tempo total notebook " + str(end_general - start_general))


# In[ ]:




