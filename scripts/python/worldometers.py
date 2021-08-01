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
import sqlalchemy
import mysql.connector


# In[ ]:


import requests
from bs4 import BeautifulSoup
from lxml import html
import re


# In[ ]:





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





# # Importar base de dados de lista de paises

# # Importar base de dados lista de país site worldometers

# In[ ]:


session_requests = requests.session()


# In[ ]:


url = 'https://www.worldometers.info/world-population/population-by-country/'
consulta = session_requests.get(url)
data_extracao = datetime.now()


# In[ ]:


consulta.ok # Will tell us if the last request was ok
consulta.status_code # Will give us the status from the last request


# In[ ]:





# ### Identificar ID Fonte de dados

# In[ ]:


nome_site = 'worldometers'
site_fonte_dados = url
nome_fonte_dados = 'population-by-country'


# In[ ]:


id_fonte_dados = consultar_fonte_dados(nome_site, site_fonte_dados, nome_fonte_dados)


# In[ ]:


print('id_fonte_dados:' , id_fonte_dados)


# In[ ]:





# ## Conversões

# In[ ]:


soup = BeautifulSoup(consulta.content, 'html.parser')


# In[ ]:


lista_pais = pd.read_html(soup.prettify())[0]


# In[ ]:


lista_pais.head(1)


# In[ ]:


lista_pais.rename(columns = {
    lista_pais.columns[1] : 'Country_Name'
}, inplace = True) 


# In[ ]:


lista_pais = lista_pais[['Country_Name']]


# In[ ]:


lista_pais['id_Fonte_Dados'] = id_fonte_dados


# In[ ]:


lista_pais['Data_Inicio_Vigencia'] = datetime.now().strftime('%Y-%m-%d')


# In[ ]:


lista_pais['Data_Fim_Vigencia'] = '9999-12-31'


# In[ ]:





# In[ ]:


with engine.connect() as conn, conn.begin():
    lista_pais.to_sql('stg_Country', conn, if_exists='replace', index=False)


# In[ ]:





# ## Executar procedure versionamento

# In[ ]:


conexao = engine.connect()


# In[ ]:


consulta_sql = "CALL Stage.Versiona_stg_Country();"


# In[ ]:


with engine.begin() as conn:
    conn.execute(consulta_sql)


# In[ ]:





# ## Deletar dados tabela stage

# In[ ]:


consulta_sql = """
    DELETE FROM Stage.stg_Country
    WHERE TRUE;
"""


# In[ ]:


with engine.begin() as conn:
    conn.execute(consulta_sql)


# In[ ]:


conexao.close()


# In[ ]:





# # Importar base de dados de população por país site worldometers

# ## Conversões

# In[ ]:


soup = BeautifulSoup(consulta.content, 'html.parser')


# In[ ]:


populacao = pd.read_html(soup.prettify())[0]


# In[ ]:


# identificar ano referência
ano_referencia = populacao.columns[2]
ano_referencia = re.search('[0-9]{4}', ano_referencia).group()
print('ano_referencia:', ano_referencia)


# In[ ]:


populacao.rename(columns = {
    populacao.columns[0] : 'Rank_Population'
    ,populacao.columns[1] : 'Country'
    ,populacao.columns[2] : 'Population'
    ,populacao.columns[4] : 'Net_Change'
    ,populacao.columns[6] : 'Land_Area'
    ,populacao.columns[7] : 'Migrants_Net_Change'
    ,populacao.columns[9] : 'Avarege_Age'
    ,populacao.columns[10] : 'Perc_Urban_Pop'
}, inplace = True) 


# In[ ]:


try:
    populacao['Perc_Urban_Pop'] = pd.to_numeric(populacao['Perc_Urban_Pop'].str.replace('%', ''), errors='coerce').apply(np.float) / 100
    populacao['Perc_Rural_Pop'] = 1- populacao['Perc_Urban_Pop']
except:
    pass


# In[ ]:


populacao['id_Fonte_Dados'] = id_fonte_dados


# In[ ]:


populacao['Ano_Referencia'] = int(ano_referencia)


# In[ ]:


populacao['Data_Extracao'] = data_extracao


# In[ ]:


# Selecionar colunas
populacao = populacao[[
    'id_Fonte_Dados'
    ,'Rank_Population'
    ,'Country'
    ,'Population'
    ,'Net_Change'
    ,'Land_Area'
    ,'Ano_Referencia'
    ,'Data_Extracao'
]]


# In[ ]:





# In[ ]:


with engine.connect() as conn, conn.begin():
    populacao.to_sql('stg_Population_Country', conn, if_exists='replace', index=False)


# In[ ]:





# ## Executar procedure versionamento

# In[ ]:


conexao = engine.connect()


# In[ ]:


consulta_sql = "CALL Stage.Versiona_stg_Population_Country();"


# In[ ]:


with engine.begin() as conn:
    conn.execute(consulta_sql)


# In[ ]:





# ## Deletar dados tabela stage

# In[ ]:


consulta_sql = """
    DELETE FROM Stage.stg_Population_Country
    WHERE TRUE;
"""


# In[ ]:


with engine.begin() as conn:
    conn.execute(consulta_sql)


# In[ ]:


conexao.close()


# In[ ]:





# In[ ]:


#Fim da execução do notebook
end_general = datetime.now()


# In[ ]:


# Tempo execução código
print("Tempo total notebook " + str(end_general - start_general))


# In[ ]:




