# -*- coding: latin-1 -*-
# -*- python: 3 -*-

import requests, json
import pandas as pd
from bs4 import BeautifulSoup
import math
from lxml import html, etree

#url = 'http://bvmf.bmfbovespa.com.br/cias-listadas/empresas-listadas/ResumoEmpresaPrincipal.aspx?codigoCvm=24805&idioma=pt-br&vi=FNNLRVHLEOKFPIMWPTKFNCERODDPVDRV-0&modifiedSince=1582893803500&bp=3&app=5286dfffe4e737f8&end=1'
#url = 'http://bvmf.bmfbovespa.com.br/pt-br/mercados/acoes/empresas/ExecutaAcaoConsultaInfoEmp.asp?CodCVM=94&ViewDoc=1&AnoDoc=2020&VersaoDoc=1&NumSeqDoc=91193#a 
def getCIADados_fromB3(cod_cvm):
    url = 'http://bvmf.bmfbovespa.com.br/pt-br/mercados/acoes/empresas/ExecutaAcaoConsultaInfoEmp.asp?CodCVM='+str(cod_cvm)+'&ViewDoc=1&AnoDoc=2020&VersaoDoc=1&NumSeqDoc=91193#a'
    nome_pregao=None
    site=None
    free_float=None
    cnpj=None
    ticker=None
    list_of_tickers=None
    atividade_principal=None
    participacao_gov=None
    
    response = requests.get(url)
    try:
        df_list = pd.read_html(response.content, decimal=',', thousands='.')
    except:
        print('Dados indisponíveis.')
        return pd.Series()

    #Dados da Companhia
    dados_cia = df_list[0]
    for n, item in enumerate(dados_cia.loc[:,0].values):
        if 'Nome de Pregão'.lower() in str(item.encode('utf8')).lower():
            #nome_pregao = dados_cia.loc[dados_cia.loc[:,0].values==item][1].values[0]
            nome_pregao = str(dados_cia.loc[n,1].encode('utf8'))
            #print(nome_pregao)

        elif 'Códigos de Negociação'.lower() in str(item.encode('utf8')).lower():
            #nome_pregao = dados_cia.loc[dados_cia.loc[:,0].values==item][1].values[0]
            split1=str(dados_cia.loc[n,1].encode('utf8')).split('CVM:')
            cvm = int(split1[1])
            #print(cvm)
            split2=split1[0].split('Códigos ISIN:')
            split3=split2[0].split('Mais Códigos')
            if 'Nenhum' in split3[1]:
                print('Nenhum ativo no Mercado a Vista')
                return pd.Series()
            else:
                list_of_tickers = list(dict.fromkeys(split3[1].split())) #Drop duplicates tickers
                list_of_tickers = pd.Series(list_of_tickers)
                list_of_tickers=list_of_tickers.str.rstrip(';')
                list_of_tickers=list_of_tickers.to_list()
                if len(list_of_tickers)>0:
                    ticker = list_of_tickers[0][0:4] #Get only the ticker 4 characters - exclude the number
        #CNPJ
        elif 'CNPJ'.lower() in str(item.encode('utf8')).lower():
            cnpj =  str(dados_cia.loc[n,1].encode('utf8'))
            #print(cnpj)

        #Atividade Princiapal
        elif 'Atividade Principal'.lower() in str(item.encode('utf8')).lower():
            if isinstance(dados_cia.loc[n,1], str):
                atividade_principal = str(dados_cia.loc[n,1].encode('utf8'))

        #Classificação Setorial
        elif 'Classificação Setorial'.lower() in str(item.encode('utf8')).lower():
            class_setorial =  str(dados_cia.loc[4,1].encode('utf8'))
            #print(class_setorial)

    #Site
        elif 'Site'.lower() in str(item.encode('utf8')).lower():
            site = str(dados_cia.loc[5,1].encode('utf8'))
            #print(site)

        #Posição Acionária
        for num, table in enumerate(df_list):
            first_col = table.columns.values[0]
            if isinstance(first_col, str):
                if('Nome' in str(first_col.encode('utf8'))):
                    acionistas = df_list[num]
                    #print(acionistas)
                    acionistas.Nome = acionistas.Nome.str.encode('utf8')
                    if(any(acionistas.Nome.str.contains('Outros', case=False))):
                        free_float = acionistas.loc[acionistas.Nome == 'Outros']['%Total']
                        print(free_float)
                    
                    #Check se há governo entre os acionistas
                    listOfGov = ['ministério', 'união federal', 'governo']
                    participacao_gov = False
                    for word in listOfGov:
                        for item in acionistas.Nome:
                            if (word in item.lower()):
                                participacao_gov=True

        row_of_dados_cia = pd.Series([cvm, nome_pregao, ticker, list_of_tickers, atividade_principal, cnpj, site, free_float, participacao_gov],index=['cvm', 'nome_pregao', 'ticker', 'list_of_tickers', 'atividade_principal', 'cnpj', 'site', 'free_float', 'participacao_gov'])
        #print(row_of_dados_cia)
        return row_of_dados_cia



list_of_cia = pd.read_csv('list_of_cia.csv')
total=list_of_cia.CD_CVM.nunique()
list_img_ids = pd.read_csv('setorial2.csv')

if __name__ == "__main__":
    print(getCIADados_fromB3(94))