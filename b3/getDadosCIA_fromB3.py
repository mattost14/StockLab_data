# -*- coding: latin-1 -*-
import requests, json
import pandas as pd
from bs4 import BeautifulSoup
import math

#url = 'http://bvmf.bmfbovespa.com.br/cias-listadas/empresas-listadas/ResumoEmpresaPrincipal.aspx?codigoCvm=24805&idioma=pt-br&vi=FNNLRVHLEOKFPIMWPTKFNCERODDPVDRV-0&modifiedSince=1582893803500&bp=3&app=5286dfffe4e737f8&end=1'
#url = 'http://bvmf.bmfbovespa.com.br/pt-br/mercados/acoes/empresas/ExecutaAcaoConsultaInfoEmp.asp?CodCVM=94&ViewDoc=1&AnoDoc=2020&VersaoDoc=1&NumSeqDoc=91193#a 
def getCIADados_fromB3(cod_cvm):
    url = 'http://bvmf.bmfbovespa.com.br/pt-br/mercados/acoes/empresas/ExecutaAcaoConsultaInfoEmp.asp?CodCVM='+str(cod_cvm)+'&ViewDoc=1&AnoDoc=2020&VersaoDoc=1&NumSeqDoc=91193#a'
    try:
        print('CVM: ' + str(cod_cvm) )
        response = requests.get(url)
        if(response.status_code==200):
            print('  -  Conexão realizada com sucesso!')
            html = response.content
            try:
                #print(html)
                df_list = pd.read_html(html, decimal=',', thousands='.')
            except:
                print('  -  Dados indisponíveis.')
                return pd.Series()


            nome_pregao=None
            site=None
            free_float=None
            cnpj=None
            ticker=None
            list_of_tickers=None
            atividade_principal=None
            participacao_gov=None
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
                        print('  -  Nenhum ativo no Mercado a Vista')
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
                    if isinstance(dados_cia.loc[n,1], basestring):
                        atividade_principal = str(dados_cia.loc[n,1].encode('utf8'))

                #Classificação Setorial
                elif 'Classificação Setorial'.lower() in str(item.encode('utf8')).lower():
                    class_setorial =  str(dados_cia.loc[4,1].encode('utf8'))
                    setor = class_setorial.split('/')[0].rstrip().lstrip()
                    subsetor = class_setorial.split('/')[1].rstrip().lstrip()
                    segmento = class_setorial.split('/')[2].rstrip().lstrip()
                    # print(setor)
                    # print(subsetor)
                    # print(segmento)


            #Site
                elif 'Site'.lower() in str(item.encode('utf8')).lower():
                    site = str(dados_cia.loc[5,1].encode('utf8'))
                    #print(site)

            #Posição Acionária
            free_float_Total= None
            free_float_ON = None
            free_float_PN = None
            acionistas = pd.DataFrame([])
            for num, table in enumerate(df_list):
                first_col = table.columns.values[0]
                if isinstance(first_col, basestring): #basestring
                    if('Nome' in str(first_col.encode('utf8'))):
                        acionistas = df_list[num]
                        # print(acionistas.to_dict('records'))
                        acionistas.Nome = acionistas.Nome.str.encode('utf8')
                        if(any(acionistas.Nome.str.contains('Outros', case=False))):
                            free_float_Total = acionistas.loc[acionistas.Nome == 'Outros']['%Total'].values[0]
                            free_float_ON = acionistas.loc[acionistas.Nome == 'Outros']['%ON'].values[0]
                            free_float_PN = acionistas.loc[acionistas.Nome == 'Outros']['%PN'].values[0]
                        
                        #Check se há governo entre os acionistas
                        listOfGov = ['ministério', 'união federal', 'governo']
                        participacao_gov = False
                        for word in listOfGov:
                            for item in acionistas.Nome:
                                if (word in item.lower()):
                                    participacao_gov=True

                        #Ativos em Circulação no Mercado
                        # ativos_circulacao = df_list[num+1]
                        # if ativos_circulacao.columns.nlevels>1:
                        #     ativos_circulacao.columns = ativos_circulacao.columns.get_level_values(1)
                        # free_float = ativos_circulacao.iloc[-1][-1]
                        # print(free_float)

            row_of_dados_cia = pd.Series([cvm, nome_pregao, ticker, list_of_tickers, atividade_principal, cnpj, site,free_float_Total, free_float_ON, free_float_PN, participacao_gov, setor, subsetor, segmento, acionistas.to_dict('records')],index=['cvm', 'nome_pregao', 'ticker', 'list_of_tickers', 'atividade_principal', 'cnpj', 'site', 'free_float_Total', 'free_float_ON', 'free_float_PN', 'participacao_gov', 'setor', 'subsetor', 'segmento', 'acionistas'])
            #print(row_of_dados_cia)
            return row_of_dados_cia

    except requests.exceptions.Timeout:
        print('TIME OUT')
    except requests.exceptions.RequestException as e:
        print(e)


# list_of_cia = pd.read_csv('list_of_cia.csv')
# total=list_of_cia.CD_CVM.nunique()

# list_img_ids = pd.read_csv('setorial2.csv')

# res=pd.DataFrame([],columns=['cvm', 'nome_pregao', 'ticker', 'list_of_tickers', 'atividade_principal', 'cnpj', 'site', 'free_float', 'participacao_gov','img'])
# i=0
# erroCount=0
# for cvm in list_of_cia['CD_CVM'].unique():
#     print(cvm)  
#     row_of_dados_cia = getCIADados_fromB3(cvm)
#     #print(row_of_dados_cia)
#     if row_of_dados_cia.empty:
#         i=i+1
#         print('%d/%d - %d: ERRO' %(i,total, cvm))
#         erroCount=erroCount+1
#     else:
#         img_id = list_img_ids.loc[list_img_ids.ticker == row_of_dados_cia.ticker]
#         if (len(img_id)>0):
#             img_id=img_id['img_id']
#             row_of_dados_cia=row_of_dados_cia.append(pd.Series(img_id.values[0], index=['img']))
#             res=res.append(row_of_dados_cia, ignore_index=True)
#             i=i+1
#             print('%d/%d - %d: OK' %(i,total, cvm))
#         else:
#             i=i+1
#             print('%d/%d - %d: ERRO - img_id NÀO REGISTRADO' %(i,total, cvm))
#             erroCount=erroCount+1

# res=res.set_index('cvm')
# print(res)
# print('')
# print('Núm de erros encontrados: %d' %erroCount)
# data = {'dados_cia': res.to_dict('index')}
# # data = res.to_dict('index')
# # print(json.dumps(data, indent=2))
# with open('./output_b3/dados_cia2.json', 'w') as outfile:
#     json.dump(data, outfile, indent=2)

#res.to_json('dados_cia.json', orient='records')

#getCIADados_fromB3(24805)
#getCIADados_fromB3(1023)
#getCIADados_fromB3(94)