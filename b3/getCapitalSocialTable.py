# -*- coding: utf-8 -*-
import requests, json
import pandas as pd

url = 'http://bvmf.bmfbovespa.com.br/CapitalSocial/'
print(' - Requesting data from B3 ...')
response = requests.get(url)

if response:
    print('Conexão realizada com sucesso. Status: {}'.format(response.status_code))
    html = response.content
    df_list = pd.read_html(html,thousands='.')
    df = df_list[-1]
    #print(df.columns[1])

    #drop rows with nan values
    df=df.dropna()
    #print(df.head(5))
    #Drop some columns
    df=df.drop([df.columns[6]], axis=1)
    print(' - Coluna APROVADAO_EM removida')

    df=df.rename(columns={
        df.columns[0]:'short_name', 
        df.columns[1]:'ticker',
        df.columns[2]:'razao_social',
        df.columns[3]:'seg_mercado',
        df.columns[4]:'tipo_capital',
        df.columns[5]:'capital_social',
        #df.columns[6]:'aprovado_em',
        df.columns[6]:'qtd_acoes_on',
        df.columns[7]:'qtd_acoes_pn',
        df.columns[8]:'qtd_acoes_total',
        })
    #df.to_csv('capital_social.csv', encoding='latin1')
    df= df.reset_index(drop=True)

    #Drop seg_mercado == ['SOMA','FIP MT', 'FIP IE', 'FIP EE']
    dropList = ['SOMA','FIP MT', 'FIP IE', 'FIP EE']
    dropIndex = df[df['seg_mercado'].isin(dropList)].index
    df.drop(dropIndex, inplace=True)
    print(' - {} ativos deletados por pertencerem aos segmentos de mercado do tipo BALCÃO ou FUNDO DE INVESTIMENTO'.format(dropIndex.size))
    #Rename some values in column seg_mercado
    seg_merc = {
        'SOMA': 'MERCADO DE BALCÃO', 
        'BOLSA': 'TRADICIONAL', 
        'BOVESPA NIVEL 2': 'BOVESPA NÍVEL 2', 
        'BOVESPA NIVEL 1': 'BOVESPA NÍVEL 1', 
        'MAIS NIVEL 2': 'BOVESPA MAIS NÍVEL 2', 
        'MAIS': 'BOVESPA MAIS',
    }
    df.seg_mercado=df.seg_mercado.replace(seg_merc)
    print(' - {} ativos listados.'.format(df.ticker.size))

    print(df.head(5))
    data = {'capital_social':df.to_dict(orient='records')}
    #js = json.dumps(data, indent=2)
    with open('./output_b3/capital_social.json', 'w') as outfile:
        json.dump(data, outfile)
    #df.head(5).to_json('capital_social.json', orient='records')
else:
    print('!!! An error has occurred try connecting to B3 server. Error: {}'.format(response.status_code))