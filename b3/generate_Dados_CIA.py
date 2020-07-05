import requests, json
import pandas as pd
from bs4 import BeautifulSoup
from getDadosCIA_fromB3 import getCIADados_fromB3

list_of_cia = pd.read_csv('/Users/BrunoMattos/Documents2/Dev/stocklab_data/cvm/output_cvm/list_of_cias.csv')

with open('/Users/BrunoMattos/Documents2/Dev/stocklab_data/b3/output_b3/capital_social.json') as file:
    data = json.load(file)
# print(data['capital_social'])
capital_social = pd.DataFrame.from_dict(data['capital_social'])
# capital_social.set_index('ticker', inplace=True)
# capital_social.set_index('ticker', inplace=True)

def getImageId(listOfTickers):
    for ticker in listOfTickers:
        url = 'http://statusinvest.com.br/acoes/'+ ticker
        print('  - Requesting data for: ' + ticker)
        response = requests.get(url)
        if response:
            html = response.content
            soup = BeautifulSoup(html, 'html.parser')
            #print soup.prettify()
            links = soup.find('div', {'class':'company-brand'})
            if(links):
                fig = str(links['data-img']).split('/')[-1]
                img_name=fig[:-1]
                return img_name
    return ''


res=pd.DataFrame([],columns=[
    'cvm',
    'nome_pregao',
    'ticker',
    'list_of_tickers',
    'atividade_principal',
    'cnpj',
    'site',
    'free_float_Total',
    'free_float_ON',
    'free_float_PN',
    'participacao_gov',
    'img',
    'setor',
    'subsetor',
    'segmento',
    'acionistas',
    "short_name",
    "razao_social",
    "seg_mercado",
    "tipo_capital",
    "capital_social",
    "qtd_acoes_on",
    "qtd_acoes_pn",
    "qtd_acoes_total",
    ])

# listOfNewCVM = [24953,22357,19100,11975,14176,24910,24902,21334]  

i=0
erroCount=0
total=list_of_cia.CD_CVM.nunique()
# total = len(listOfNewCVM)
for cvm in list_of_cia.CD_CVM.unique():
    # cvm = 21490
    # cvm=18970
    i=i+1
    row_of_dados_cia=getCIADados_fromB3(cvm)
    if row_of_dados_cia.empty:  
        print('%d/%d - %d: ERRO' %(i,total, cvm))
        erroCount=erroCount+1
    else:
        listOfTickers = row_of_dados_cia.list_of_tickers
        row_of_dados_cia['img']=getImageId(listOfTickers)
        cs = capital_social[capital_social.ticker == row_of_dados_cia.ticker]
        if not cs.empty:
            newData = pd.Series([cs.short_name.values[0].encode('utf-8'), cs.razao_social.values[0].encode('utf-8'), cs.seg_mercado.values[0].encode('utf-8'), cs.tipo_capital.values[0].encode('utf-8'), float((cs.capital_social.values[0].replace('.','')).replace(',','.')), cs.qtd_acoes_on.values[0], cs.qtd_acoes_pn.values[0], cs.qtd_acoes_total.values[0]], 
            index=[
                "short_name",
                "razao_social",
                "seg_mercado",
                "tipo_capital",
                "capital_social",
                "qtd_acoes_on",
                "qtd_acoes_pn",
                "qtd_acoes_total"
            ])
            row_of_dados_cia = row_of_dados_cia.append(newData)
        res=res.append(row_of_dados_cia, ignore_index=True)

print('Num de erros encontrados: ' + str(erroCount))
res=res.set_index('cvm')
res[' cvm']=res.index
data = {'dados_cia': res.to_dict('index')}
# print(json.dumps(data, indent=2))
with open('./b3/output_b3/dados_cia.json', 'w') as outfile:
    json.dump(data, outfile, indent=2)
# res.to_csv('./output_b3/dados_cia.csv', index=False)
