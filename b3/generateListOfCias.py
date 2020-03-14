import json
import pandas as pd
import sys
sys.path.append('/Users/BrunoMattos/Documents2/Dev/stocklab_data/Firestore')
from uploadToFirestore import uploadDocumentFromJsonData

inputfile = './output_b3/dados_cia.json'
collectionID = 'dados_cia'

with open(inputfile) as file:
    data = json.load(file)
df = pd.DataFrame.from_dict(data['dados_cia'], orient='index')
df['cvm']=df.index
#df = df[['nome_pregao','img', 'ticker', 'list_of_tickers']]


def generateKeywords(nome, ticker):
    keywords = []
    for l in range(2,len(nome)+1):
        newKeyword = nome[0:l]
        keywords.append(newKeyword)
    for l in range(2,len(ticker)+1):
        newKeyword = ticker[0:l]
        keywords.append(newKeyword)
    #Drop duplicates
    keywords = list(dict.fromkeys(keywords))
    return keywords


df['keywords'] = df.apply(lambda row: generateKeywords(row.nome_pregao, row.ticker), axis=1)
#print(df[['nome_pregao','img', 'ticker', 'list_of_tickers']].head(4))
# print(df.head(5).to_dict(orient='index'))
cias = df.to_dict(orient='index')
contador=0
for key in cias:
    contador = contador+1
    print('Uploading dados da empresa: ' + cias[key]['nome_pregao'])
    uploadDocumentFromJsonData('dados_cia', cias[key], key)
print('Total de empresas: ' + str(contador))
# uploadDocumentFromJsonData(collectionID, jsonData, 'all')



