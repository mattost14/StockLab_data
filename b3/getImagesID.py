# -*- coding: utf-8 -*-
import requests
import pandas as pd
import urllib2
from bs4 import BeautifulSoup

setorial = pd.read_csv('./setorial.csv')

output = pd.DataFrame()

for ticker in setorial.ticker:
    ticker_ends = [3,4,5,6,11] 
    img_name = None
    for ticker_end in ticker_ends:
        url = 'http://statusinvest.com.br/acoes/'+ ticker + str(ticker_end)
        print(' - Requesting data for: ' + ticker + str(ticker_end))
        response = requests.get(url)
        if response:
            html = response.content
            soup = BeautifulSoup(html, 'html.parser')
            #print soup.prettify()
            links = soup.find('div', {'class':'company-brand'})
            if(links):
                fig = str(links['data-img']).split('/')[-1]
                img_name=fig[:-1]
                print(' --- SAVING: ' + img_name)
                setorial.img_id.iloc[setorial[setorial.ticker == ticker].index] = img_name
                break
    # row = setorial[setorial.ticker==ticker]
    # output=output.append(row)
    # row.to_csv('setorial2.csv', index=False, header=False, mode='a', chunksize=1)

print(setorial[['ticker','img_id']])
setorial.to_csv('setorial2.csv', index=False)