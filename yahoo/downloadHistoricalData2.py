#!/usr/bin/python3.6
import requests
import scrapy
import os, io
import json
import pandas as pd
from datetime import datetime, timedelta




def getCrumb():
    url = 'http://finance.yahoo.com/lookup?s=x' #Dummy url
    response=requests.get(url)
    cookies=response.cookies
    html = response.content
    selector = scrapy.Selector(text=html, type='html')
    extracted_script = selector.xpath('//body//script//text()').extract()
    html_str = str(html)
    i = html_str.find("RequestPlugin")
    #print(html_str[i:i+60])
    crumb = html_str[i:i+60].split('"crumb":')[1].split(',')[0].replace('"','')
    result = dict()
    result['crumb'] = crumb
    result['cookies'] = cookies
    return result

def isJson(data):
    try:
        json_content = json.loads(data)
    except ValueError as e:
        return False
    return True

def checkResponse(res):
    #Check response status
    if(res.status_code != 200):
        print('Error: status code: '+ str(res.status_code))
        return False
    elif(isJson(res.content)):
        json_content = json.loads(data)
        if(json_content['finance']['error']['code']):
            if(json_content['finance']['error']['code']=="Unauthorized"):
                print('Error: Unauthorized - ' + json_content['finance']['error']['description'])
                return False
        else:
            return True
    else:
        return True


def downloadData(ticker, period1, period2, interval, event, crumb, cookies):
    url = 'http://query1.finance.yahoo.com/v7/finance/download/{}.SA'.format(ticker)
    payload = {'period1': period1, 'period2': period2, 'interval': interval, 'events': event, 'crumb':crumb}
    response = requests.get(url, params=payload, allow_redirects=True, cookies=cookies)
    # if(response.status_code == 200):
    if(checkResponse(response)):
        # if(event == 'history'):
        newdata = pd.read_csv(io.StringIO(response.content.decode('utf-8')))
        path = './output/'+event+'/'+ ticker + '.csv'
        if os.path.exists(path):
            df = pd.read_csv(path)
            df.Date = pd.to_datetime(df.Date)
            lastday=df.Date.iloc[-1]
            newdata.Date = pd.to_datetime(newdata.Date)
            newdata=newdata.sort_values('Date').reset_index(drop=True)
            newdata=newdata.loc[newdata.Date > lastday]
            df = pd.concat([df,newdata]).reset_index(drop=True)
            df.to_csv(path, index=False)
        else:
            newdata.Date = pd.to_datetime(newdata.Date)
            newdata=newdata.sort_values('Date').reset_index(drop=True)
            newdata.to_csv(path, index=False)


def getLastDayInDataBase(ticker, database):
    path = './output/'+ database+ '/'+ ticker + '.csv'
    if not os.path.exists(path):
        return  datetime(1969,12,31)
    else:
        df = pd.read_csv(path)
        df.Date = pd.to_datetime(df.Date)
        df=df.sort_values('Date').reset_index(drop=True)
        return df.Date.iloc[-1]

def updateDailyDataBase(ticker, l, database):
    refDate = datetime(1969,12,31)
    hoje = datetime.now() - timedelta(hours=3) #Brasilia Local Time (UTC-3h)
    lastday = getLastDayInDataBase(ticker, database)
    print(lastday)
    period1 = round((lastday-refDate).total_seconds())
    period2 = round((hoje-refDate).total_seconds())
    # print(lastday)
    # # print(period1)
    # # print(period2)
    # print((period2-period1)/3600 )
    if (period2-period1)/3600 > 24:
        print(' - atualizando dados para: ' + ticker)
        downloadData(ticker, period1, period2, '1d', database, l['crumb'], l['cookies'])
    else:
        print( '   ' + ticker + ' - já atualizado no banco de dados DAILY.')
        #downloadData(ticker, period1, period2, '1d', 'history', l['crumb'], l['cookies'])

# def createIntradayDataBase(tickers, l):
#     refDate = datetime(1969,12,31)
#     hoje = datetime.now()
#     period1 = hoje - timedelta(days=7)
#     period1 = round((period1-refDate).total_seconds())
#     period2 = round((hoje-refDate).total_seconds()) #last minute
#     downloadData(ticker, period1, period2, '1d', 'history', l['crumb'], l['cookies'])
    
if __name__ == "__main__":

    #inputfile = '~/stocklab_data/b3/output_b3/dados_cia.json'
    inputfile = '/Users/BrunoMattos/Documents2/Dev/stocklab_data/b3/output_b3/dados_cia.json'
    # inputfile = '/home/mattost14/StockLab_data/b3/output_b3/dados_cia.json'
    collectionID = 'dados_cia'

    with open(inputfile) as file:
        data = json.load(file)
    df = pd.DataFrame.from_dict(data['dados_cia'], orient='index')
    allTickers = []
    df.list_of_tickers.map(lambda l: allTickers.extend(l))
    allTickers.sort()
    allTickers = ['ABEV3']
    l = getCrumb()
    numOfTickers = len(allTickers)
    count=0
    for ticker in allTickers:
        count=count+1
        print(str(count) + '/' + str(numOfTickers) + ' - ' + ticker)
        print(' - historical:')
        updateDailyDataBase(ticker, l, 'historical')
        print(' - dividends:')
        updateDailyDataBase(ticker, l, 'dividends')
        #print(str(count)+ '/'+ str(numOfTickers) +' - downloading data for: ' + ticker)
        #downloadData(ticker, period1, period2, '1d', 'history', l['crumb'], l['cookies'])
        #get dividends:
        #downloadData(ticker, period1, period2, '1d', 'div', l['crumb'], l['cookies'])
    #print(os.path)