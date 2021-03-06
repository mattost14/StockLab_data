import requests
import scrapy
import os
import json
import pandas as pd
import datetime
    
inputfile = '/Users/BrunoMattos/Documents2/Dev/stocklab_data/b3/output_b3/dados_cia.json'
collectionID = 'dados_cia'

with open(inputfile) as file:
    data = json.load(file)
df = pd.DataFrame.from_dict(data['dados_cia'], orient='index')
allTickers = []
df.list_of_tickers.map(lambda l: allTickers.extend(l))
allTickers.sort()


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
        if(event == 'history'):
            with open('output/historical/{}.csv'.format(ticker), 'wb') as f:
                f.write(response.content)
        elif(event == 'div'):
            with open('output/dividends/{}.csv'.format(ticker), 'wb') as f:
                f.write(response.content)



if __name__ == "__main__":
    tickers = allTickers
    # tickers = ['EMBR3']
    period = 'MAX'

    refDate = datetime.datetime(1969,12,31)
    hoje = datetime.datetime.now()
    
    if period == 'MAX':
        period1 = refDate
    elif period ==  '5yrs':
        period1 = hoje - datetime.timedelta(days=365*5)
    elif period == '1yr':
        period1 = hoje - datetime.timedelta(days=365)
    elif period == '1d':
        period1 = hoje - datetime.timedelta(days=1)
    period1 = round((period1-refDate).total_seconds())
    period2 = round((hoje-refDate).total_seconds()) #last minute
    l = getCrumb()
    numOfTickers = len(tickers)
    count=0
    for ticker in tickers:
        ticker
        count=count+1
        print(str(count)+ '/'+ str(numOfTickers) +' - downloading data for: ' + ticker)
        downloadData(ticker, period1, period2, '1d', 'history', l['crumb'], l['cookies'])
        #get dividends:
        downloadData(ticker, period1, period2, '1d', 'div', l['crumb'], l['cookies'])
    #print(os.path)