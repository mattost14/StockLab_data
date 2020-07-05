import firebase_admin
from firebase_admin import credentials, firestore

import requests
import scrapy
import time
import os, io
import json
import pandas as pd
import datetime as dt
from datetime import datetime, timedelta
# from uploadData import uploadTicker


cred = credentials.Certificate("yahoo/serviceAccountKey-US.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

PROJECT_NAME = 'stocklab-8255c'

# initialize firebase sdk
# CREDENTIALS = credentials.ApplicationDefault()
# firebase_admin.initialize_app(CREDENTIALS, {
#     'projectId': PROJECT_NAME,
# })

# get firestore client
# FIRESTORE_CLIENT = firestore.client()
# from google.cloud import firestore
# db = firestore.Client()

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



def getLastDayInDataBase(ticker):
    doc_ref = db.collection('quotes').document('max').collection(ticker).document('data')
    doc = doc_ref.get()
    data = doc.to_dict()
    if(data!=None):
        dataset = data['data']
        if(len(dataset)==0):
            return datetime(1969,12,31)
        else: 
            return dataset[0]['Date']
    else:
        return datetime(1969,12,31)

def isJson(data):
    try:
        json_content = json.loads(data.decode('utf-8'))
    except ValueError as e:
        return False
    return True

def checkResponse(res):
    #Check response status
    if(res.status_code != 200):
        print('Error: status code: '+ str(res.status_code))
        return False
    elif(isJson(res.content)):
        json_content = json.loads(res.content.decode('utf-8'))
        if(json_content['finance']['error']['code']):
            if(json_content['finance']['error']['code']=="Unauthorized"):
                print('Error: Unauthorized - ' + json_content['finance']['error']['description'])
        return False
    else:
        return True


def downloadNewData(ticker, period1, period2, interval, event, crumb, cookies):
    url = 'http://query1.finance.yahoo.com/v7/finance/download/{}.SA'.format(ticker)
    payload = {'period1': period1, 'period2': period2, 'interval': interval, 'events': event, 'crumb':crumb}
    response = requests.get(url, params=payload, allow_redirects=True, cookies=cookies)
    if(checkResponse(response)):
        newdata = pd.read_csv(io.StringIO(response.content.decode('utf-8')))
        newdata = newdata[['Date','Adj Close','Volume']]
        newdata.sort_values('Date', ascending=False,inplace=True)
        newdata.dropna(inplace=True)
        return [newdata, True]
    else:
        return [[], False]

def uploadDocumentFromJsonData(collectionID, jsonData, documentID=False):
    # print('Uploading document to collection: ' + collectionID)
    if(documentID):
        doc_ref = db.collection(collectionID).document(documentID)
        doc_ref.set(jsonData)
    else: #Set automatic ID for document
        db.collection(collectionID).add(jsonData)

def uploadQuotes(df, ticker):
    # print('Uploading '+ target + ' of ' + ticker + ' to Firestore:')
    #Get last 1 week
    # df.Date=pd.to_datetime(df.Date)
    # #Drop row if Volume=0
    # indexRows = df[ df['Volume'] == 0 ].index
    # df.drop(indexRows , inplace=True)
    # df.sort_values('Date', ascending=False,inplace=True)
    # df.dropna(inplace=True)
    # df.reset_index(inplace=True, drop=True)
    # df.Date = df.Date.apply(lambda d: d.replace(hour=12))
    
    delta = dt.datetime.today() - df.Date
    delta = delta.map(lambda d : d.days) 
    indexRows = (delta <= 7)
    data = df[indexRows]
    
    if(data.empty==False):
        jsonData = {
            'ticker': ticker,
            'data': data.to_dict('records')
        }
        uploadDocumentFromJsonData('quotes', jsonData, 'last1w/'+ticker+'/data')


    #Get last 1 month
    delta = dt.datetime.today() - df.Date
    delta = delta.map(lambda d : d.days) 
    indexRows = (delta <= 30)
    data = df[indexRows]
    if(data.empty==False):
        jsonData = {
            'ticker': ticker,
            'data': data.to_dict('records')
            }
    else: 
        jsonData = {
            'ticker': ticker,
            'data': []
        }
    # print(' - Last month...')
    uploadDocumentFromJsonData('quotes', jsonData, 'last1m/'+ticker+'/data')

    #Get last 6 months
    delta = dt.datetime.today() - df.Date
    delta = delta.map(lambda d : d.days) 
    indexRows = (delta <= 30*6)
    data = df[indexRows]
    if(data.empty==False):
        jsonData = {
            'ticker': ticker,
            'data': data.to_dict('records')
            }
    else: 
        jsonData = {
            'ticker': ticker,
            'data': []
        }
    # print(' - Last 6 months...')
    uploadDocumentFromJsonData('quotes', jsonData, 'last6m/'+ticker+'/data')
    
    #Get last 1 year
    delta = dt.datetime.today() - df.Date
    delta = delta.map(lambda d : d.days) 
    indexRows = (delta <= 30*12)
    data = df[indexRows]
    if(data.empty==False):
        jsonData = {
            'ticker': ticker,
            'data': data.to_dict('records')
            }
    else: 
        jsonData = {
            'ticker': ticker,
            'data': []
        }
    # print(' - Last 1 year...')
    uploadDocumentFromJsonData('quotes', jsonData, 'last1yr/'+ticker+'/data')

    #Get last 2 years
    delta = dt.datetime.today() - df.Date
    delta = delta.map(lambda d : d.days) 
    indexRows = (delta <= 30*12*2)
    data = df[indexRows]
    if(data.empty==False):
        jsonData = {
            'ticker': ticker,
            'data': data.to_dict('records')
            }
    else: 
        jsonData = {
            'ticker': ticker,
            'data': []
        }
    # print(' - Last 2 years...')
    uploadDocumentFromJsonData('quotes', jsonData, 'last2yr/'+ticker+'/data')

    #Get last 5 years
    delta = dt.datetime.today() - df.Date
    delta = delta.map(lambda d : d.days) 
    indexRows = (delta <= 30*12*5)
    data = df[indexRows]
    if(data.empty==False):
        jsonData = {
            'ticker': ticker,
            'data': data.to_dict('records')
            }
    else: 
        jsonData = {
            'ticker': ticker,
            'data': []
        }
    # print(' - Last 5 years...')
    uploadDocumentFromJsonData('quotes', jsonData, 'last5yr/'+ticker+'/data')
    return True

def updateDailyDataBase(ticker, l):
    refDate = datetime(1969,12,31)
    hoje = datetime.now() - timedelta(hours=3) #Brasilia Local Time (UTC-3h)
    # lastday = getLastDayInDataBase(ticker)
    doc_ref = db.collection('quotes').document('last5yr').collection(ticker).document('data')
    doc = doc_ref.get()
    data = doc.to_dict()
    if(data!=None):
        dataset = data['data']
        if(len(dataset)==0):
            lastday = datetime(1969,12,31)
        else: 
            lastday = dataset[0]['Date']
            lastday = datetime(lastday.year , lastday.month, lastday.day, lastday.hour, lastday.minute)
    else:
        data = {'data': []}
        lastday = datetime(1969,12,31)

    if (hoje-lastday).total_seconds()/3600 > 24:
        period1 = round((lastday-refDate).total_seconds())
        period2 = round((hoje-refDate).total_seconds())
        [newdata, flag] = downloadNewData(ticker, period1, period2, '1d','history', l['crumb'], l['cookies'])
        if(flag):
            
            newdata.Date=pd.to_datetime(newdata.Date)
            #Drop row if Volume=0
            indexRows = newdata[ newdata['Volume'] == 0 ].index
            newdata.drop(indexRows , inplace=True)
            newdata.sort_values('Date', ascending=False,inplace=True)
            newdata.dropna(inplace=True)
            newdata.reset_index(inplace=True, drop=True)
            newdata.Date = newdata.Date.apply(lambda d: d.replace(hour=12))
            lastData = pd.DataFrame(data['data'])
            # print(lastData)
            if(len(newdata)!=0):         
                if(lastData.empty):
                    check = uploadQuotes(newdata, ticker)
                else:    
                    lastData.Date = lastData.Date.dt.strftime('%Y-%m-%d')
                    lastData.Date=pd.to_datetime(lastData.Date)
                    lastData.Date = lastData.Date.apply(lambda d: d.replace(hour=12))
                    updatedData = newdata.append(lastData, ignore_index=True, sort=True)
                    check = uploadQuotes(updatedData, ticker)
                if(check):
                    print('Updated')
                    return True
                else:
                    print('Error during uploadQuotes')
                    return False
            elif lastData.empty:
                print('lastData is empty')
                return False
            else:
                print('newdata is empty')
        else:
            print('Error during download new data')
            return False
    else:
        print('        '+ 'Já Atualizado')
        return True


def extractListOfTickers():
    docs = db.collection('dados_cia').stream()
    listOfTickers = []
    for doc in docs:
        d = doc.to_dict()
        listOfTickers.extend(d['list_of_tickers'])
    return listOfTickers

if __name__ == "__main__":
    l = getCrumb()
    # flag = updateDailyDataBase('MGLU3', l)
    # print(flag)
    docs = db.collection(u'dados_cia').stream()
    dados_cia = []
    for doc in docs:
        d = doc.to_dict()
        tickers = d['list_of_tickers']
        # tickers = d['list_of_tickers_onmarket']
        dados_cia.append({"id": doc.id, "tickers": tickers})
    
    countError = 0
    list_of_tickers_error = []
    for cia in dados_cia:
        list_of_tickers_onmarket = []
        tickers = cia['tickers']
        for ticker in tickers:
            flag = updateDailyDataBase(ticker, l)
            if(flag):
                list_of_tickers_onmarket.append(ticker)
            else:
                countError = countError + 1
                list_of_tickers_error.append(ticker)
        try:
            db.collection('dados_cia').document(cia['id']).set({'list_of_tickers_onmarket': list_of_tickers_onmarket}, True)
        except:
            print('Ops')
        print(str(tickers) +' -> '+ str(list_of_tickers_onmarket))
    
    print('FINISH - número de erros: ' + str(countError))
    print(str(list_of_tickers_error))

    # allTickers = extractListOfTickers()
    # l = getCrumb()
    # numOfTickers = len(allTickers)
    # count=0
    # for ticker in allTickers[0:10]:
    #     count=count+1
    #     print('        ' + str(count) + '/' + str(numOfTickers) + ' - ' + ticker)
    #     updateDailyDataBase(ticker, l)

