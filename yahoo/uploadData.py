import sys, os
import pandas as pd
import datetime as dt
sys.path.append('Firestore')
from uploadToFirestore import uploadDocumentFromJsonData

### UPLOAD DOCUMENTS TO FIRESTORE ####
path_to_output_historical = 'yahoo/output/historical/'
path_to_output_dividends = 'yahoo/output/dividends/'


def uploadQuotes(df, ticker, target):
    print('Uploading '+ target + ' of ' + ticker + ' to Firestore:')
    #Get last 1 week
    delta = dt.datetime.today() - df.Date
    delta = delta.map(lambda d : d.days) 
    indexRows = (delta <= 7)
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
    print(' - Last 1 week...')
    uploadDocumentFromJsonData(target, jsonData, 'last1w/'+ticker+'/data')
    
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
    print(' - Last month...')
    uploadDocumentFromJsonData(target, jsonData, 'last1m/'+ticker+'/data')

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
    print(' - Last 6 months...')
    uploadDocumentFromJsonData(target, jsonData, 'last6m/'+ticker+'/data')
    
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
    print(' - Last 1 year...')
    uploadDocumentFromJsonData(target, jsonData, 'last1yr/'+ticker+'/data')

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
    print(' - Last 2 years...')
    uploadDocumentFromJsonData(target, jsonData, 'last2yr/'+ticker+'/data')

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
    print(' - Last 5 years...')
    uploadDocumentFromJsonData(target, jsonData, 'last5yr/'+ticker+'/data')

def uploadDividends(df, ticker, target):
    if(df.empty==False):
        jsonData = {
            'ticker': ticker,
            'data': df.to_dict('records')
            }
    else: 
        jsonData = {
            'ticker': ticker,
            'data': []
        }
    uploadDocumentFromJsonData(target, jsonData, ticker)

def uploadTicker(ticker, target):
    if(target == 'quotes'):
        path = path_to_output_historical
    elif(target == 'dividends'):
        path = path_to_output_dividends
    file = ticker + '.csv'
    df = pd.read_csv(path+file)
    if(df.empty == False):
        ticker = file.split('.')[0]
        df.Date=pd.to_datetime(df.Date)
        if target == 'quotes':
            df=df[['Date','Adj Close','Volume']]
            #Drop row if Volume=0
            indexRows = df[ df['Volume'] == 0 ].index
            df.drop(indexRows , inplace=True)
        else:
            df=df[['Date','Dividends']]
        df.sort_values('Date', ascending=False,inplace=True,)
        df.dropna(inplace=True)


        df.reset_index(inplace=True, drop=True)
        
        if(target == 'quotes'):
            uploadQuotes(df, ticker, target)
        else:
            uploadDividends(df, ticker, target)
        
    else:
        print('- No data found.')

if __name__ == "__main__":
    # target = 'dividends'
    print('Start uploading to Firestore ...')
    target = 'quotes'
    # if(target == 'quotes'):
    #     path = path_to_output_historical
    # elif(target == 'dividends'):
    #     path = path_to_output_dividends

    csv_files = [pos_csv for pos_csv in os.listdir(path) if pos_csv.endswith('.csv')]
    for file in csv_files:
        ticker = file.split('.')[0]
        uploadTicker(ticker, target)
        # if(file == 'AALR3.csv'):
            # df = pd.read_csv(path+file)
            # if(df.empty == False):
            #     ticker = file.split('.')[0]
            #     df.Date=pd.to_datetime(df.Date)
            #     df=df[['Date','Adj Close','Volume']]
            #     df.sort_values('Date', ascending=False,inplace=True,)
            #     df.dropna(inplace=True)
            #     #Drop row if Volume=0
            #     indexRows = df[ df['Volume'] == 0 ].index
            #     df.drop(indexRows , inplace=True)

            #     df.reset_index(inplace=True, drop=True)
            #     print('Uploading '+ target + ' of ' + ticker + ' to Firestore:')

            #     #Get last 1 week
            #     delta = dt.datetime.today() - df.Date
            #     delta = delta.map(lambda d : d.days) 
            #     indexRows = (delta <= 7)
            #     data = df[indexRows]
            #     if(data.empty==False):
            #         jsonData = {
            #             'ticker': ticker,
            #             'data': data.to_dict('records')
            #             }
            #     else: 
            #         jsonData = {
            #             'ticker': ticker,
            #             'data': []
            #         }
            #     print(' - Last 1 week...')
            #     uploadDocumentFromJsonData(target, jsonData, 'last1w/'+ticker+'/data')
                
            #     #Get last 1 month
            #     delta = dt.datetime.today() - df.Date
            #     delta = delta.map(lambda d : d.days) 
            #     indexRows = (delta <= 30)
            #     data = df[indexRows]
            #     if(data.empty==False):
            #         jsonData = {
            #             'ticker': ticker,
            #             'data': data.to_dict('records')
            #             }
            #     else: 
            #         jsonData = {
            #             'ticker': ticker,
            #             'data': []
            #         }
            #     print(' - Last month...')
            #     uploadDocumentFromJsonData(target, jsonData, 'last1m/'+ticker+'/data')

            #     #Get last 6 months
            #     delta = dt.datetime.today() - df.Date
            #     delta = delta.map(lambda d : d.days) 
            #     indexRows = (delta <= 30*6)
            #     data = df[indexRows]
            #     if(data.empty==False):
            #         jsonData = {
            #             'ticker': ticker,
            #             'data': data.to_dict('records')
            #             }
            #     else: 
            #         jsonData = {
            #             'ticker': ticker,
            #             'data': []
            #         }
            #     print(' - Last 6 months...')
            #     uploadDocumentFromJsonData(target, jsonData, 'last6m/'+ticker+'/data')
                
            #     #Get last 1 year
            #     delta = dt.datetime.today() - df.Date
            #     delta = delta.map(lambda d : d.days) 
            #     indexRows = (delta <= 30*12)
            #     data = df[indexRows]
            #     if(data.empty==False):
            #         jsonData = {
            #             'ticker': ticker,
            #             'data': data.to_dict('records')
            #             }
            #     else: 
            #         jsonData = {
            #             'ticker': ticker,
            #             'data': []
            #         }
            #     print(' - Last 1 year...')
            #     uploadDocumentFromJsonData(target, jsonData, 'last1yr/'+ticker+'/data')

            #     #Get last 2 years
            #     delta = dt.datetime.today() - df.Date
            #     delta = delta.map(lambda d : d.days) 
            #     indexRows = (delta <= 30*12*2)
            #     data = df[indexRows]
            #     if(data.empty==False):
            #         jsonData = {
            #             'ticker': ticker,
            #             'data': data.to_dict('records')
            #             }
            #     else: 
            #         jsonData = {
            #             'ticker': ticker,
            #             'data': []
            #         }
            #     print(' - Last 2 years...')
            #     uploadDocumentFromJsonData(target, jsonData, 'last2yr/'+ticker+'/data')

            #     #Get last 5 years
            #     delta = dt.datetime.today() - df.Date
            #     delta = delta.map(lambda d : d.days) 
            #     indexRows = (delta <= 30*12*5)
            #     data = df[indexRows]
            #     if(data.empty==False):
            #         jsonData = {
            #             'ticker': ticker,
            #             'data': data.to_dict('records')
            #             }
            #     else: 
            #         jsonData = {
            #             'ticker': ticker,
            #             'data': []
            #         }
            #     print(' - Last 5 years...')
            #     uploadDocumentFromJsonData(target, jsonData, 'last5yr/'+ticker+'/data')
                
            # else:
            #     print('- No data found.')


