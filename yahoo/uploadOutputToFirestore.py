import sys, os
import pandas as pd
import datetime as dt
sys.path.append('/Users/BrunoMattos/Documents2/Dev/stocklab_data/Firestore')
from uploadToFirestore import uploadDocumentFromJsonData

### UPLOAD DOCUMENTS TO FIRESTORE ####
path_to_output_historical = './output/historical/'
path_to_output_dividends = './output/dividends/'


if __name__ == "__main__":
    # target = 'dividends'
    target = 'quotes'
    if(target == 'quotes'):
        path = path_to_output_historical
    elif(target == 'dividends'):
        path = path_to_output_dividends

    csv_files = [pos_csv for pos_csv in os.listdir(path) if pos_csv.endswith('.csv')]
    for file in csv_files:
        # if(file == 'BALM3.csv'):
            df = pd.read_csv(path+file)
            if(df.empty == False):
                ticker = file.split('.')[0]
                df.Date=pd.to_datetime(df.Date)
                df=df[['Date','Adj Close','Volume']]
                df.sort_values('Date', ascending=False,inplace=True,)
                df.dropna(inplace=True)
                #Drop row if Volume=0
                indexRows = df[ df['Volume'] == 0 ].index
                df.drop(indexRows , inplace=True)

                df.reset_index(inplace=True, drop=True)
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
                
            else:
                print('- No data found.')


