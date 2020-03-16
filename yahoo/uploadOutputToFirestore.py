import sys, os
import pandas as pd
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
        df = pd.read_csv(path+'AMAR3.csv')
        if(df.empty == False):
            ticker = file.split('.')[0]
            df.Date=pd.to_datetime(df.Date)
            df=df[['Date','Adj Close','Volume']]
            df.sort_values('Date', ascending=False,inplace=True,)
            df.reset_index(inplace=True, drop=True)
            print('Uploading '+ target + ' of ' + ticker + ' to Firestore:')
            #Get last 1 month 
            data = df.head(20)
            if(data.empty==False):
                jsonData = {
                    'data': data.to_dict('records')
                    }
                print(' - Last month...')
                uploadDocumentFromJsonData(target, jsonData, 'last1m/'+ticker+'/data')
            #Get last 6 months
            data = df.head(20*6)
            if(data.empty==False):
                jsonData = {
                    'data': data.to_dict('records')
                    }
                print(' - Last 6 months...')
                uploadDocumentFromJsonData(target, jsonData, 'last6m/'+ticker+'/data')
            #Get last 1 year
            data = df.head(20*12)
            if(data.empty==False):
                jsonData = {
                    'data': data.to_dict('records')
                    }
                print(' - Last 1yr...')
                uploadDocumentFromJsonData(target, jsonData, 'last1yr/'+ticker+'/data')
            #Get last 5 year
            data = df.head(20*12*5)
            if(data.empty==False):
                jsonData = {
                    'data': data.to_dict('records')
                    }
                print(' - Last 5yrs...')
                uploadDocumentFromJsonData(target, jsonData, 'last5yr/'+ticker+'/data')
        else:
            print('- No data found.')


