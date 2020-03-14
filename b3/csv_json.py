import pandas as pd

arquivo_name = 'setorial'

arquivo_csv = pd.read_csv('./'+ arquivo_name + '.csv')

arquivo_csv.to_json(arquivo_name + '.json', orient='records')