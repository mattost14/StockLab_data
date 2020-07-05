import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)


def extractDataFrom_fundgrEB():
    fundgrEB = pd.read_excel('./original_files/US'+'/fundgrEB.xls', sheet_name='Sheet1', skiprows=list(range(0,7)))
    fundgrEB.drop(columns=['Unnamed: 5'], inplace=True)
    return fundgrEB