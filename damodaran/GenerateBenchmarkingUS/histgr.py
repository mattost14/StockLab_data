import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)


def extractDataFrom_histgr():
    histgr = pd.read_excel('./original_files/US'+'/histgr.xls', sheet_name='Sheet1', skiprows=list(range(0,7)))
    return histgr