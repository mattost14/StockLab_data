import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)


def extractDataFrom_margin():
    margin = pd.read_excel('./original_files/US'+'/margin.xls', sheet_name='Sheet1', skiprows=list(range(0,8)))
    return margin