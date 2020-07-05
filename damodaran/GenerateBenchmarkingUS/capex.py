import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)


def extractDataFrom_capex():
    capex = pd.read_excel('./original_files/US'+'/capex.xls', sheet_name='Industry Averages', skiprows=list(range(0,7)))
    return capex