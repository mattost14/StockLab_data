import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)


def extractDataFrom_indname():
    indname = pd.read_excel('./original_files/US'+'/indname.xls', sheet_name='US', skiprows=list(range(0,9)))
    return indname