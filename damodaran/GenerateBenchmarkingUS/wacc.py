import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)

#INPUTS CONSTANTS 
longTermTreasuryBondRate = 0.0192
ERP = 0.0695
GlobalDefaultSpread = .008
MarginalTaxRate = 0.2616
ExpectedInflationAtREAL = 0.035
ExpectedInflationAtDolar = 0.015

calculatedColumns = [
    'Cost of Equity',
    'Cost of Debt',
    'After-tax Cost of Debt',
    'Cost of Capital',
    'Cost of Capital (Local Currency)'
]

def extractDataFrom_wacc():
    wacc = pd.read_excel('./original_files/US'+'/wacc.xls', sheet_name='Industry Averages', skiprows=list(range(0,18)))

    wacc.drop(columns=calculatedColumns, inplace=True)

    wacc['Cost of Equity'] = wacc.apply(lambda row: longTermTreasuryBondRate + row.Beta*ERP, axis=1)

    wacc['ERP'] = wacc.apply(lambda row: row.Beta*ERP, axis=1)

    def stdLookUpTable(std):
        if std <=.25: return .008
        elif std <=.4: return .0135
        elif std <=.65: return .0175
        elif std <= .75: return .025
        elif std <=.9: return .05
        elif std <= 1: return .065
        else: return 0.075

    wacc['Cost of Debt']=wacc.apply(lambda row: stdLookUpTable(row['Std Dev in Stock']) + longTermTreasuryBondRate + GlobalDefaultSpread, axis=1)

    wacc['After-tax Cost of Debt'] = wacc.apply(lambda row: row['Cost of Debt']*(1-MarginalTaxRate), axis=1)

    wacc['Cost of Capital'] = wacc.apply(lambda row: row['Cost of Equity']*row['E/(D+E)'] + row['After-tax Cost of Debt']*row['D/(D+E)'], axis=1)

    wacc['Cost of Capital - Reais']=  wacc.apply(lambda row: (row['Cost of Capital'] + 1)*(1+ ExpectedInflationAtREAL)/(1+ ExpectedInflationAtDolar) -1, axis=1)

    return wacc