
import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import matplotlib, json
import matplotlib.pyplot as plt
import sys
sys.path.append('/Users/BrunoMattos/Documents2/Dev/stocklab_data/Firestore')
from uploadToFirestore import uploadDocumentToFirestore
import os
#Disable warnings
pd.options.mode.chained_assignment = None

itr_input = './input_cvm/itr/'
dfp_input = './input_cvm/dfp/'


MapaNiveis = {
    'Receita Líquida' : 1,
    'Custos': 2,
    'Lucro Bruto':3,
    'Despesas/Receitas Operacionais':4,
    'EBITDA': 5,
    'Despesas de Depreciação e Amortização': 6,
    'EBIT' : 7,
    'Resultado Financeiro': 8,
        'Receitas Financeiras': 8.1,
        'Despesas Financeiras': 8.2,
    'EBT': 9,
    'Impostos':10,
    'Lucro Líquido': 11,
}
MapNivel2 = {
    'Receitas da Intermediação Financeira' : 'Receita Líquida',
    'Receitas das Operações' : 'Receita Líquida',
    'Receita de Venda de Bens e/ou Serviços': 'Receita Líquida',
    'Despesas da Intermediação Financeira': 'Custos',
    'Custo dos Bens e/ou Serviços Vendidos': 'Custos',
    'Sinistros e Despesas das Operações':'Custos',
    'Resultado Bruto': 'Lucro Bruto',
    'Resultado Bruto Intermediação Financeira': 'Lucro Bruto',
    'Despesas/Receitas Operacionais': 'Despesas/Receitas Operacionais',
    'Despesas Administrativas': 'Despesas/Receitas Operacionais',
    'Outras Despesas/Receitas Operacionais': 'Despesas/Receitas Operacionais',
    'Outras Receitas e Despesas Operacionais': 'Despesas/Receitas Operacionais',
    'Resultado Antes dos Tributos sobre o Lucro': 'EBT',
    'Resultado Antes do Resultado Financeiro e dos Tributos' : 'EBIT',
    'Imposto de Renda e Contribuição Social sobre o Lucro': 'Impostos',
    # 'Resultado de Equivalência Patrimonial' : 'Resultado de Equivalência Patrimonial',
    'Resultado Financeiro' : 'Resultado Financeiro',
    'Lucro/Prejuízo Consolidado do Período' : 'Lucro Líquido',
    # 'Resultado Líquido das Operações Continuadas': 'Lucro Líquido'
}
MapNivel3 = {
    'Receitas Financeiras': 'Receitas Financeiras',
    'Despesas Financeiras': 'Despesas Financeiras',
    'Despesas de Depreciação e Amortização' : 'Amortização/Depreciação'
}

print('STEP1 - Loading data from INPUT_CVM ...')
DRE_ITR_CON = []
path = './input_cvm/itr/itr_cia_aberta_dre_con_'
for year in range(2015,2020,1):
    DRE_ITR_CON = pd.concat([pd.read_csv(path+str(year)+'.csv', thousands=',', sep=';', encoding='latin-1')])

DRE_ITR_IND = []
path = './input_cvm/itr/itr_cia_aberta_dre_ind_'
for year in range(2015,2020,1):
    DRE_ITR_IND = pd.concat([pd.read_csv(path+str(year)+'.csv', thousands=',', sep=';', encoding='latin-1')])

path = './input_cvm/dfp/dre_cia_aberta_con_'
for year in range(2015,2020,1):
    DRE_DFP_CON = pd.concat([pd.read_csv(path+str(year)+'.csv', thousands=',', sep=';', encoding='latin-1')])

path = './input_cvm/dfp/dre_cia_aberta_ind_'
for year in range(2015,2020,1):
    DRE_DFP_IND = pd.concat([pd.read_csv(path+str(year)+'.csv', thousands=',', sep=';', encoding='latin-1')])

DRE=pd.concat([DRE_ITR_CON,DRE_DFP_CON])


#DATA REF
DRE.DT_REFER=pd.to_datetime(DRE.DT_REFER)
#INICIO
DRE.DT_INI_EXERC=pd.to_datetime(DRE.DT_INI_EXERC)
#FIM
DRE.DT_FIM_EXERC=pd.to_datetime(DRE.DT_FIM_EXERC)

#--- Limpando dados incoerentes com os inícios e fins de trimestres ---
#Drop ORDEM_EXERC==PENÚLTIMO
DRE=DRE[DRE.ORDEM_EXERC=='ÚLTIMO']

print('STEP1 - COMPLETED')
print('STEP2 - CREATE COLUMNS: TRIM, YEAR, TRIM_VL, DESC_SIMPLES, ESCALA ...')

def getLevel(row):
    return len(row['CD_CONTA'].split('.'))
DRE['NIVEL']=DRE.apply(lambda r: getLevel(r), axis=1)


DRE_NIVEL1 = DRE[DRE.NIVEL==2]
DRE_NIVEL1['DESC_SIMPLES']=DRE_NIVEL1['DS_CONTA'].map(MapNivel2)
DRE_NIVEL1.dropna(subset=['DESC_SIMPLES'], inplace=True)
DRE_NIVEL1['CD_CONTA']=DRE_NIVEL1['DESC_SIMPLES'].map(MapaNiveis)
#Filtrar para somente NIVEL 3
DRE_NIVEL2 = DRE[DRE.NIVEL==3]
DRE_NIVEL2['DESC_SIMPLES']=DRE_NIVEL2['DS_CONTA'].map(MapNivel3)
DRE_NIVEL2.dropna(subset=['DESC_SIMPLES'], inplace=True)
DRE_NIVEL2['CD_CONTA']=DRE_NIVEL2['DESC_SIMPLES'].map(MapaNiveis)
#Concat Nivel 1 e 2
DRE_NIVEL1e2=pd.concat([DRE_NIVEL1,DRE_NIVEL2])
#Mapeamento da Escala dos Valores
DRE_NIVEL1e2['ESCALA'] = DRE_NIVEL1e2.ESCALA_MOEDA.map({'MIL': 1000, 'MILHAR': 1000, 'UNIDADE': 1})
#Drop colunas desnecessárias
DRE_NIVEL1e2.drop(['DT_REFER', 'VERSAO', 'GRUPO_DFP', 'ESCALA_MOEDA', 'MOEDA', 'ORDEM_EXERC', 'DS_CONTA'], axis=1, inplace=True)

print('STEP2 - COMPLETED')

print('STEP3 - LOOP TROUGH ALL CVM TO GENERATE JSON FILE AS OUTPUT ...')

def getTrim(month):
    if(month==2 or month==3):
        return 1
    elif(month==5 or month==6):
        return 2
    elif(month==8 or month==9):
        return 3
    elif(month==11 or month==12):
        return 4
    else:
        return np.nan

DRE_NIVEL1e2['TRIM'] = DRE_NIVEL1e2.DT_FIM_EXERC.map(lambda d: getTrim(d.month))
DRE_NIVEL1e2=DRE_NIVEL1e2.dropna()
#Create column with YEAR
DRE_NIVEL1e2['YEAR']=DRE_NIVEL1e2.DT_FIM_EXERC.dt.year
#Create DAYS column (diff entre INI e FIM)
DRE_NIVEL1e2['DAYS'] = DRE_NIVEL1e2.apply(lambda x: (x['DT_FIM_EXERC']-x['DT_INI_EXERC']).days , axis=1)


# %%
def calculateAV(r):
    yr = r.YEAR
    trim = r.TRIM
    receita = df[(df.YEAR==yr) & (df.TRIM == trim) & (df.DESC_SIMPLES == 'Receita Líquida')]['TRIM_VL'].values[0]
    valor = r.TRIM_VL
    if receita!=0:
        return valor/receita
    else:
        return np.nan

def calculateMarginTrim(r):
    campo = r.DESC_SIMPLES
    if  campo in ['EBIT', 'Lucro Líquido', 'EBT', 'Lucro Bruto']:
        yr = r.YEAR
        trim = r.TRIM
        receita = df[(df.YEAR==yr) & (df.TRIM == trim) & (df.DESC_SIMPLES == 'Receita Líquida')]['TRIM_VL'].values[0]
        valor = r.TRIM_VL
        if receita!=0:
            return valor/receita
        else:
            return np.nan

def calculateMarginYear(r):
    campo = r.DESC_SIMPLES
    if  campo in ['EBIT', 'Lucro Líquido', 'EBT', 'Lucro Bruto']:
        yr = r.YEAR
        trim = r.TRIM
        receita = df[(df.YEAR==yr) & (df.TRIM == trim) & (df.DESC_SIMPLES == 'Receita Líquida')]['VL_CONTA'].values[0]
        valor = r.VL_CONTA
        if receita!=0:
            return valor/receita
        else:
            return np.nan

def calculateMarginTTM(r):
    campo = r.DESC_SIMPLES
    if  campo in ['EBIT', 'Lucro Líquido', 'EBT', 'Lucro Bruto']:
        yr = r.YEAR
        trim = r.TRIM
        receita = df[(df.YEAR==yr) & (df.TRIM == trim) & (df.DESC_SIMPLES == 'Receita Líquida')]['TTM'].values[0]
        valor = r.TTM
        if receita!=0:
            return valor/receita
        else:
            return np.nan

numberOfCVM = DRE_NIVEL1e2.CD_CVM.nunique()
count=0
for cvm in DRE_NIVEL1e2.CD_CVM.unique()[0:1]:
    cvm=1155
    count=count+1
    print(str(count) + '/' + str(numberOfCVM) + ' - cvm: ' + str(cvm))
    
    df=[]
    df = DRE_NIVEL1e2[(DRE_NIVEL1e2.CD_CVM==cvm)]
    df = df.sort_values(['DESC_SIMPLES','DT_INI_EXERC', 'DT_FIM_EXERC'])
    df.reset_index(inplace=True, drop=True)
    
    #CALCULATE TRIM
    criteria1 = (df.DAYS <=100)
    criteria2 = ((df.DESC_SIMPLES == df.DESC_SIMPLES.shift(1)) & (df.DAYS>100) & (abs(df.YEAR-df.YEAR.shift(1))<=1) & ((df.DAYS-df.DAYS.shift(1))<=100) & ((df.TRIM-df.TRIM.shift(1)).isin([1,-3])))
    TRIM_VL = []
    for i, row in df.iterrows():
        if criteria1[i]:
            TRIM_VL.append(row['VL_CONTA'])
        elif criteria2[i]:
            TRIM_VL.append(df.loc[i]['VL_CONTA']-df.loc[i-1]['VL_CONTA'])
        else:
            TRIM_VL.append(np.nan)
    df['TRIM_VL'] = TRIM_VL
    #Drop DT_FIM_EXERC e DT_INI_EXERC
    df.drop(['DT_FIM_EXERC', 'DT_INI_EXERC', 'DAYS', 'CNPJ_CIA', 'CD_CVM', 'DENOM_CIA'], axis=1, inplace=True)
    
    #Sort para garantir o sequenciamento correto
    df = df.sort_values(['DESC_SIMPLES', 'YEAR','TRIM']).reset_index(drop=True)
    #Calculate Variação entre Trimestre - AH (Análise Horizontal)
    cutTwo = (df.DESC_SIMPLES == df.DESC_SIMPLES.shift(1)) 
    AH = []
    for i, row in df.iterrows():
        if cutTwo[i]:
            if df.iloc[i-1]['TRIM_VL']!=0:
                ah = (df.iloc[i]['TRIM_VL']-df.iloc[i-1]['TRIM_VL'])/df.iloc[i-1]['TRIM_VL']
            else:
                ah = np.nan
            AH.append(ah)
        else:
            AH.append(np.nan)
    df['AH']=pd.Series(AH)
    # Calculate TTM (Soma das últimas 4 rows)
    cutFour = (df.DESC_SIMPLES == df.DESC_SIMPLES.shift(1)) & (df.DESC_SIMPLES == df.DESC_SIMPLES.shift(2)) & (df.DESC_SIMPLES == df.DESC_SIMPLES.shift(3))
    TTM = []
    for i, row in df.iterrows():
        if cutFour[i]:
            TTM.append(sum(df.iloc[i-3:i+1]['TRIM_VL'].values))
        else:
            TTM.append(np.nan)
    df['TTM']=pd.Series(TTM)
    #Sort novamente
    df = df.sort_values(['YEAR','TRIM','CD_CONTA']).reset_index(drop=True)
    #Cálculo da Variação Vertical no Trimestre - AV (Análise Vertical)
    df['AV'] = df.apply(lambda r: calculateAV(r), axis=1)
    #Cálculo das Margins
    df['MARGEM_TRIM']=df.apply(lambda r: calculateMarginTrim(r), axis=1)
    df['MARGEM_ANO']=df.apply(lambda r: calculateMarginYear(r), axis=1)
    df['MARGEM_TTM']=df.apply(lambda r: calculateMarginTTM(r), axis=1)
    #Adicionando o TRIM_ANO como coluna
    df['TRIM_ANO']=df.apply(lambda r: "%dT%d" %(r['TRIM'],r['YEAR']), axis=1)
    print(df)
    #Gravando arquivo
    data = {"data" : df.to_dict('records')}
    data = {
        "cvm": int(cvm), 
        "data" : data,
    }
    with open('./output_cvm/dre/'+str(cvm)+'.json', 'w') as outfile:
        json.dump(data, outfile, indent=2)
        print(' - ' + str(cvm)+'.json gravado.' )

print('STEP3 - COMPLETED')


