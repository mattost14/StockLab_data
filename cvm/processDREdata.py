# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'

# %%
# This Python 3 environment comes with many helpful analytics libraries installed
# It is defined by the kaggle/python docker image: https://github.com/kaggle/docker-python
# For example, here's several helpful packages to load in 

import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import matplotlib, json
import matplotlib.pyplot as plt

#Disable warnings
pd.options.mode.chained_assignment = None

# import uploadToFirestore.uploadToFirestore             
#import seaborn as sns
#sns.set(rc={'figure.figsize':(15,10),"font.size":16,"axes.titlesize":16,"axes.labelsize":16})

# Input data files are available in the "../input/" directory.
# For example, running this (by clicking run or pressing Shift+Enter) will list the files in the input directory

import os
#print(os.listdir("./dre_input"))
itr_input = './input_cvm/itr/'
dfp_input = './input_cvm/dfp/'

# Any results you write to the current directory are saved as output.


# %%
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


# %%
print('STEP1 - Loading data from INPUT_CVM ...')
DRE_ITR2015 = pd.read_csv("./input_cvm/itr/itr_cia_aberta_dre_con_2015.csv", thousands=',', sep=';', encoding='latin-1')
DRE_ITR2016 = pd.read_csv("./input_cvm/itr/itr_cia_aberta_dre_con_2016.csv", thousands=',', sep=';', encoding='latin-1')
DRE_ITR2017 = pd.read_csv("./input_cvm/itr/itr_cia_aberta_dre_con_2017.csv", thousands=',', sep=';', encoding='latin-1')
DRE_ITR2018 = pd.read_csv("./input_cvm/itr/itr_cia_aberta_dre_con_2018.csv", thousands=',', sep=';', encoding='latin-1')
DRE_ITR2019 = pd.read_csv("./input_cvm/itr/itr_cia_aberta_dre_con_2019.csv", thousands=',', sep=';', encoding='latin-1')
#Concat todos
DRE_ITR=pd.concat([DRE_ITR2015,DRE_ITR2016,DRE_ITR2017,DRE_ITR2018,DRE_ITR2019])
#Carregando dados padronizados (DFP) de cada ano
DRE_DFP2015 = pd.read_csv("./input_cvm/dfp/dre_cia_aberta_con_2015.csv", thousands=',', sep=';', encoding='latin-1')
DRE_DFP2016 = pd.read_csv("./input_cvm/dfp/dre_cia_aberta_con_2016.csv", thousands=',', sep=';', encoding='latin-1')
DRE_DFP2017 = pd.read_csv("./input_cvm/dfp/dre_cia_aberta_con_2017.csv", thousands=',', sep=';', encoding='latin-1')
DRE_DFP2018 = pd.read_csv("./input_cvm/dfp/dre_cia_aberta_con_2018.csv", thousands=',', sep=';', encoding='latin-1')
DRE_DFP2019 = pd.read_csv("./input_cvm/dfp/dre_cia_aberta_con_2019.csv", thousands=',', sep=';', encoding='latin-1')
#Concat todos
DRE_DFP=pd.concat([DRE_DFP2015,DRE_DFP2016,DRE_DFP2017,DRE_DFP2018,DRE_DFP2019])
#Concat DRE_ITR e DRE_DFP
DRE=pd.concat([DRE_ITR,DRE_DFP])
#DATA REF
DRE.DT_REFER=pd.to_datetime(DRE.DT_REFER)
#INICIO
DRE.DT_INI_EXERC=pd.to_datetime(DRE.DT_INI_EXERC)
#FIM
DRE.DT_FIM_EXERC=pd.to_datetime(DRE.DT_FIM_EXERC)
#--- Limpando dados incoerentes com os inícios e fins de trimestres ---
#Drop ORDEM_EXERC==PENÚLTIMO
DRE=DRE[DRE.ORDEM_EXERC=='ÚLTIMO']
#Drop Dados com DATA de INÍCIO diferente de JAN
DRE=DRE[DRE.DT_INI_EXERC.dt.month==1]
#Drop Dados com DATA de FIM diferente de MAR(3),JUN(6),SEP(9),DEC(12) 
DRE=DRE[DRE.DT_FIM_EXERC.dt.month.isin([3,6,9,12])]

print('STEP1 - COMPLETED')

# %% [markdown]
# Definido o trimestre correspondente de cada dado

# %%
print('STEP2 - CREATE COLUMNS: TRIM, YEAR, TRIM_VL, DESC_SIMPLES, ESCALA ...')
def getTrim (di,df):
    n=(df-di)
    if (n.days<95 and n.days>85):
        return 1
    elif n.days<190 and n.days>170:
        return 2
    elif n.days<275 and n.days>265:
        return 3
    elif n.days<366 and n.days>360:
        return 4
    else:
        return np.nan
    

DRE['TRIM']=DRE.apply(lambda x: getTrim(x['DT_INI_EXERC'], x['DT_FIM_EXERC']), axis=1)
DRE=DRE.dropna()

#Create column with YEAR
DRE['YEAR']=DRE.DT_FIM_EXERC.dt.year

#Adicionar column do nível do dado conforme columna CD_CONTA. Ex.: '3.04.02' -> nível 3
def getLevel(row):
    return len(row['CD_CONTA'].split('.'))
DRE['NIVEL']=DRE.apply(lambda r: getLevel(r), axis=1)
DRE=DRE.sort_values(['CD_CVM', 'DS_CONTA', 'NIVEL', 'YEAR','TRIM'])
DRE=DRE.reset_index(drop=True)


# %%
#Operação de substração de linhas sucessoras
DRE['TRIM_VL']=DRE.VL_CONTA-DRE.VL_CONTA.shift(+1)
#Checando coerência da operação anterior
DRE['CHECK']=(DRE.DS_CONTA==DRE.DS_CONTA.shift(+1)) & (DRE.CD_CVM==DRE.CD_CVM.shift(+1)) & (DRE.YEAR==DRE.YEAR.shift(+1)) & (DRE.TRIM-DRE.TRIM.shift(+1)==1)

checkFalseIndex = DRE[(DRE['CHECK']==False) & (DRE['TRIM']==1)].index
DRE.loc[checkFalseIndex, 'VL_CONTA']
DRE.loc[checkFalseIndex, 'TRIM_VL']= DRE.loc[checkFalseIndex, 'VL_CONTA']
DRE.loc[checkFalseIndex, 'CHECK'] = DRE.loc[checkFalseIndex, 'CHECK'].map({False: True})

checkFalseIndex2 = DRE[(DRE['CHECK']==False) & (DRE['TRIM']!=1)].index
DRE.loc[checkFalseIndex2, 'TRIM_VL'] = pd.np.nan


# %%
#Filtrar para somente NIVEL 2
DRE_NIVEL1 = DRE[DRE.NIVEL==2]
DRE_NIVEL1['DESC_SIMPLES']=DRE_NIVEL1['DS_CONTA'].map(MapNivel2)
DRE_NIVEL1.dropna(subset=['DESC_SIMPLES'], inplace=True)
DRE_NIVEL1['CD_CONTA']=DRE_NIVEL1['DESC_SIMPLES'].map(MapaNiveis)
#Filtrar para somente NIVEL 3
DRE_NIVEL2 = DRE[DRE.NIVEL==3]
DRE_NIVEL2['DESC_SIMPLES']=DRE_NIVEL2['DS_CONTA'].map(MapNivel3)
DRE_NIVEL2.dropna(subset=['DESC_SIMPLES'], inplace=True)
DRE_NIVEL2['CD_CONTA']=DRE_NIVEL2['DESC_SIMPLES'].map(MapaNiveis)

#
DRE_NIVEL1e2=pd.concat([DRE_NIVEL1,DRE_NIVEL2])
#Mapeamento da Escala dos Valores
DRE_NIVEL1e2['ESCALA'] = DRE_NIVEL1e2.ESCALA_MOEDA.map({'MIL': 1000, 'MILHAR': 1000, 'UNIDADE': 1})

print('STEP2 - COMPLETED')

print('STEP3 - LOOP TROUGH ALL CVM TO GENERATE JSON FILE AS OUTPUT ...')
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
for cvm in DRE_NIVEL1e2.CD_CVM.unique():
    count=count+1
    print(str(count) + '/' + str(numberOfCVM) + ' - cvm: ' + str(cvm))
    df = DRE_NIVEL1e2[DRE_NIVEL1e2.CD_CVM==cvm].groupby(['YEAR','TRIM','DESC_SIMPLES','CD_CONTA'])['TRIM_VL', 'VL_CONTA', 'NIVEL','ESCALA'].sum().reset_index()

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
    df['MARGIN_TRIM']=df.apply(lambda r: calculateMarginTrim(r), axis=1)
    df['MARGIN_ANO']=df.apply(lambda r: calculateMarginYear(r), axis=1)
    df['MARGIN_TTM']=df.apply(lambda r: calculateMarginTTM(r), axis=1)
    #Adicionando o TRIM_ANO como coluna
    df['TRIM_ANO']=df.apply(lambda r: "%dT%d" %(r['TRIM'],r['YEAR']), axis=1)
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
