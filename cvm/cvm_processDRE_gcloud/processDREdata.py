
import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import matplotlib, json
import matplotlib.pyplot as plt
# import sys
# sys.path.append('/Users/BrunoMattos/Documents2/Dev/stocklab_data/Firestore')
# from uploadToFirestore import uploadDocumentToFirestore
import os
#Disable warnings
pd.options.mode.chained_assignment = None

itr_input = './input_cvm/itr/'
dfp_input = './input_cvm/dfp/'

#All cvm not included on this list will be ignored
listOfCVM = pd.read_csv('input_cvm/listOfCVM.csv')
listOfCVM=listOfCVM.CVM.values

yearStart = 2015
yearEnd = 2020
# print(os.listdir("./input_cvm/itr"))

# %%
MapaNiveis = {
    'Receita Líquida' : 1,
    'Custos': 2,
    'Lucro Bruto':3,
    'Despesas Administrativas':4,
        'Despesas com Vendas': 4.1,
        'Despesas Gerais e Administrativas': 4.2,
    'Despesas/Receitas Operacionais':5,
    'Resultado de Equivalência Patrimonial':6,
    'EBIT' : 7,
    'Resultado Operacional':8,
    'Resultado Não Operacional':9,
    'Resultado Financeiro': 10,
        'Receitas Financeiras': 10.1,
        'Despesas Financeiras': 10.2,
    'EBT': 11,
    'Impostos':12,
    'Lucro Líquido': 13,
}

MapNivel2 = {
    #1
    'Receitas da Intermediação Financeira' : 'Receita Líquida', #Bancos (Ex.: ITAU)
    'Receitas das Operações' : 'Receita Líquida', #Seguradoras (Ex.:BBSE)
    'Receita de Venda de Bens e/ou Serviços': 'Receita Líquida', #Geral (Ex.: Vale)
    #2
    'Despesas da Intermediação Financeira': 'Custos', #Bancos (Ex.: ITAU)
    'Sinistros e Despesas das Operações':'Custos', #Seguradoras (Ex.:BBSE)
    'Custo dos Bens e/ou Serviços Vendidos': 'Custos', #Geral (Ex.: Vale)
    #3
    'Resultado Bruto': 'Lucro Bruto', #Geral (Ex.: Vale) e Seguradoras (Ex.:BBSE)
    'Resultado Bruto Intermediação Financeira': 'Lucro Bruto', #Bancos (Ex.: ITAU)
    #4
    'Despesas Administrativas': 'Despesas Administrativas', #Seguradoras (Ex.:BBSE)
    #5
    'Despesas/Receitas Operacionais': 'Despesas/Receitas Operacionais', #Geral (Ex.: Vale)
    'Outras Despesas/Receitas Operacionais': 'Despesas/Receitas Operacionais', #Bancos (Ex.: ITAU)
    'Outras Receitas e Despesas Operacionais': 'Despesas/Receitas Operacionais', #Seguradoras (Ex.:BBSE)
    #6
    'Resultado de Equivalência Patrimonial':'Resultado de Equivalência Patrimonial',#Seguradoras (Ex.:BBSE)
    #7
    'Resultado Antes do Resultado Financeiro e dos Tributos' : 'EBIT', #Geral (Ex.: Vale) e Seguradoras (Ex.:BBSE)
    #8
    'Resultado Operacional': 'Resultado Operacional', #Bancos (Ex.: ITAU) (Individual)
    #9
    'Resultado Não Operacional': 'Resultado Não Operacional', #Bancos (Ex.: ITAU) (Individual)
    #10
    'Resultado Financeiro' : 'Resultado Financeiro', #Geral (Ex.: Vale) e Seguradoras (Ex.:BBSE)
    #11
    'Resultado Antes dos Tributos sobre o Lucro': 'EBT', #Geral (Ex.: Vale) e Seguradoras (Ex.:BBSE) e Bancos (Ex.: ITAU) (Consolidado)
    'Resultado Antes Tributação/Participações':'EBT', #Bancos (Ex.: ITAU) (Individual)
    #12
    'Imposto de Renda e Contribuição Social sobre o Lucro': 'Impostos', ##Geral (Ex.: Vale) e Seguradoras (Ex.:BBSE) e Bancos (Ex.: ITAU) (Consolidado)
    'Provisão para IR e Contribuição Social': 'Impostos', #Bancos (Ex.: ITAU) (Individual)
    #13
    'Lucro/Prejuízo Consolidado do Período' : 'Lucro Líquido', #Geral (Ex.: Vale) e Seguradoras (Ex.:BBSE) e Bancos (Ex.: ITAU) 
    'Lucro/Prejuízo do Período':'Lucro Líquido', #Geral (Ex.: Vale) e Seguradoras (Ex.:BBSE) e Bancos (Ex.: ITAU) 
}
MapNivel3 = {
    'Despesas com Vendas': 'Despesas com Vendas',
    'Despesas Gerais e Administrativas' : 'Despesas Gerais e Administrativas',
    'Receitas Financeiras': 'Receitas Financeiras',
    'Despesas Financeiras': 'Despesas Financeiras',
    #6
    'Despesas de Depreciação e Amortização' : 'Amortização/Depreciação'
}
MapGrupo = {
    'DF Individual - Demonstração do Resultado': 'Individual',
    'DF Consolidado - Demonstração do Resultado': 'Consolidado'

}
# %%
DRE_ITR_CON = pd.DataFrame()
path = './input_cvm/itr/itr_cia_aberta_dre_con_'
for year in range(yearStart,yearEnd+1,1):
    if os.path.exists(path+str(year)+'.csv'):
        DRE_ITR_CON = pd.concat([DRE_ITR_CON, pd.read_csv(path+str(year)+'.csv', thousands=',', sep=';', encoding='latin-1')])

DRE_ITR_IND = pd.DataFrame()
path = './input_cvm/itr/itr_cia_aberta_dre_ind_'
for year in range(yearStart,yearEnd+1,1):
    if os.path.exists(path+str(year)+'.csv'):
        DRE_ITR_IND = pd.concat([DRE_ITR_IND, pd.read_csv(path+str(year)+'.csv', thousands=',', sep=';', encoding='latin-1')])

DRE_DFP_CON = pd.DataFrame()
path = './input_cvm/dfp/dre_cia_aberta_con_'
for year in range(yearStart,yearEnd+1,1):
    if os.path.exists(path+str(year)+'.csv'):
        DRE_DFP_CON = pd.concat([DRE_DFP_CON, pd.read_csv(path+str(year)+'.csv', thousands=',', sep=';', encoding='latin-1')])

DRE_DFP_IND = pd.DataFrame()
path = './input_cvm/dfp/dre_cia_aberta_ind_'
for year in range(yearStart,yearEnd+1,1):
    if os.path.exists(path+str(year)+'.csv'):
        DRE_DFP_IND = pd.concat([DRE_DFP_IND, pd.read_csv(path+str(year)+'.csv', thousands=',', sep=';', encoding='latin-1')])

DRE=pd.concat([DRE_ITR_CON,DRE_DFP_CON,DRE_ITR_IND,DRE_DFP_IND])
#Apply filter by listOfCVM
DRE = DRE[DRE.CD_CVM.isin(listOfCVM)]

# %%
#Adicionar column do nível do dado conforme columna CD_CONTA. Ex.: '3.04.02' -> nível 3
def getLevel(row):
    return len(row['CD_CONTA'].split('.'))
DRE['NIVEL']=DRE.apply(lambda r: getLevel(r), axis=1)
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
#Concat Nivel 1 e 2
DRE=pd.concat([DRE_NIVEL1,DRE_NIVEL2])
#Map Grupo (Individual, Consolidado)
DRE['GRUPO_DFP']=DRE['GRUPO_DFP'].map(MapGrupo)
#Mapeamento da Escala dos Valores
DRE['ESCALA'] = DRE.ESCALA_MOEDA.map({'MIL': 1000, 'MILHAR': 1000, 'UNIDADE': 1})
#INICIO
DRE.DT_INI_EXERC=pd.to_datetime(DRE.DT_INI_EXERC)
#FIM
DRE.DT_FIM_EXERC=pd.to_datetime(DRE.DT_FIM_EXERC)
#DATA DE REF
DRE.DT_REFER=pd.to_datetime(DRE.DT_REFER)

#Drop colunas desnecessárias
DRE.drop(['VERSAO', 'ESCALA_MOEDA', 'MOEDA', 'DS_CONTA'], axis=1, inplace=True)

ListOfCias = DRE[['CD_CVM', 'DENOM_CIA']].drop_duplicates('CD_CVM')
ListOfCias.sort_values('DENOM_CIA', inplace=True)
ListOfCias.reset_index(inplace=True, drop=True)
ListOfCias.to_csv('./output_cvm/list_of_cias.csv')
print('- Saved list_of_cias.csv')

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

DRE['TRIM'] = DRE.DT_FIM_EXERC.map(lambda d: getTrim(d.month))
DRE.dropna(inplace=True)
#Create column with YEAR
DRE['YEAR']=DRE.DT_FIM_EXERC.dt.year
#Create DAYS column (diff entre INI e FIM)
DRE['DAYS'] = DRE.apply(lambda x: (x['DT_FIM_EXERC']-x['DT_INI_EXERC']).days , axis=1)

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

def isMostUpdated(r, dataFrame):
    sameDataTable = dataFrame[(dataFrame.CD_CVM == r.CD_CVM)&(dataFrame.GRUPO_DFP == r.GRUPO_DFP) & (dataFrame.DESC_SIMPLES==r.DESC_SIMPLES) & (dataFrame.YEAR == r.YEAR) & (dataFrame.TRIM == r.TRIM) & (dataFrame.DAYS == r.DAYS)]
    if(len(sameDataTable)==1):
        return True
    else: 
        dataRefs = sameDataTable.DT_REFER.sort_values(ascending=False)
    return (r.DT_REFER - dataRefs.values[0]).days == 0

def normalizeScale(row):
    if row.ESCALA == 1:
        return row.VL_CONTA/1000
    else:
        return row.VL_CONTA

DRE.VL_CONTA = DRE.apply(lambda row: normalizeScale(row), axis =1 )
DRE.ESCALA = 1000

numberOfCVM = DRE.CD_CVM.nunique()
count=0

for cvm in DRE.CD_CVM.unique():
    count=count+1
    print(str(count) + '/' + str(numberOfCVM) + ' - cvm: ' + str(cvm))
    # cvm=22020
    DF = DRE[(DRE.CD_CVM==cvm)]
    for grupo in ['Individual', 'Consolidado']:
        print('Grupo: ' + grupo)
        df = DF[DF.GRUPO_DFP == grupo]
        if(len(df)):
            #Filter to only show mostUpdated rows
            df['isMostUpdated']=df.apply(lambda r: isMostUpdated(r,df),axis=1)
            df = df[df.isMostUpdated == True]

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
            df.drop(['DT_FIM_EXERC', 'DT_INI_EXERC', 'DAYS', 'CNPJ_CIA', 'DT_REFER', 'isMostUpdated'], axis=1, inplace=True)
            
            #Sort para garantir o sequenciamento correto
            df = df.sort_values(['DESC_SIMPLES', 'YEAR','TRIM']).reset_index(drop=True)

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

            for ano in df.YEAR.unique():
                #Gravando arquivo
                if(len(df[df.YEAR==ano])):
                    data = df[df.YEAR==ano].to_dict('records')
                else:
                    data = []
                
                data = {
                    "cvm": int(cvm),
                    "ano": int(ano),
                    "grupo": grupo,
                    "listOfTrims": df[df.YEAR==ano].TRIM.unique().tolist(), 
                    "data" : data,
                }
                path = './output_cvm/dre/'+str(cvm)+'/'+grupo+'/'
                if not os.path.exists(path):
                    os.makedirs(path)
                with open(path+str(ano)+'.json', 'w') as outfile:
                    json.dump(data, outfile, indent=2)
                    print('  '+ str(cvm) +  ' - ' + grupo +' - ' + str(ano)+'.json gravado - ' + str(df[df.YEAR==ano].TRIM.unique()))#str(df[df.YEAR==ano].size))
        else:
            print('  ' + str(cvm) +  ' - ' + grupo +' - ' + 'EMPTY')