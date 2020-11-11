import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
from firebase import client, db
import io
from RowMapNames import MapNivel2, MapNivel3, MapaNiveis, MapGrupo
#Disable warnings
pd.options.mode.chained_assignment = None

bucket = client.get_bucket('stocklab-8255c.appspot.com')

def loadDREdata(year):
    DRE_ITR_CON = pd.DataFrame()
    DRE_ITR_IND = pd.DataFrame()
    DRE_DFP_CON = pd.DataFrame()
    DRE_DFP_IND = pd.DataFrame()
    #Load DRE CONSOLIDADO from ITR files
    try:
        path = 'cvm/inputs/itr_cia_aberta_DRE_con_'+str(year)+'.csv'
        blob = bucket.blob(path)
        if(blob!=None):
            content = blob.download_as_string()
            DRE_ITR_CON = pd.read_csv(io.BytesIO(content), thousands=',', sep=';', encoding='latin-1')
    except:
        print(f'Fatal Error: {path} not found.')
        return
        
    #Load DRE INDIVIDUAL from ITR files
    try:
        path = 'cvm/inputs/itr_cia_aberta_DRE_ind_'+str(year)+'.csv'
        blob = bucket.blob(path)
        if(blob!=None):
            content = blob.download_as_string()
            DRE_ITR_IND = pd.read_csv(io.BytesIO(content), thousands=',', sep=';', encoding='latin-1')
    except:
        print(f'Fatal Error: {path} not found.')
        return
        
    #Load DRE CONSOLIDADO from DFP files
    try:
        path = 'cvm/inputs/dre_cia_aberta_con_'+str(year)+'.csv'
        blob = bucket.blob(path)
        if(blob!=None):
            content = blob.download_as_string()
            DRE_DFP_CON = pd.read_csv(io.BytesIO(content), thousands=',', sep=';', encoding='latin-1')
    except:
        print(f'Fatal Error: {path} not found.')
        return
        
    #Load DRE INDIVIDUAL from DFP files
    try:
        path = 'cvm/inputs/dre_cia_aberta_ind_'+str(year)+'.csv'
        blob = bucket.blob(path)
        if(blob!=None):
            content = blob.download_as_string()
            DRE_DFP_IND = pd.read_csv(io.BytesIO(content), thousands=',', sep=';', encoding='latin-1')
    except:
        print(f'Fatal Error: {path} not found.')
        return
        
    #Concatenate all and return
    return pd.concat([DRE_ITR_CON,DRE_DFP_CON,DRE_ITR_IND,DRE_DFP_IND])

def getListOfCVM():
    doc_ref = db.collection('dados_cia')
    listOfCVM = []
    try:
        docs = doc_ref.get()
        for doc in docs:
            listOfCVM.append(doc.id)
        return listOfCVM
    except:
        print('Error on loading list of CVM in dados_cia.')

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

def uploadDREtoFirestore(data):
    doc_ref = db.collection('DRE').document(data.cvm).collection(data.grupo).document(data.ano)
    try:
        doc_ref.set(data)
        print(f'cvm:{data.cvm}, grupo:{data.grupo}, ano:{data.ano} - DRE data successfully uploaded.')
    except:
        print(f'Error on upload DRE data. cvm: {data.cvm}, ano: {data.ano}, grupo: {data.grupo}')


def processDREdata(year):
    #Get list of CVM from dados_cia collection - only these cvms are processed in the DRE dataset
    listOfCVM = getListOfCVM()
    DRE = loadDREdata(year)
    if(DRE == None): return 
    #Apply filter by listOfCVM
    DRE = DRE[DRE.CD_CVM.isin(listOfCVM)]
    #Define column NIVEL by using column CD_CONTA
    DRE['NIVEL']=DRE.apply(lambda r: len(r['CD_CONTA'].split('.')), axis=1)
    #Filter NIVEL 2
    DRE_NIVEL1 = DRE[DRE.NIVEL==2]
    DRE_NIVEL1['DESC_SIMPLES']=DRE_NIVEL1['DS_CONTA'].map(MapNivel2)
    DRE_NIVEL1.dropna(subset=['DESC_SIMPLES'], inplace=True)
    DRE_NIVEL1['CD_CONTA']=DRE_NIVEL1['DESC_SIMPLES'].map(MapaNiveis)
    #Filter NIVEL 3
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
   
    DRE['TRIM'] = DRE.DT_FIM_EXERC.map(lambda d: getTrim(d.month))
    DRE.dropna(inplace=True)
    #Create column with YEAR
    DRE['YEAR']=DRE.DT_FIM_EXERC.dt.year
    #Create DAYS column (diff entre INI e FIM)
    DRE['DAYS'] = DRE.apply(lambda x: (x['DT_FIM_EXERC']-x['DT_INI_EXERC']).days , axis=1)

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
                    # uploadDREtoFirestore(data)
            else:
                print('  ' + str(cvm) +  ' - ' + grupo +' - ' + 'EMPTY')


