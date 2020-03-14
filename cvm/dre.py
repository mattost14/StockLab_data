#!/usr/bin/env python
# coding: utf-8

# In[1]:


# This Python 3 environment comes with many helpful analytics libraries installed
# It is defined by the kaggle/python docker image: https://github.com/kaggle/docker-python
# For example, here's several helpful packages to load in 

import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import matplotlib 
import matplotlib.pyplot as plt
#import seaborn as sns
#sns.set(rc={'figure.figsize':(15,10),"font.size":16,"axes.titlesize":16,"axes.labelsize":16})

# Input data files are available in the "../input/" directory.
# For example, running this (by clicking run or pressing Shift+Enter) will list the files in the input directory

import os
print(os.listdir("./dre_input"))

# Any results you write to the current directory are saved as output.

# Carregando dados trimestrais (ITR) de cada ano

# In[2]:


DRE_ITR2015 = pd.read_csv("../input/cvm-itr/itr_cia_aberta_dre_con_2015.csv", thousands=',', sep=';', encoding='latin-1')
DRE_ITR2016 = pd.read_csv("../input/cvm-itr/itr_cia_aberta_dre_con_2016.csv", thousands=',', sep=';', encoding='latin-1')
DRE_ITR2017 = pd.read_csv("../input/cvm-itr/itr_cia_aberta_dre_con_2017.csv", thousands=',', sep=';', encoding='latin-1')
DRE_ITR2018 = pd.read_csv("../input/cvm-itr/itr_cia_aberta_dre_con_2018.csv", thousands=',', sep=';', encoding='latin-1')
DRE_ITR2019 = pd.read_csv("../input/cvm-itr/itr_cia_aberta_dre_con_2019.csv", thousands=',', sep=';', encoding='latin-1')
#Concat todos
DRE_ITR=pd.concat([DRE_ITR2015,DRE_ITR2016,DRE_ITR2017,DRE_ITR2018,DRE_ITR2019])

# Carregando dados padronizados (DFP) de cada ano

# In[3]:


DRE_DFP2015 = pd.read_csv("../input/cvm-dfp/dre_cia_aberta_con_2015.csv", thousands=',', sep=';', encoding='latin-1')
DRE_DFP2016 = pd.read_csv("../input/cvm-dfp/dre_cia_aberta_con_2016.csv", thousands=',', sep=';', encoding='latin-1')
DRE_DFP2017 = pd.read_csv("../input/cvm-dfp/dre_cia_aberta_con_2017.csv", thousands=',', sep=';', encoding='latin-1')
DRE_DFP2018 = pd.read_csv("../input/cvm-dfp/dre_cia_aberta_con_2018.csv", thousands=',', sep=';', encoding='latin-1')
DRE_DFP2019 = pd.read_csv("../input/cvm-dfp/dre_cia_aberta_con_2019.csv", thousands=',', sep=';', encoding='latin-1')
#Concat todos
DRE_DFP=pd.concat([DRE_DFP2015,DRE_DFP2016,DRE_DFP2017,DRE_DFP2018,DRE_DFP2019])

# Concat DRE_ITR e DRE_DFP

# In[4]:


#Concat DRE_ITR e DRE_DFP
DRE=pd.concat([DRE_ITR,DRE_DFP])

# In[5]:


#DATA REF
DRE.DT_REFER=pd.to_datetime(DRE.DT_REFER)
#INICIO
DRE.DT_INI_EXERC=pd.to_datetime(DRE.DT_INI_EXERC)
#FIM
DRE.DT_FIM_EXERC=pd.to_datetime(DRE.DT_FIM_EXERC)

# Limpando dados incoerentes com os inícios e fins de trimestres 

# In[6]:


#Drop ORDEM_EXERC==PENÚLTIMO
DRE=DRE[DRE.ORDEM_EXERC=='ÚLTIMO']
#Drop Dados com DATA de INÍCIO diferente de JAN
DRE=DRE[DRE.DT_INI_EXERC.dt.month==1]
#Drop Dados com DATA de FIM diferente de MAR(3),JUN(6),SEP(9),DEC(12) 
DRE=DRE[DRE.DT_FIM_EXERC.dt.month.isin([3,6,9,12])]

# Definido o trimestre correspondente de cada dado

# In[7]:


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

# In[8]:


DRE['YEAR']=DRE.DT_FIM_EXERC.dt.year

# In[9]:


DRE=DRE.sort_values(['CD_CVM','DS_CONTA','YEAR','TRIM'])
DRE=DRE.reset_index(drop=True)

# In[10]:


DRE['TRIM_VL']=DRE.VL_CONTA-DRE.VL_CONTA.shift(+1)

# In[11]:


DRE

# In[12]:


DRE['CHECK']=(DRE.DS_CONTA==DRE.DS_CONTA.shift(+1)) & (DRE.CD_CVM==DRE.CD_CVM.shift(+1)) & (DRE.YEAR==DRE.YEAR.shift(+1)) & (DRE.TRIM-DRE.TRIM.shift(+1)==1)

# In[13]:


checkFalseIndex = DRE[(DRE['CHECK']==False) & (DRE['TRIM']==1)].index
DRE.loc[checkFalseIndex, 'VL_CONTA']
DRE.loc[checkFalseIndex, 'TRIM_VL']= DRE.loc[checkFalseIndex, 'VL_CONTA']
DRE.loc[checkFalseIndex, 'CHECK'] = DRE.loc[checkFalseIndex, 'CHECK'].map({False: True})

checkFalseIndex2 = DRE[(DRE['CHECK']==False) & (DRE['TRIM']!=1)].index
DRE.loc[checkFalseIndex2, 'TRIM_VL'] = pd.np.nan

# In[14]:


DRE[(DRE.DENOM_CIA=='AMBEV S.A.') & (DRE.DS_CONTA=='Receita de Venda de Bens e/ou Serviços')].groupby(['YEAR', 'TRIM'])['TRIM_VL'].sum().plot.bar()

# In[15]:


f=DRE[(DRE.DENOM_CIA=='AMBEV S.A.') & (DRE.DS_CONTA=='Receita de Venda de Bens e/ou Serviços')].groupby(['YEAR', 'TRIM'])['TRIM_VL'].sum()
f=f.reset_index()
f['TMT']=f.TRIM_VL.shift(+3)+f.TRIM_VL.shift(+2)+f.TRIM_VL.shift(+1)+f.TRIM_VL

# In[16]:


f.set_index(['YEAR','TRIM']).TMT.plot.bar()

# In[17]:



