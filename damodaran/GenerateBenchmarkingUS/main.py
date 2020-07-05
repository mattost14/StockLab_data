from fundgrEB import extractDataFrom_fundgrEB 
from wacc import extractDataFrom_wacc
from histgr import extractDataFrom_histgr
from capex import extractDataFrom_capex
from indname import extractDataFrom_indname
from margin import extractDataFrom_margin

import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import json
import firebase_admin
from firebase_admin import credentials, firestore

cred = credentials.Certificate("./serviceAccountKey-US.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

#Translate Industry Name
with open('industry_names_translations.json') as json_file:
    translation = json.load(json_file)


print('Running...')
fundgrEB = extractDataFrom_fundgrEB()
wacc = extractDataFrom_wacc()
histgr  = extractDataFrom_histgr()
capex = extractDataFrom_capex()
margin = extractDataFrom_margin()


print('Merging...')
mergedDF = fundgrEB.merge(wacc, on='Industry Name',validate='one_to_one')
mergedDF = mergedDF.merge(histgr, on='Industry Name',validate='one_to_one')
mergedDF = mergedDF.merge(capex, on='Industry Name',validate='one_to_one')
mergedDF = mergedDF.merge(margin, on='Industry Name',validate='one_to_one')

mergedDF['Industria'] = mergedDF['Industry Name'].map(translation['industry_pt'])

jsonData = mergedDF.to_dict('records')

indname = extractDataFrom_indname()
for ind in jsonData:
    industria=ind['Industria']
    print(industria.replace('/', "-"))
    docRef = db.collection('benchmarking').document(industria.replace('/', "-"))
    docRef.set(ind)
    listOfInd = indname[indname['Industry Group'] == ind['Industry Name']]['Company Name'].values
    docRef.collection('companies').document('US').set({'listOfCompanies': listOfInd.tolist()})
