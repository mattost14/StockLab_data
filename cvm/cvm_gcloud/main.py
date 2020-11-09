import requests, io
import json
from zipfile import ZipFile
import pandas as pd
import sys 
import os, io
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime


# cred = credentials.Certificate("./serviceAccountKey-US.json")
# firebase_admin.initialize_app(cred)
# db = firestore.client()

PROJECT_NAME = 'stocklab-8255c'

# initialize firebase sdk
CREDENTIALS = credentials.ApplicationDefault()
firebase_admin.initialize_app(CREDENTIALS, {
    'projectId': PROJECT_NAME,
})

# get firestore client
FIRESTORE_CLIENT = firestore.client()
from google.cloud import firestore
db = firestore.Client()
from google.cloud import storage
client = storage.Client()


# ---- Atualizar package -----
def atualizar_package(zip_file_url, document):
    print('Downloading ...')
    r = requests.get(zip_file_url, stream=True)
    z = ZipFile(io.BytesIO(r.content))
    bucket = client.get_bucket('stocklab-8255c.appspot.com')
    formUploadStatus = {}
    error_count = 0
    for filename in z.namelist():
        with z.open(filename) as file:
            if(document == 'cia_aberta-doc-itr'):
                filename_cmp = filename.split('_')
                if(len(filename_cmp) >=3):
                    typeOfdoc = filename_cmp[3]
                    if(not typeOfdoc in ['DRE', 'BPA', 'BPP', 'DFC']):
                        print(f'Skiping - {filename}')
                        continue
            blob = bucket.blob('cvm/inputs/'+filename)
            try:
                start = datetime.now()
                # destination.upload_from_string(file.read())
                blob.upload_from_string(file.read(), timeout=300)
                end = datetime.now()
                formUploadStatus[filename] = f'sucessfully uploaded to firestore in : {end-start}'
                print(f'{filename} - sucessfully uploaded to firestore in : {end-start}')
            except:
                print (f'{filename} - upload failed due to Timeout')
                error_count = error_count + 1
                formUploadStatus[filename] = f'upload failed due to Timeout'
                pass

    formUploadStatus['errors'] = error_count
    return formUploadStatus



# ----- Informações Trimestrais -----
def checkPackages(document):
    # Documentação da API    https://docs.ckan.org/en/ckan-2.6.0/api/
    url = 'http://dados.cvm.gov.br/api/3/action/package_search'
     # ------ Checking input_log stored in Firestore -----
    doc_ref = db.collection('cvm_data').document(document)
    doc = doc_ref.get()
    input_log = doc.to_dict()
    #Request CVM api to see what is available to download
    response = requests.get(url, params={'q': document})
    data = response.json()
    results = data['result']['results'][0]
    num = results['num_resources']
    resources = results['resources']
    
    for i in range(0,num):
        #Check whether there is the same package in the input_log
        package_id = resources[i]['package_id']
        resource_id = resources[i]['id']
        name = resources[i]['name']
        last_modified = resources[i]['last_modified']
        revision_id = resources[i]['revision_id']
        created = resources[i]['created']
        # print(name)
        if(input_log != None):
            list_of_existing_docs = list(input_log.keys())
        else: 
            list_of_existing_docs = []
        if name in list_of_existing_docs:
            #Checking if it is updated
            print(f'Find {name}')
            if(last_modified):
                package_date = pd.to_datetime(last_modified).tz_localize(tz='Brazil/East')
                creation_date = pd.to_datetime(created).tz_localize(tz='Brazil/East')
                input_log_date = pd.to_datetime(input_log[name]['last_modified'].rfc3339()).tz_convert(tz='Brazil/East')
                delta = (package_date-input_log_date).days
                if delta>0: #Atualização disponível
                    print(f'New version of the document ({name}) is available. Proceeding to update.')
                    zip_file_url = resources[i]['url']
                    status = atualizar_package(zip_file_url, document)
                    doc = doc_ref.set({
                        name:{
                            'updatedIn' : datetime.now(),
                            'createdIn' : creation_date,
                            'last_modified': package_date,
                            'status': status,
                            'processed': False
                        }
                    }, merge=True)
                else: 
                    print(f'{name} - Package já atualizado.')           
        else: #Package novo!
            if(last_modified):
                print(f'{name} - Package novo.')
                zip_file_url = resources[i]['url']
                status = atualizar_package(zip_file_url, document)
                package_date = pd.to_datetime(last_modified).tz_localize(tz='Brazil/East')
                creation_date = pd.to_datetime(created).tz_localize(tz='Brazil/East')
                doc = doc_ref.set({
                    name:{
                        'updatedIn' : datetime.now(),
                        'createdIn' : creation_date,
                        'last_modified': package_date,
                        'status': status,
                        'processed': False
                    }
                }, merge=True)


def downloadCVMdata(event,context):
    print('--- Downloading CVM data ---')
    start = datetime.now()
    checkPackages('cia_aberta-doc-itr')
    end = datetime.now()
    print(f'Operation completed in : {end - start}')
    

if __name__ == "__main__": 
    downloadCVMdata([],[])
    
    # checkPackages('cia_aberta-doc-dfp-dre')
    # checkPackages('cia_aberta-doc-dfp-bpa')
    # checkPackages('cia_aberta-doc-dfp-bpp')
