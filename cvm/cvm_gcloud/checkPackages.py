import requests, io
import json
from zipfile import ZipFile
import pandas as pd
import sys 
import os, io
from datetime import datetime
from firebase import db, client

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
    
    newlog_data = {}
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
                    newlog_data[name]={
                            'updatedIn' : datetime.now(),
                            'createdIn' : creation_date,
                            'last_modified': package_date,
                            'status': status,
                            'processed': False
                        }
                    # doc = doc_ref.set({
                    #     name:{
                    #         'updatedIn' : datetime.now(),
                    #         'createdIn' : creation_date,
                    #         'last_modified': package_date,
                    #         'status': status,
                    #         'processed': False
                    #     }
                    # }, merge=True)
                else: 
                    print(f'{name} - Package já atualizado.')           
        else: #Package novo!
            if(last_modified):
                print(f'{name} - Package novo.')
                zip_file_url = resources[i]['url']
                status = atualizar_package(zip_file_url, document)
                package_date = pd.to_datetime(last_modified).tz_localize(tz='Brazil/East')
                creation_date = pd.to_datetime(created).tz_localize(tz='Brazil/East')
                newlog_data[name]={
                    'updatedIn' : datetime.now(),
                    'createdIn' : creation_date,
                    'last_modified': package_date,
                    'status': status,
                    'processed': False
                }
                # doc = doc_ref.set({
                #     name:{
                #         'updatedIn' : datetime.now(),
                #         'createdIn' : creation_date,
                #         'last_modified': package_date,
                #         'status': status,
                #         'processed': False
                #     }
                # }, merge=True)
        #Update log in Firestore
    print(f'Updating log information in cvm_data/{document}')
    doc = doc_ref.set(newlog_data, merge=True)