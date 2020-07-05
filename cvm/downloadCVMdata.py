import requests, io
import json
from zipfile import ZipFile
import pandas as pd
import sys 


def query_yes_no(question, default="yes"):
    valid = {"sim": True, "s": True, "si": True,
             "nao": False, "n": False}
    if default is None:
        prompt = " [s/n] "
    elif default == "sim":
        prompt = " [S/n] "
    elif default == "nao":
        prompt = " [s/N] "
    else:
        raise ValueError("Resposta inválida: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Por favor, responda com 'sim' or 'nao' "
                             "(ou 's' or 'n').\n")

# ---- Atualizar package -----
def atualizar_package(zip_file_url, document):
    print('Downloading ...')
    r = requests.get(zip_file_url, stream=True)
    z = ZipFile(io.BytesIO(r.content))
    if document == 'cia_aberta-doc-itr': z.extractall('./input_cvm/itr')
    elif document == 'cia_aberta-doc-dfp-dre': z.extractall('./input_cvm/dfp')
    elif document == 'cia_aberta-doc-dfp-bpa': z.extractall('./input_cvm/dfp')
    elif document == 'cia_aberta-doc-dfp-bpp': z.extractall('./input_cvm/dfp')
    
# ----- Informações Trimestrais -----
def checkPackages(document):
    # Documentação da API    https://docs.ckan.org/en/ckan-2.6.0/api/
    url = 'http://dados.cvm.gov.br/api/3/action/package_search'
    # ------ Checking input_log -----
    input_log = pd.read_csv('input_cvm/input_log.csv')
    # print('Log atual:')
    # print(input_log)

    response = requests.get(url, params={'q': document})
    data = response.json()
    results = data['result']['results'][0]
    num = results['num_resources']
    resources = results['resources']
    print(num)
    # print(resources)

    for i in range(0,num):
        #Check whether there is the same package in the input_log
        package_id = resources[i]['package_id']
        resource_id = resources[i]['id']
        name = resources[i]['name']
        last_modified = resources[i]['last_modified']
        revision_id = resources[i]['revision_id']
        if any(input_log.name == name):
            #Checking if it is updated
            if(last_modified):
                package_date = pd.to_datetime(resources[i]['last_modified'])
                input_log_date = pd.to_datetime(input_log[input_log.name == name]['last_modified'].values[0])
                delta = (package_date-input_log_date).days
                if delta>0: #Atualização disponível
                    input_log.loc[(input_log.name == name).index, 'atualizado']=False
                    ans = query_yes_no(f'{name} - Desatualizado. Gostaria de atualizado o package?', default="nao")
                    if ans: #Substituindo package desatualizado pelo atualizado
                        zip_file_url = resources[i]['url']
                        atualizar_package(zip_file_url, document)
                        input_log.loc[(input_log.name == name).index, 'revision_id']=revision_id
                        input_log.loc[(input_log.name == name).index, 'atualizado']=True
                        input_log.loc[(input_log.name == name).index, 'last_modified']=package_date
                        print('Package atualizado.')
                else: #Tudo ok!
                    input_log.loc[(input_log.name == name).index, 'atualizado']=True
                    print(f'{name} - Package já atualizado.')             
        else: #Package novo!
            print(f'{name} - Package novo.')
            ans = query_yes_no('Gostaria de incluir o package novo?', default="sim")
            if ans:
                zip_file_url = resources[i]['url']
                atualizar_package(zip_file_url, document)
                created = resources[i]['created']
                input_log=input_log.append(pd.Series([resource_id, name, pd.Timestamp(created), pd.Timestamp(last_modified), revision_id, zip_file_url, True], index=['package_id','name', 'created',  'last_modified', 'revision_id', 'zip_file_url', 'atualizado']), ignore_index=True)
    #Save new input_log
    print('Registrando alterações no log.')
    input_log.to_csv('input_cvm/input_log.csv', index=False)

if __name__ == "__main__":  
    # checkPackages('cia_aberta-doc-itr')
    # checkPackages('cia_aberta-doc-dfp-dre')
    checkPackages('cia_aberta-doc-dfp-bpa')
   # checkPackages('cia_aberta-doc-dfp-bpp')
#print('Novo log:')
#print(input_log)

docs = ["cia_aberta-cad",
 "cia_aberta-doc-dfp-bpa",
  "cia_aberta-doc-dfp-bpp", 
  "cia_aberta-doc-dfp-dfc_md", 
  "cia_aberta-doc-dfp-dfc_mi", 
  "cia_aberta-doc-dfp-dmpl", 
  "cia_aberta-doc-dfp-dre", 
  "cia_aberta-doc-dfp-dva", 
  "cia_aberta-doc-fre", 
  "cia_aberta-doc-itr", 
  "distrpubl",
  "emissores",
  "fi-cad",
  "fidc-doc-inf_mensal", 
  "fi-doc-balancete", 
  "fi-doc-cda",
  "fi-doc-compl",
  "fi-doc-eventual",
  "fi-doc-extrato",
  "fi-doc-inf_diario",
  "fi-doc-lamina",
  "fi-doc-perfil_mensal",
  "fie-cad",
  "fie-medidas",
  "fip-doc-inf_trimestral", 
  "intermediario-cad"]

