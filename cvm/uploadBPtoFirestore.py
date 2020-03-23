import os
import sys
sys.path.append('/Users/BrunoMattos/Documents2/Dev/stocklab_data/Firestore')
from uploadToFirestore import uploadDocumentToFirestore

### UPLOAD DOCUMENTS TO FIRESTORE ####
print('STEP4 - UPLOADING BALANÇO PATRIMONIAL TO FIRESTORE')
path_to_output_folder = './output_cvm/bp/'
cvm_folders = [cvm_folder for cvm_folder in os.listdir(path_to_output_folder)]
total = len(cvm_folders)
count=0
for cvm_folder in cvm_folders:
    count=count+1
    print(str(count)+'/'+str(total))
    for grupo in ['consolidado', 'individual']:
        path =path_to_output_folder+cvm_folder+'/'+grupo
        if os.path.exists(path):
            json_files = [pos_json for pos_json in os.listdir(path) if pos_json.endswith('.json')]
            for file in json_files:
                print('Uploading ' + cvm_folder + '/' + grupo + '/' + file)
                uploadDocumentToFirestore('bp/'+cvm_folder+'/'+grupo, path+'/'+file, file.split('.')[0])