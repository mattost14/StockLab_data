import os
import sys
sys.path.append('/Users/BrunoMattos/Documents2/Dev/stocklab_data/Firestore')
from uploadToFirestore import uploadDocumentToFirestore

### UPLOAD DOCUMENTS TO FIRESTORE ####
print('STEP4 - UPLOADING DATA TO FIRESTORE')
path_to_output_folder = './output_cvm/dre/'
json_files = [pos_json for pos_json in os.listdir(path_to_output_folder) if pos_json.endswith('.json')]
for file in json_files:
    if file in ['1155.json', '24228.json']: 
        print('Uploading ' + file + '...')
        uploadDocumentToFirestore('dre',path_to_output_folder+file,file.split('.')[0])