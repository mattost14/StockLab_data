import firebase_admin, json
from firebase_admin import credentials
from firebase_admin import firestore
import pandas as pd


cred = credentials.Certificate("Firestore/serviceAccountKey-US.json")
firebase_admin.initialize_app(cred)
db = firestore.client()


def uploadDocumentFromJsonData(collectionID, jsonData, documentID=False):
    # print('Uploading document to collection: ' + collectionID)
    if(documentID):
        doc_ref = db.collection(collectionID).document(documentID)
        doc_ref.set(jsonData)
    else: #Set automatic ID for document
        db.collection(collectionID).add(jsonData)

def uploadDocumentToSubcollectionFromJsonData(path, jsonData):
    # print('Uploading document to collection: ' + collectionID)
    doc_ref = db.document(path)
    doc_ref.set(jsonData)

def uploadDocumentToFirestore(collectionID, documentJson, documentID=False):
    # print('Uploading document to collection: ' + collectionID)
    with open(documentJson) as file:
        data = json.load(file)
    if(documentID):
        doc_ref = db.collection(collectionID).document(documentID)
        doc_ref.set(data)
    else: #Set automatic ID for document
        db.collection(collectionID).add(data)

def uploadCollectionToFirestore(collectionID, documentJson):
    print('Uploading many documents to collection: ' + collectionID)
    # print('Uploading ... ->  ' + documentJson)
    # with open(documentJson) as file:
    #     data = json.load(file)

    # #print(data)
    # if(documentID):
    #     doc_ref = db.collection(collectionID).document(documentID)
    #     doc_ref.set(data)
    # else: #Set automatic ID for document
    #     db.collection(collectionID).add(data)

def getDocument (collectionID, documentID):
    users_ref = db.collection(collectionID).document(str(documentID))
    doc = users_ref.get()
    print(pd.DataFrame.from_dict(doc.to_dict()['data']))
    # for doc in docs:
    #     #print(u'{} => {}'.format(doc.id, doc.to_dict()))
    #     print(doc)

if __name__ == "__main__":
    print('Import functions.')
    
    #getDocument ('dre', 94)
    
    # import sys, os
    # collection = sys.argv[1]
    # path_to_json = sys.argv[2] #'../cvm/output_cvm/dre/'
    # json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]
    # for file in json_files:
    #     uploadToFirestore(u'dre', path_to_json + file, file.split('.')[0])
