import firebase_admin
from firebase_admin import credentials, firestore

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