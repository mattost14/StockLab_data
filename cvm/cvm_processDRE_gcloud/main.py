from datetime import datetime
from firebase import db
from processDREdata_fromStorage import processDREdata
import json


# gcloud functions deploy FUNCTION_NAME \
#   --runtime RUNTIME \
#   --trigger-event providers/cloud.firestore/eventTypes/document.write \
#   --trigger-resource projects/YOUR_PROJECT_ID/databases/(default)/documents/messages/{pushId}

# gcloud beta functions deploy processDREdata --runtime python37 --trigger-event providers/cloud.firestore/eventTypes/document.write --trigger-resource projects/stocklab-8255c/databases/firestore/documents/cvm_data/cia_aberta-doc-itr


# def processDREdata(event,context):
#     print('--- Processing DRE data ---')
#     start = datetime.now()
#     # processDREdata()
#     end = datetime.now()
#     print(f'Operation completed in : {end - start}')
    



def checkUnprocessedPackages():
    doc_ref = db.collection('cvm_data').document('cia_aberta-doc-itr')
    try:
        doc = doc_ref.get()
        input_log = doc.to_dict()
        unprocessedPackages = []
        # Iterate over all the items in dictionary and filter items which has even keys
        for (key, value) in input_log.items():
            # print(value['processed'])
            if(not value['processed']):
                unprocessedPackages.append(key)
        print('The following packages need to be processed:') 
        print(unprocessedPackages)
    except:
        print('Error - cvm_data log not found.')

def processDREdata_PubSub(event,context):
    # 1 - Read the log fields (processed flag)
    checkUnprocessedPackages()


def processDREdata_FirestoreTrigger(data, context):
    """ Triggered by a change to a Firestore document.
    Args:
        data (dict): The event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """
    trigger_resource = context.resource

    print('Function triggered by change to: %s' % trigger_resource)

    print('\nOld value:')
    print(json.dumps(data["oldValue"]))

    print('\nNew value:')
    print(json.dumps(data["value"]))

if __name__ == "__main__":
    processDREdata(2015) 
    # checkUnprocessedPackages()