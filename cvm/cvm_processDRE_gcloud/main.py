from datetime import datetime
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
    

if __name__ == "__main__": 
    processDREdata([],[])


def processDREdata(data, context):
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