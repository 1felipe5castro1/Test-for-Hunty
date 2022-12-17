# TEST PARTE UNO 

import json
import google.auth
from google.cloud import storage
from googleapiclient.discovery import build

# Authenticate and create a client for the Google Storage API
credentials, project = google.auth.default()
storage_client = storage.Client(credentials=credentials)

# Load the JSON data
with open('office_modality.json', 'r') as f:
    data1 = json.load(f)
with open('tamaño_empresas.json', 'r') as f:
    data2 = json.load(f)

# Save the JSON data to a Google Storage bucket
bucket_name = 'my-bucket'
bucket = storage_client.bucket(bucket_name)
blob1 = bucket.blob('data1.json')
blob1.upload_from_string(json.dumps(data1))
blob2 = bucket.blob('data2.json')
blob2.upload_from_string(json.dumps(data2))

# Convert the JSON files to a Google Sheets file
service = build('sheets', 'v4', credentials=credentials)
sheet_id = service.spreadsheets().create(body={
    'sheets': [{
        'properties': {
            'title': 'Sheet1'
        }
    }]
}).execute()['spreadsheetId']
requests = [{
    'importData': {
        'source': {
            'fileType': 'json',
            'gcsUrl': 'gs://{}/data1.json'.format(bucket_name)
        },
        'startLine': 0,
        'skipLeadingRows': 0,
        'columnDelimiter': ',',
        'quote': '"'
    }
}, {
    'importData': {
        'source': {
            'fileType': 'json',
            'gcsUrl': 'gs://{}/data2.json'.format(bucket_name)
        },
        'startLine': 0,
        'skipLeadingRows': 0,
        'columnDelimiter': ',',
        'quote': '"',
        'insertDataOption': 'INSERT_ROWS'
    }
}]
service.spreadsheets().batchUpdate(spreadsheetId=sheet_id, body={
    'requests': requests
}).execute()
