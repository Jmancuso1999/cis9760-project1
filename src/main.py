from sodapy import Socrata
import requests
from requests.auth import HTTPBasicAuth
import argparse
import sys
import os
import json

# Creates a parser. Parser is the thing where you add your arguments. 
parser = argparse.ArgumentParser(description='311 Requests Data')

# Required Argument
parser.add_argument('--page_size', type=int, help='how many rows to get per page', required=True)

# Optional Argument
parser.add_argument('--num_pages', type=int, help='how many pages to get in total')
args = parser.parse_args(sys.argv[1:])
#print(args)

# Environent arguments passed in through command line
DATASET_ID=os.environ["DATASET_ID"]
APP_TOKEN=os.environ["APP_TOKEN"] # NYC Open Data App token 
ES_HOST=os.environ["ES_HOST"] # elastic search cluster
ES_USERNAME=os.environ["ES_USERNAME"] # elastic search username
ES_PASSWORD=os.environ["ES_PASSWORD"] # elastic search password
INDEX_NAME=os.environ["INDEX_NAME"] 

if __name__ == '__main__': 
    try:
        #Using requests.put(), we are creating an index (db) first.
        resp = requests.put(f"{ES_HOST}/{INDEX_NAME}", auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),
            json={
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1
                },
                #We are specifying the columns and define what we want the data to be.
                #However, it is not guaranteed that the data will come us clean. 
                #We will might need to clean it in the next steps.
                #If the data you're pushing to the Elasticsearch is not compatible with these definitions, 
                #you'll either won't be able to push the data to Elasticsearch in the next steps 
                #and get en error due to that or the columns will not be usable in Elasticsearch 
                "mappings": {
                    "properties": {
                        "starfire_incident_id": {"type": "keyword"},
                        "incident_datetime": {"type": "date"},
                        "incident_response_seconds_qy": {"type": "integer"},
                        "engines_assigned_quantity": {"type": "integer"},
                        "incident_borough": {"type": "keyword"},
                        "incident_classification": {"type": "keyword"},
                    }
                },
            }
        )
        resp.raise_for_status()
        #print(resp.json())
        
    except Exception as e:
        print("Index already exists! Skipping")    
    
    client = Socrata("data.cityofnewyork.us", APP_TOKEN, timeout=10000)
    
    recordCount = client.get("8m42-w767", select = 'COUNT(*)')
    print("Total Records:", int(recordCount[0]['COUNT'])) # This value should be about the same as the es_rows size
    
    es_rows = []
    
    num_pages = 1
    
    # if num_pages is given
    if args.num_pages:
        num_pages = args.num_pages
    
    #print(num_pages)
    
    for i in range(0, num_pages):
        # Gets the database rows 
        # rows = list of dictionaries
        rows = client.get(DATASET_ID, limit=args.page_size, offset = i * num_pages)
        
        for row in rows:
            # dictionary of the CURRENT rows key and value (value being the column value for that specific row)
            es_row = {}
            
            try:
                # Python Scripting #4 - ONLY get rows where starfire_incident_id and incident_datetime is NOT NULL
                if row["starfire_incident_id"] is not None and row["incident_datetime"] is not None:
                    # row == current row of the dataset
                    es_row["starfire_incident_id"] = row["starfire_incident_id"] 
                    es_row["incident_datetime"] = row["incident_datetime"]
                    es_row["incident_response_seconds_qy"] = int(row["incident_response_seconds_qy"])
                    es_row["engines_assigned_quantity"] = int(row["engines_assigned_quantity"])
                    es_row["incident_borough"] = row["incident_borough"]
                    es_row["incident_classification"] = row["incident_classification"]
                    
                    # Adds the current dictionary of the current row to the list
                    es_rows.append(es_row)
                                    
                    #print("Works: ", row)
                    
            except Exception as e:
                print(f"Error!: {e}, skipping row: {row}")
                continue

    
    print("Size of es_rows:", len(es_rows))
    print(es_rows)
    
    # Preparing data for bulk upload to Elastic Search
    
    bulk_upload_data = ""
    
    for line in es_rows:
        print(f'Handling row {line["starfire_incident_id"]}')
        action = '{"index": {"_index": "' + INDEX_NAME + '", "_type": "_doc", "_id": "' + line["starfire_incident_id"] + '"}}'
        data = json.dumps(line) # Converts the given row line into JSON object
        bulk_upload_data += f"{action}\n"
        bulk_upload_data += f"{data}\n"
    
    print(bulk_upload_data)


    # Uploading the bulk data
    try:
        # Upload to Elasticsearch by creating a document
        resp = requests.post(f"{ES_HOST}/_bulk",
            # We upload es_row to Elasticsearch
                    data=bulk_upload_data,auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD), headers = {"Content-Type": "application/x-ndjson"})
        resp.raise_for_status()
        print ('Done')

            
    # If it fails, skip that row and move on.
    except Exception as e:
        print(f"Failed to insert in ES: {e}")
