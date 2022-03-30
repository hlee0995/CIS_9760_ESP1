# Allows us to connect to the data source and pulls the information
import argparse
import sys
from sodapy import Socrata
import requests
from requests.auth import HTTPBasicAuth
import os
import json

parser = argparse.ArgumentParser(description='processed data from open data')
parser.add_argument('--page_size', type=int, help='how many rows to get per page', required=True)
parser.add_argument('--num_pages', type=int, help='how many pages to get in total')
args = parser.parse_args(sys.argv[1:])
print(args)

#DATASET_ID="nc67-uf89"
#APP_TOKEN="aoXvaClrHZ8lf0RHcPSBWeeMu"
#ES_HOST="https://search-cis9760-hyunjik-lee-tl3cq44xwz2qdfgeepsz5qz3ha.us-east-2.es.amazonaws.com"
#ES_USERNAME="hyunjik_lee"
#ES_PASSWORD="010265Lee@"
#INDEX_NAME ="Violation"

"""
docker run -e DATASET_ID="nc67-uf89" -e APP_TOKEN="jnRNsxAqncIsd9aQ8edpY1O01" -e ES_HOST="https://search-cis9760-hyunjik-lee-tl3cq44xwz2qdfgeepsz5qz3ha.us-east-2.es.amazonaws.com" -e ES_USERNAME="hyunjik_lee" -e ES_PASSWORD="010265Lee@" -e INDEX_NAME="violationx20" image1:1.0 --page_size=10
"""


DATASET_ID=os.environ["DATASET_ID"]
APP_TOKEN=os.environ["APP_TOKEN"]
ES_HOST=os.environ["ES_HOST"]
ES_USERNAME=os.environ["ES_USERNAME"]
ES_PASSWORD=os.environ["ES_PASSWORD"]
INDEX_NAME=os.environ["INDEX_NAME"]


if args.num_pages:
        page=0
        while page < args.num_pages:
            page +=1
            if __name__ == '__main__': 
                try:
        # {ES_HOST}/{INDEX_NAME}: This is the URL to create payroll index, which is our Elasticsearch db.
                    resp = requests.put(f"{ES_HOST}/{INDEX_NAME}", auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),
                        json={
                            "settings": {
                                "number_of_shards": 1,
                                "number_of_replicas": 1
                            },
                            "mappings": {
                        # These are the columns of this database. 
                        # We define here what we want the data to be. For instance, we want base salary to be float 
                        # So we can do numerical analysis like average. However, the data is not guaranteed to be clean.   
                                "properties": {
                                    "plate": {"type": "keyword" },
                                    "state": {"type": "text" },
                                    "license_type": {"type": "text" },
                                    "summons_number": {"type": "text" },
                                    "issue_date": {"type": "date", "format": "mm/dd/yyyy"},
                                    "violation": {"type": "text" },
                                    "fine_amount": {"type": "float" },
                                    "penalty_amount": {"type": "float" },
                                    "interest_amount": {"type": "float" },
                                    "reduction_amount": {"type": "float" },
                                    "payment_amount": {"type": "float" },
                                    "amount_due": {"type": "float" },
                                    "precinct": {"type": "text" },
                                    "county": {"type": "text" },
                                }
                            },
                        }
                    )
                    resp.raise_for_status()
                    print(resp.json())
        
                except Exception as e:
                    print("Index already exists! Skipping")

                client = Socrata("data.cityofnewyork.us", APP_TOKEN,)
                rows = client.get(DATASET_ID, limit=args.page_size, offset=page)
                es_rows = [] #creating empty array for ES *becareful with s

                for row in rows:
                    try:
                        es_row={}
                        es_row["plate"] = row["plate"]
                        es_row["state"] = row["state"]
                        es_row["license_type"] = row["license_type"]
                        es_row["summons_number"] = row["summons_number"]
            # We specify the format of the issue date above so we just have to pass along, no conversion
                        es_row["issue_date"] = row["issue_date"]
                        es_row["violation"] = row["violation"]
                        es_row["fine_amount"] = float(row["fine_amount"])
                        es_row["penalty_amount"] = float(row["penalty_amount"])
                        es_row["interest_amount"] = float(row["interest_amount"])
                        es_row["reduction_amount"] = float(row["reduction_amount"])
                        es_row["payment_amount"] = float(row["payment_amount"])
                        es_row["amount_due"] = float(row["amount_due"])
                        es_row["precinct"] = row["precinct"]
                        es_row["county"] = row["county"]
                        print(es_row)

                    except Exception as e:
                        print(f"error!: {e}, skipping row {row}")
                        continue

                    es_rows.append(es_row)


                bulk_upload_data = ""
                for line in es_rows:
                    print(f'Handling row {line["summons_number"]}')
                    action = '{"index": {"_index": "' + INDEX_NAME + '", "_type": "_doc", "_id": "' + line["summons_number"] + '"}}'
                    data = json.dumps(line)
                    bulk_upload_data += f"{action}\n"
                    bulk_upload_data += f"{data}\n"
        
            #print (bulk_upload_data)

            
                try:
                # Upload to Elasticsearch by creating a document
                    resp = requests.post(f"{ES_HOST}/_bulk",
                # We upload es_row to Elasticsearch
                                data=bulk_upload_data,auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD), headers = {"Content-Type": "application/x-ndjson"})
                    resp.raise_for_status()
                    print ('Done')
                
            # If it fails, skip that row and move on.
                except Exception as e:
                    print(f"Failed to insert in ES: {e}, skipping row: {row}")

    
else:
    if __name__ == '__main__': 
        try:
        # {ES_HOST}/{INDEX_NAME}: This is the URL to create payroll index, which is our Elasticsearch db.
            resp = requests.put(f"{ES_HOST}/{INDEX_NAME}", auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),
                json={
                    "settings": {
                        "number_of_shards": 1,
                        "number_of_replicas": 1
                    },
                    "mappings": {
                    # These are the columns of this database. 
                    # We define here what we want the data to be. For instance, we want base salary to be float 
                    # So we can do numerical analysis like average. However, the data is not guaranteed to be clean.   
                        "properties": {
                            "plate": {"type": "keyword" },
                            "state": {"type": "text" },
                            "license_type": {"type": "text" },
                            "summons_number": {"type": "text" },
                            "issue_date": {"type": "date", "format": "mm/dd/yyyy"},
                            "violation": {"type": "text" },
                            "fine_amount": {"type": "float" },
                            "penalty_amount": {"type": "float" },
                            "interest_amount": {"type": "float" },
                            "reduction_amount": {"type": "float" },
                            "payment_amount": {"type": "float" },
                            "amount_due": {"type": "float" },
                            "precinct": {"type": "text" },
                            "county": {"type": "text" },
                        }
                    },
                }
            )
            resp.raise_for_status()
            print(resp.json())
        
        except Exception as e:
            print("Index already exists! Skipping")

        client = Socrata("data.cityofnewyork.us", APP_TOKEN,)
        rows = client.get(DATASET_ID, limit=args.page_size,)
        es_rows = [] #creating empty array for ES *becareful with s

        for row in rows:
            try:
                #we need to translate our row into a dictionary that properly encodes the data as we defined it
                es_row={}
                es_row["plate"] = row["plate"]
                es_row["state"] = row["state"]
                es_row["license_type"] = row["license_type"]
                es_row["summons_number"] = row["summons_number"]
        # We specify the format of the issue date above so we just have to pass along, no conversion
                es_row["issue_date"] = row["issue_date"]
                es_row["violation"] = row["violation"]
                es_row["fine_amount"] = float(row["fine_amount"])
                es_row["penalty_amount"] = float(row["penalty_amount"])
                es_row["interest_amount"] = float(row["interest_amount"])
                es_row["reduction_amount"] = float(row["reduction_amount"])
                es_row["payment_amount"] = float(row["payment_amount"])
                es_row["amount_due"] = float(row["amount_due"])
                es_row["precinct"] = row["precinct"]
                es_row["county"] = row["county"]
                print(es_row)

            except Exception as e:
                print(f"error!: {e}, skipping row {row}")
                continue

            es_rows.append(es_row)


        bulk_upload_data = ""
        for line in es_rows:
            print(f'Handling row {line["summons_number"]}')
            action = '{"index": {"_index": "' + INDEX_NAME + '", "_type": "_doc", "_id": "' + line["summons_number"] + '"}}'
            data = json.dumps(line)
            bulk_upload_data += f"{action}\n"
            bulk_upload_data += f"{data}\n"
    
        #print (bulk_upload_data)

        
        try:
            # Upload to Elasticsearch by creating a document
            resp = requests.post(f"{ES_HOST}/_bulk",
            # We upload es_row to Elasticsearch
                        data=bulk_upload_data,auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD), headers = {"Content-Type": "application/x-ndjson"})
            resp.raise_for_status()
            print ('Done')
            
        # If it fails, skip that row and move on.
        except Exception as e:
            print(f"Failed to insert in ES: {e}, skipping row: {row}")
            
        
        #print(resp.json())

    




