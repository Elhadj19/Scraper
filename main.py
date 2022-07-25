

#!/usr/bin/env python3

import requests
import json
import sys

from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage

NOW = datetime.now()
FILE = "file_" + str(NOW.year) + str(NOW.month) + str(NOW.day) + ".json"
CLIENT = bigquery.Client()
DATASET_ID = 'xxx'
TABLE_ID = FILE.replace(".json", "")
DATASET_REF = CLIENT.dataset(DATASET_ID)
DATASET = ""
TABLE_REF = DATASET_REF.table(TABLE_ID)
JOB_CONFIG = bigquery.LoadJobConfig()
JOB_CONFIG.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
JOB_CONFIG.autodetect = True
DATE = datetime.now().replace(microsecond=0).isoformat()
DATE_1 = datetime(year=NOW.year,
                  month=NOW.month,
                  day=NOW.day - 1
).isoformat()


def download_dataset(date_1=DATE_1, date=DATE, name_file=FILE):
    """Download a dataset"""

    url = "http://api.xxx.com/oauth2/token"
    payload = "grant_type=client_credentials&client_id=&client_secret="
    headers = {
        "Accept": "text/plain",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    response = requests.request("POST", url, data=payload, headers=headers)
    r = response.json()
    Authorization = r["token_type"] + ' ' + r["access_token"]
    url = "http://api.xxx.com/preview/advertisers/me"
    headers = {
        "Authorization": Authorization,
        "Accept": "application/json",
        "Content-Type": "application/*+json"
    }
    response = requests.request("GET", url, headers=headers)
    url = "http://api.xxx.com/preview/statistics/report"
    payload = "{'dimensions':[],'metrics':[],'format':'Json','currency':'','startDate':'" + date_1 + "','endDate':'" + date + "'}"
    headers = {
        "Authorization": Authorization,
        "Accept": "text/plain",
        "Content-Type": "application/*+json"
    }
    response = requests.request("POST", url, data=payload, headers=headers)
    response.encoding='utf-8-sig'
    r_statistics = response.json()

    url = "http://api.xxx.com/preview/transactions/report"
    
    payload = "{\"data\":[{\"attributes\":{\"advertiserIds\":'" + r_statistics["Rows"][0]["AdvertiserId"] + "',\"startDate\":'" + date_1 + "',\"endDate\":'" + date + "',\"format\":\"Json\",\"currency\":\"EUR\"},\"type\":\"Transaction\"}]}"
    headers = {
        "Authorization": Authorization,
        "Accept": "text/plain",
        "Content-Type": "application/*+json"
    }

    response = requests.request("POST", url, data=payload, headers=headers)

    response.encoding='utf-8-sig'
    r_transaction = response.json()
    r_statistics["Transaction"] = r_transaction
    with open(name_file, "x") as outfile:
        json.dump(r_statistics, outfile)
        print("[+] Write file {} finished".format(name_file))
        outfile.close()
    return r_statistics, name_file


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)

    
    
if __name__ == "__main__":
    try:
        if len(sys.argv) > 3:
            arg1 = datetime.strptime(sys.argv[1], "%d-%m-%Y %H:%M:%S").isoformat()
            arg2 = datetime.strptime(sys.argv[2], "%d-%m-%Y %H:%M:%S").isoformat()
            DATASET, name_file = download_dataset(arg1, arg2, sys.argv[3])
        elif len(sys.argv) > 2:
            arg1 = datetime.strptime(sys.argv[1], "%d-%m-%Y %H:%M:%S").isoformat()
            arg2 = datetime.strptime(sys.argv[2], "%d-%m-%Y %H:%M:%S").isoformat()
            DATASET, name_file = download_dataset(arg1, arg2)
        elif len(sys.argv) > 1:
            DATASET, name_file = download_dataset(name_file=sys.argv[1])
        else:
            DATASET, name_file = download_dataset()
        #print("DATA", DATASET)
        upload_blob("dataset", name_file, name_file)
        print("[+] Loaded file in bucket finished", name_file)
        with open(name_file, "rb") as source_file:
            job = CLIENT.load_table_from_file(
	        source_file,
	        TABLE_REF,
	        location="US",
                job_config=JOB_CONFIG,
            )
            job.result()

        print("[+] Loaded {} rows into {}:{}".format(
            job.output_rows,
            DATASET_ID,
            TABLE_ID
        ))
        exit()
    except FileExistsError:
        print("file exist")

    print("Error upload file.json")
