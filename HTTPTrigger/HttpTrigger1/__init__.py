import logging
import os
from datetime import datetime

import azure.functions as func
from azure.storage.blob import ContainerClient


def generate_wildcards(tracker_path):
    connect_str = os.getenv('AzureWebJobsStorage')
    container_client = ContainerClient.from_connection_string(connect_str, container_name="utils")
    tracker_file_names = [blob.name for blob in container_client.list_blobs(tracker_path)]
    tracker_file_names.sort()
    last_tracker = tracker_file_names[-1].split('/')[-1]
    track_time = datetime.strptime(last_tracker, '%Y_%m_%d_%H_%M')
    year = str(track_time.year)
    month = '{:0>2d}'.format(track_time.month)
    day = '{:0>2d}'.format(track_time.day)
    hour = '{:0>2d}'.format(track_time.hour)
    minute_start = track_time.minute
    minute_wildcard_str = '{' + '{:0>2d}'.format(minute_start)
    for m in range(minute_start + 1, minute_start + 15):
        minute_wildcard_str += ',{:0>2d}'.format(m)
    minute_wildcard_str += '}'

    wildcards_str = f'/raw/{year}/{month}/{day}/{hour}/' +\
        f'*_*_*_{year}-{month}-{day}T{hour}_{minute_wildcard_str}*.json'

    wildcards_blob = container_client.get_blob_client('wildcards')
    wildcards_blob.delete_blob()
    wildcards_blob.upload_blob(wildcards_str)

    # print(wildcards_str)


def main(req: func.HttpRequest) -> func.HttpResponse:
    generate_wildcards('.trackerfile')
    # logging.info('Python HTTP trigger function processed a request.')

    # name = req.params.get('name')
    # if not name:
    #     try:
    #         req_body = req.get_json()
    #     except ValueError:
    #         pass
    #     else:
    #         name = req_body.get('name')

    return func.HttpResponse(f"Hello, wildcards. This HTTP triggered function executed successfully.")

    # if name:
    #     return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    # else:
    #     return func.HttpResponse(
    #          "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
    #          status_code=200
    #     )
