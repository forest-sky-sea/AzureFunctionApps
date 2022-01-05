import logging
import os
from datetime import datetime, timedelta

import azure.functions as func
from azure.storage.blob import ContainerClient, BlobClient


connect_str = os.getenv('AzureWebJobsStorage')


def copy_blobs(container, blob_path, output_path):
    container_client = ContainerClient.from_connection_string(connect_str, container_name=container)
    blob_names = [blob.name for blob in container_client.list_blobs(blob_path) if blob.name.endswith('.json')]
    for blob in blob_names:
        source_blob = container_client.get_blob_client(blob)
        new_path = output_path + '/' + blob.split('/')[1].split('=')[1] + '.json'
        dest_blob = container_client.get_blob_client(new_path)
        dest_blob.start_copy_from_url(source_blob.url)
    # print(len(file_names))


def write_tracker(tracker_path):
    container_client = ContainerClient.from_connection_string(connect_str, container_name="utils")
    tracker_file_names = [blob.name for blob in container_client.list_blobs(tracker_path)]
    tracker_file_names.sort()
    last_tracker = tracker_file_names[-1]
    last_tracker_time = datetime.strptime(last_tracker.split('/')[-1], '%Y_%m_%d_%H_%M')
    new_tracker_time = last_tracker_time + timedelta(minutes=15)
    # print(new_track_time.strftime('%Y_%m_%d_%H_%M'))
    new_tracker = tracker_path + '/' + new_tracker_time.strftime('%Y_%m_%d_%H_%M')
    new_tracker_blob = container_client.get_blob_client(new_tracker)
    new_tracker_blob.upload_blob('')


def main(req: func.HttpRequest) -> func.HttpResponse:
    # logging.info('Python HTTP trigger function processed a request.')

    # name = req.params.get('name')
    # if not name:
    #     try:
    #         req_body = req.get_json()
    #     except ValueError:
    #         pass
    #     else:
    #         name = req_body.get('name')

    # if name:
    #     return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    # else:
    #     return func.HttpResponse(
    #          "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
    #          status_code=200
    #     )
    copy_blobs(container='filter-internal-output', blob_path='test_funcapp', output_path='funcout')
    write_tracker(tracker_path='.trackerfile')
    return func.HttpResponse(f"Hello, wildcards. This HTTP triggered function executed successfully.")
