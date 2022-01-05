from azure.storage.blob import ContainerClient, BlobClient
from datetime import datetime, timedelta

# dt = datetime.strptime('2021_12_31_23_45', '%Y_%m_%d_%H_%M')
# dt2 = dt + timedelta(minutes=15)
# print(dt2.strftime('%Y_%m_%d_%H_%M'))

connect_str = 'DefaultEndpointsProtocol=https;AccountName=mufenglinadfstorage;AccountKey=3NDRSVdBopI1q2F7X5Vmuo7A16N/3QZ9L3eNOWyM3/vlGIUaKvvimo2VBpV1VzLMG7TeuAbNh8inZO7lKYrJLg==;EndpointSuffix=core.windows.net'

container_client = ContainerClient.from_connection_string(connect_str, container_name="filter-internal-output")
blob_names = [blob.name for blob in container_client.list_blobs('test_funcapp') if blob.name.endswith('.json')]
for blob in blob_names:
    source_blob = container_client.get_blob_client(blob)
    new_path = 'funcout/' + blob.split('/')[1].split('=')[1] + '.json'
    dest_blob = container_client.get_blob_client(new_path)
    dest_blob.start_copy_from_url(source_blob.url)
# container_client = ContainerClient.from_connection_string(connect_str, container_name="utils")
# tracker_file_names = [blob.name for blob in container_client.list_blobs('.trackerfile')]
# tracker_file_names.sort()
# last_tracker = tracker_file_names[-1]
# last_tracker_time = datetime.strptime(last_tracker.split('/')[-1], '%Y_%m_%d_%H_%M')
# new_tracker_time = last_tracker_time + timedelta(minutes=15)
# new_tracker = '.trackerfile/' + new_tracker_time.strftime('%Y_%m_%d_%H_%M')
# new_tracker_blob = container_client.get_blob_client(new_tracker)
# # new_tracker_blob.delete_blob()
# new_tracker_blob.upload_blob('')
# src_blob = container_client.get_blob_client(last_tracker)
# dest_blob = container_client.get_blob_client(new_tracker)
# dest_blob.start_copy_from_url(src_blob.url)
# src = container_client.get_blob_client('funcout/CY1SCH203011815_VisualStudio2AME_16_2021-12-08T15_58_34.json')
# dest = container_client.get_blob_client('funcout/new.json')
# dest.start_copy_from_url(src.url)
# file_names = [blob.name for blob in container_client.list_blobs('test_funcapp') if blob.name.endswith('.json')]
# for name in file_names:
#     source_blob = BlobClient.from_connection_string(connect_str, 'filter-internal-output', name)
#     filename = 'funcout/' + name.split('/')[1].split('=')[1] + '.json'11 
#     dest_blob = BlobClient.from_connection_string(connect_str, 'filter-internal-output', filename)
#     dest_blob.start_copy_from_url(source_blob.url)
# print(len(file_names))
