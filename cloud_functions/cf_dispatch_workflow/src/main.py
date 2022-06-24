import os
import time
import json
import base64

from google.cloud import storage
from google.cloud import bigquery
from google.cloud.workflows import executions_v1



def receive_messages(event, context):
    pubsub_event = event
    print(pubsub_event)
    
    table_name = base64.b64decode(pubsub_event['data']).decode('utf-8')

    bucket_name = pubsub_event['attributes']["bucket_name"]
    file_path = pubsub_event['attributes']["file_path"]

    is_inserted = False
    try:
        insert_into_raw(table_name, bucket_name, file_path)
        move_file(bucket_name, file_path, 'archive')
        is_inserted = True
    except Exception as e:
        print(e)
        move_file(bucket_name, file_path, 'reject')

    if is_inserted:
        trigger_worflow(table_name)


def insert_into_raw(table_name, bucket_name, file_path):

    # get the schema
    util_bucket_name = os.environ["util_bucket_name"]

    storage_client = storage.Client()
    bucket_util = storage_client.bucket(util_bucket_name)
    blob_schema = bucket_util.blob(os.path.join('schemas', 'raw', f'{table_name}.json'))
    schema = json.loads(blob_schema.download_as_string(client=None))

    # get the uri path
    blob_uri = f'gs://{bucket_name}/{file_path}'

    # extract the format 
    if blob_uri.endswith('json'): 
        source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        skip_leading_rows = 0
    elif blob_uri.endswith('csv'):
        source_format = bigquery.SourceFormat.CSV
        skip_leading_rows = 1
    else:
        raise Exception(f'Unknown format for file {blob_uri}')

    # create job to load the file into BigQuery
    bigquery_client = bigquery.Client()
    table_id = bigquery_client.dataset('raw').table(table_name)

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=source_format,
        skip_leading_rows=skip_leading_rows
    )

    load_job = bigquery_client.load_table_from_uri(
        blob_uri,
        table_id,
        location='eu',  # Location must match that of the destination dataset.
        job_config=job_config
    )  # API request

    load_job.result()  # Waits for the job to complete.

    destination_table = bigquery_client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))

   
def trigger_worflow(table_name):

    wkf_project_id = os.environ['wkf_project_id']
    wkf_location = os.environ['wkf_location']

    parent = f'projects/{wkf_project_id}/locations/{wkf_location}/workflows/{table_name}_wkf'

    execution_client = executions_v1.ExecutionsClient()
    execution = execution_client.create_execution(request={"parent": parent})

    print(f'Triggering workflow: {execution.name}')

    # Wait for execution to finish, then print results.
    execution_finished = False
    backoff_delay = 1  # Start wait with delay of 1 second
    print('Poll every second for result...')
    while (not execution_finished):
        execution_finished = execution.state != execution.State.ACTIVE
        # If we haven't seen the result yet, wait a second.
        if not execution_finished:
            print('- Waiting for results...')
            time.sleep(backoff_delay)
            backoff_delay *= 2  # Double the delay to provide exponential backoff.
        else:
            print(f'Execution finished with state: {execution.state.name}')
            print(execution.result)
            return execution.result


def move_file(bucket_name, file_path, new_subfolder):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)

    *subfolder, file = file_path.split('/')  
    new_file_path = os.path.join(new_subfolder, file)
    bucket.rename_blob(blob, new_file_path)

    print(f'{blob.name} moved to {new_file_path}')


if __name__ == '__main__':
    import os
    realpath = os.path.realpath(__file__)
    material_path = os.sep.join(['', *realpath.split(os.sep)[:-4], '__materials__'])
    init_files_path = os.path.join(material_path, 'data', 'init')

    for file_name in [None]:  # os.listdir(init_files_path):
        false_event = {
            'data': 'store',
            'attributes': {
                'bucket_name': 'sandbox-athevenot-magasin-cie-landing',
                'file_path': 'input/store_20220531.csv'

            }
        }

        false_context = {}
        receive_message(false_event, false_context)