import os
import datetime
from google.cloud import storage
from google.cloud import pubsub_v1



FILES_AND_EXTENSION = {
    'store': 'csv',
    'customer': 'csv',
    'basket': 'json'
}


def check_file_format(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload. 
                       https://cloud.google.com/storage/docs/json_api/v1/objects#resource-representations
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file_event = event

    print(f"\nProcessing file: {file_event['name']}.")

    # isolate the subfolder, the file name and its extension
    bucket_name = file_event["bucket"]
    file_path = file_event['name']


    *subfolder, file = file_path.split('/')  
    subfolder =  os.path.join(*subfolder) if subfolder != [] else ''
    file_name, file_extention = file.split('.') 

    print(f'Bucket name: {bucket_name}')
    print(f'File path: {file_path}')
    print(f'Subfolder: {subfolder}')
    print(f'Full file name: {file}')
    print(f'File name: {file_name}')
    print(f'File Extension: {file_extention}')

    is_valid = False
    # Check if the file is in the subfolder `input/`
    # if subfolder in ('invalid', 'reject'):
    #     raise Exception(f'{file} is in {subfolder}/ subfolder: we do not process it')
    # else:
    assert subfolder == 'input', 'File must be in `input/ subfolder to be processed`'
    
    try:
        # check if the file name has the good format
        ## check first it has two parts
        parts = file_name.split('_')
        assert len(parts) == 2, 'File must have a 2-parts format as `<table_name>_<date>`'

        ## then check the name is a correct accepted file
        table_name, date_string = parts
        accepted_names = list(FILES_AND_EXTENSION.keys())
        assert table_name in accepted_names, f'Only files for {accepted_names} are accepted'

        ## then check the date format is clear 
        ## (will raise an error if it can not be parsed)
        date_format = '%Y%m%d'
        datetime.datetime.strptime(date_string, date_format)

        # check the extension of the file
        expected_extension = FILES_AND_EXTENSION[table_name]
        assert file_extention == expected_extension, f'{table_name} file expected to have {expected_extension} extension' 

        is_valid = True

    except Exception as e:
        print(e)


    if is_valid:
        project_id = os.environ["pubsub_project_id"]
        topic_id = os.environ["pubsub_topic_id"]

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_id)
        # send the name of the workflow to trigger as data 
        data = table_name.encode('utf-8')
        # Add two attributes, bucket and file_path, to the message
        future = publisher.publish(
            topic_path, data, bucket_name=bucket_name, file_path=file_path
        )
        print(future.result())
        print(f'Published messages with custom attributes to {topic_path}.')
    else:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        blob = bucket.blob(file_path)
        new_name = os.path.join('invalid', file)
        bucket.rename_blob(blob, new_name)

        print(f'{blob.name} moved to {new_name}')


if __name__ == '__main__':
    import os
    realpath = os.path.realpath(__file__)
    material_path = os.sep.join(['', *realpath.split(os.sep)[:-4], '__materials__'])
    init_files_path = os.path.join(material_path, 'data', 'init')

    for file_name in os.listdir(init_files_path):
        false_event = {
            'bucket': 'sandbox-athevenot-magasin-cie-landing',
            'name': os.path.join('input', file_name)
        }

        false_context = {}
        check_file_format(false_event, false_context)

    

