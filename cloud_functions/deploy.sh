if [ "$#" -eq 0 ]; 
then
    PROJECT_ID="sandbox-athevenot"
    echo 'No argument supplied: default PROJECT_ID to "${PROJECT_ID}"'
else
    PROJECT_ID=$1
fi

# gcloud functions deploy cf_trigger_on_file \
#     --project=$PROJECT_ID \
#     --region=europe-west1 \
#     --entry-point=check_file_format \
#     --runtime=python39 \
#     --source=./cloud_functions/cf_trigger_on_file/src \
#     --trigger-resource $PROJECT_ID-magasin-cie-landing \
#     --trigger-event google.storage.object.finalize \
#     --env-vars-file=./cloud_functions/cf_trigger_on_file/env.yaml
    
gcloud functions deploy cf_dispatch_workflow \
    --project=$PROJECT_ID \
    --region=europe-west1 \
    --entry-point=receive_messages \
    --runtime=python39 \
    --source=./cloud_functions/cf_dispatch_workflow/src \
    --trigger-topic=valid_file \
    --env-vars-file=./cloud_functions/cf_dispatch_workflow/env.yaml  