if [ "$#" -eq 0 ]; 
then
    PROJECT_ID="sandbox-athevenot"
    echo 'No argument supplied: default PROJECT_ID to "${PROJECT_ID}"'
else
    PROJECT_ID=$1
fi

gcloud pubsub topics create valid_file --project=$PROJECT_ID