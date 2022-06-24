if [ "$#" -eq 0 ]; 
then
    PROJECT_ID="sandbox-athevenot"
    echo 'No argument supplied: default PROJECT_ID to "${PROJECT_ID}"'
else
    PROJECT_ID=$1
fi

# sh ./cloud_storage/deploy.sh $PROJECT_ID

# sh ./schemas/deploy.sh $PROJECT_ID
# sh ./queries/deploy.sh $PROJECT_ID
# sh ./bigquery/deploy.sh $PROJECT_ID

# sh ./cloud_functions/deploy.sh $PROJECT_ID
sh ./cloud_workflows/deploy.sh $PROJECT_ID


# sh ./pubsub/deploy.sh $PROJECT_ID
