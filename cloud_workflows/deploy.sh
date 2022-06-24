
if [ "$#" -eq 0 ]; 
then
    PROJECT_ID="sandbox-athevenot"
    echo 'No argument supplied: default PROJECT_ID to "${PROJECT_ID}"'
else
    PROJECT_ID=$1
fi

# create all the workflows
for workflow_file in ./cloud_workflows/*.yaml; do
    workflow_name=$(basename $workflow_file | cut -d'.' -f1)

    echo "create workflow ${workflow_name}..."
    gcloud workflows deploy $workflow_name --location=europe-west1 --source=$workflow_file

done

