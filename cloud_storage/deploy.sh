
if [ "$#" -eq 0 ]; 
then
    PROJECT_ID="sandbox-athevenot"
    echo 'No argument supplied: default PROJECT_ID to "${PROJECT_ID}"'
else
    PROJECT_ID=$1
fi

gsutil mb -p $PROJECT_ID -l eu gs://$PROJECT_ID-magasin-cie-landing
gsutil mb -p $PROJECT_ID -l eu gs://$PROJECT_ID-magasin-cie-utils

gsutil lifecycle set ./cloud_storage/magasin-cie-landing_lifecycle.json gs://sandbox-athevenot-magasin-cie-landing

