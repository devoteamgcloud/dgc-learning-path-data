if [ "$#" -eq 0 ]; 
then
    PROJECT_ID="sandbox-athevenot"
    echo 'No argument supplied: default PROJECT_ID to "${PROJECT_ID}"'
else
    PROJECT_ID=$1
fi

gsutil -m cp ./queries/raw/* gs://$PROJECT_ID-magasin-cie-utils/queries/raw
gsutil -m cp ./queries/staging/* gs://$PROJECT_ID-magasin-cie-utils/queries/staging
gsutil -m cp ./queries/cleaned/* gs://$PROJECT_ID-magasin-cie-utils/queries/cleaned
gsutil -m cp ./queries/aggregated/* gs://$PROJECT_ID-magasin-cie-utils/queries/aggregated