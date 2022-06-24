if [ "$#" -eq 0 ]; 
then
    PROJECT_ID="sandbox-athevenot"
    echo 'No argument supplied: default PROJECT_ID to "${PROJECT_ID}"'
else
    PROJECT_ID=$1
fi

gsutil -m cp ./schemas/raw/* gs://$PROJECT_ID-magasin-cie-utils/schemas/raw
gsutil -m cp ./schemas/staging/* gs://$PROJECT_ID-magasin-cie-utils/schemas/staging
gsutil -m cp ./schemas/cleaned/* gs://$PROJECT_ID-magasin-cie-utils/schemas/cleaned
gsutil -m cp ./schemas/aggregated/* gs://$PROJECT_ID-magasin-cie-utils/schemas/aggregated
