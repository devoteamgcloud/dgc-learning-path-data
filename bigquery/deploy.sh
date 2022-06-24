if [ "$#" -eq 0 ]; 
then
    PROJECT_ID="sandbox-athevenot"
    echo 'No argument supplied: default PROJECT_ID to "${PROJECT_ID}"'
else
    PROJECT_ID=$1
fi

# to avoid looping over empty directories
shopt -s nullglob

# create all the datasets
for dataset_path in ./schemas/*/; do

    dataset=$(basename $dataset_path)
    echo "create dataset ${PROJECT_ID}.${dataset}..."

    bq mk --location eu $PROJECT_ID:$dataset

    # create all the tables in the dataset
    for schema in ./schemas/$dataset/*.json; do
        table=$(basename $schema | cut -d'.' -f1)

        echo "create table ${PROJECT_ID}.${dataset}.${table}..."
        bq mk -f --project_id=$PROJECT_ID --location eu --schema=$schema -t $dataset.$table

    done
done
