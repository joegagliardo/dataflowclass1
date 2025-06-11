#! /bin/sh
export BUCKET=gs://$PROJECT
export REGION=us-central1
gsutil cp /home/jupyter/dataflowclass1/datasets/northwind/AVRO/regions/regions.avro $BUCKET
gsutil cp /home/jupyter/dataflowclass1/datasets/northwind/AVRO/territories/territories.avro $BUCKET

python3 dataflow-sql.py --rfile $BUCKET/regions.avro --territoryfile $BUCKET/territories.avro --output $BUCKET/sql_out --temp_location $BUCKET/tmp/ --region $REGION --project $PROJECT runner DataflowRunner