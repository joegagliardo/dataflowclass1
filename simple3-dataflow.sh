#! /bin/sh
export BUCKET=gs://qwiklabs-gcp-02-d69631df9d97
export REGION=us-west1
python3 simple3.py --input $BUCKET/regions.csv --output $BUCKET/regions_out --temp_location gs://$BUCKET/tmp/ \
  --region $REGION --project $PROJECT --runner DataflowRunner
