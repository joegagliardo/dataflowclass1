#! /bin/sh
export BUCKET=gs://$PROJECT
export REGION=us-central1
python3 simple3.py --input $BUCKET/regions.csv --output $BUCKET/regions_out \
--temp_location gs://$BUCKET/tmp/ --region $REGION --project $PROJECT --runner DataflowRunner
