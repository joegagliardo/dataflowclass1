#! /bin/sh
python3 -m apache_beam.examples.wordcount \
  --region $REGION \
  --input gs://dataflow-samples/shakespeare/kinglear.txt \
  --output gs://$BUCKET/results/outputs \
  --runner DataflowRunner \
  --project $PROJECT \
  --temp_location gs://$BUCKET/tmp/

gsutil ls -lh "gs://$BUCKET/results/outputs*"  
gsutil cat "gs://$BUCKET/results/outputs*"
  
