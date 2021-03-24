#! /bin/sh
BUCKET=gs://dataflowclass1-bucket
python3 simple3.py --input $BUCKET/regions.csv --output $BUCKET/regions_out

