#! /bin/sh
# git clone https://github.com/GoogleCloudPlatform/training-data-analyst/
cd ~/training-data-analyst/quests/dataflow_python/6_SQL_Streaming_Analytics/solution
export BASE_DIR=$(pwd)
sudo apt-get install -y python3-venv
python3 -m venv df-env
source df-env/bin/activate
python3 -m pip install -q --upgrade pip setuptools wheel
python3 -m pip install apache-beam[gcp]
gcloud services enable dataflow.googleapis.com
PROJECT_ID=$(gcloud config get-value project)
export PROJECT_NUMBER=$(gcloud projects list --filter="$PROJECT_ID" --format="value(PROJECT_NUMBER)")
export serviceAccount=""$PROJECT_NUMBER"-compute@developer.gserviceaccount.com"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${serviceAccount}" --role="roles/dataflow.worker"
# Create GCS buckets and BQ dataset
#source ~/training-data-analyst/quests/dataflow_python/create_streaming_sinks.sh
cd ~/training-data-analyst/quests/dataflow_python/
bash ~/training-data-analyst/quests/dataflow_python/generate_streaming_events.sh
cd $BASE_DIR
# gcloud pubsub subscriptions create my_subscription --topic my_topic
# gcloud pubsub subscriptions pull --auto-ack --limit=1 my_subscription



# [
#     {
#         "name": "event_timestamp",
#         "type": "TIMESTAMP",
#         "description": "Pub/Sub event timestamp",
#         "mode": "REQUIRED"
#     },
#     {
#         "name": "http_request",
#         "type": "STRING",
#         "mode": "NULLABLE"
#     },
#     {
#         "name": "http_response",
#         "type": "INT64"
#     },
#     {
#         "name": "user_id",
#         "type": "STRING"
#     }
# ]

# gcloud dataflow sql query 'SELECT
#   user_id,
#   COUNT(http_request) AS request_cnt,
#   TUMBLE_START('INTERVAL 10 MINUTES') AS window_start
# FROM
#   pubsub.topic.`qwiklabs-gcp-04-9bb4f5119a5c`.`my_topic`
# GROUP BY
#   user_id,
#   TUMBLE(event_timestamp,
#     'INTERVAL 10 MINUTE')
# ' --job-name dfsql-6bcf6030-179015f849a --region us-central1 --bigquery-write-disposition write-empty --bigquery-project qwiklabs-gcp-04-9bb4f5119a5c --bigquery-dataset logs --bigquery-table logs_df
