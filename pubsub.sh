#! /bin/sh
git clone https://github.com/GoogleCloudPlatform/training-data-analyst/
cd ~/training-data-analyst/quests/dataflow_python/
# Change directory into the lab
cd 6_SQL_Streaming_Analytics/solution
export BASE_DIR=$(pwd)
sudo apt-get install -y python3-venv
## Create and activate virtual environment
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
cd $BASE_DIR/../..
source create_streaming_sinks.sh

# Change to the directory containing the practice version of the code
cd $BASE_DIR

bash generate_streaming_events.sh
gcloud pubsub subscritpions create 
gcloud pubsub subscriptions create my_subscription --topic my_topic
gcloud pubsub subscriptions pull --auto-ack --limit=1 my_subscription



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

