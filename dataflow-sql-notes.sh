gcloud pubsub subscriptions create my_subscription --topic my_topic && gcloud pubsub subscriptions pull --auto-ack --limit=1 my_subscription

[
    {
        "name": "event_timestamp",
        "type": "TIMESTAMP",
        "description": "Pub/Sub event timestamp",
        "mode": "REQUIRED"
    },
    {
        "name": "http_request",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "http_response",
        "type": "INT64"
    },
    {
        "name": "user_id",
        "type": "STRING"
    }
]


gcloud components update

sudo apt-get update && sudo apt-get --only-upgrade install google-cloud-sdk-config-connector google-cloud-sdk-kubectl-oidc google-cloud-sdk google-cloud-sdk-skaffold kubectl google-cloud-sdk-app-engine-grpc google-cloud-sdk-app-engine-java google-cloud-sdk-firestore-emulator google-cloud-sdk-bigtable-emulator google-cloud-sdk-minikube google-cloud-sdk-anthos-auth google-cloud-sdk-pubsub-emulator google-cloud-sdk-datastore-emulator google-cloud-sdk-app-engine-go google-cloud-sdk-spanner-emulator google-cloud-sdk-local-extract google-cloud-sdk-app-engine-python google-cloud-sdk-datalab google-cloud-sdk-cbt google-cloud-sdk-kpt google-cloud-sdk-gke-gcloud-auth-plugin google-cloud-sdk-app-engine-python-extras google-cloud-sdk-cloud-build-local

gcloud data-catalog entries update \
--lookup-entry='pubsub.topic.`qwiklabs-gcp-02-d0ca2f823b47`.my_topic' \
--schema-from-file=my_topic_schema.yaml

SELECT user_id, COUNT(http_request) as request_cnt, TUMBLE_START('INTERVAL 2 MINUTE') as window_start
FROM pubsub.topic.`qwiklabs-gcp-02-70a385c1381f`.`my_topic`
GROUP BY user_id, TUMBLE(event_timestamp, 'INTERVAL 2 MINUTE')


gcloud dataflow sql query "SELECT user_id, COUNT(http_request) as request_cnt, TUMBLE_START('INTERVAL 2 MINUTE') as window_start FROM pubsub.topic.\`"$PROJECT_ID"\`.\`my_topic\` GROUP BY user_id, TUMBLE(event_timestamp, 'INTERVAL 2 MINUTE')" \
--job-name dfsql-my-topic --region us-central1 --bigquery-write-disposition write-empty --bigquery-project $PROJECT_ID \
--bigquery-dataset logs --bigquery-table logs_df
