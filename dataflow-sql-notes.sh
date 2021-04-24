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

SELECT user_id, COUNT(http_request) as request_cnt, TUMBLE_START('INTERVAL 2 MINUTE') as window_start
FROM pubsub.topic.`qwiklabs-gcp-02-70a385c1381f`.`my_topic`
GROUP BY user_id, TUMBLE(event_timestamp, 'INTERVAL 2 MINUTE')


gcloud dataflow sql query "SELECT user_id, COUNT(http_request) as request_cnt, TUMBLE_START('INTERVAL 2 MINUTE') as window_start FROM pubsub.topic.\`"$PROJECT_ID"\`.\`my_topic\` GROUP BY user_id, TUMBLE(event_timestamp, 'INTERVAL 2 MINUTE')" \
--job-name dfsql-my-topic --region us-central1 --bigquery-write-disposition write-empty --bigquery-project $PROJECT_ID \
--bigquery-dataset logs --bigquery-table logs_df
