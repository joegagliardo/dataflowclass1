gcloud pubsub subscriptions create my_subscription --topic my_topic
gcloud pubsub subscriptions pull --auto-ack --limit=1 my_subscription



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

gcloud dataflow sql query 'SELECT
  user_id,
  COUNT(http_request) AS request_cnt,
  TUMBLE_START('INTERVAL 10 MINUTES') AS window_start
FROM
  pubsub.topic.`qwiklabs-gcp-04-9bb4f5119a5c`.`my_topic`
GROUP BY
  user_id,
  TUMBLE(event_timestamp,
    'INTERVAL 10 MINUTE')
' --job-name dfsql-6bcf6030-179015f849a --region us-central1 --bigquery-write-disposition write-empty --bigquery-project qwiklabs-gcp-04-9bb4f5119a5c --bigquery-dataset logs --bigquery-table logs_df
