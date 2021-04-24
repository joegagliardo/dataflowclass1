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
