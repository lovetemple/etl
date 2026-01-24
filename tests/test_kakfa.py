//tests/test_kafka_to_raw.py
from core.kafka_client import KafkaClient


from validators.kafka_validations import (
    assert_topic_not_empty,
    assert_required_fields,
    assert_event_freshness,
    assert_unique_keys
)


def test_kafka_topic_quality():
    kafka = KafkaClient(
        brokers="kafka01:9092",
        group_id="dq-validation",
        topic="customers"
    )

    messages = kafka.poll_messages(max_messages=20)

    assert_topic_not_empty(messages)

    assert_required_fields(
        messages,
        required_fields=[
            "customer_id",
            "email",
            "event_ts"
        ]
    )

    assert_event_freshness(
        messages,
        ts_field="event_ts",
        max_age_minutes=60
    )

    assert_unique_keys(
        messages,
        key="customer_id"
    )

    //Optional: Kafka → BQ Reconciliation

//Ensures Kafka volume ≈ Raw table volume

def test_kafka_to_raw_row_count():
    kafka = KafkaClient(
        brokers="kafka01:9092",
        group_id="dq-validation",
        topic="customers"
    )
    messages = kafka.poll_messages(max_messages=1000)

    kafka_count = len(messages)

    from core.bigquery_client import BigQueryClient
    bq = BigQueryClient("my-gcp-project")

    raw_count = bq.count(
        "raw_structured.customers_raw"
    )

    assert raw_count >= kafka_count * 0.95