import json
import logging
from kafka import KafkaConsumer, TopicPartition

log = logging.getLogger(__name__)

def extract_from_kafka(topic: str, servers: str, group: str, timeout: int, **context):
    log.info(f"Connecting to Kafka at {servers}, topic={topic}")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=servers,
        group_id=group,
        auto_offset_reset="latest",       # ✅ CHANGED: only new messages
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=timeout * 1000,
    )

    records = []
    try:
        for message in consumer:
            records.append(message.value)

        log.info(f"Extracted {len(records)} records from Kafka")

        if not records:
            log.warning("No new records in Kafka topic — skipping pipeline")
            consumer.commit()             # ✅ Still commit even if empty
            return []

        context["ti"].xcom_push(key="raw_records", value=records)

        consumer.commit()                 # ✅ Commit AFTER successful read
        return records

    except Exception as e:
        log.error(f"Kafka extract failed: {e}")
        raise
    finally:
        consumer.close()