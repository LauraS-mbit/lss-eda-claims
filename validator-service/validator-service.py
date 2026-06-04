import json
import os
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer


# ------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

INPUT_TOPIC = os.getenv("INPUT_TOPIC", "claims-reported")
VALID_TOPIC = os.getenv("VALID_TOPIC", "claims-valid")
DLQ_TOPIC = os.getenv("DLQ_TOPIC","claims-dlq")

GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP","validator-group")

# ------------------------------------------------------------------
# UTILS
# ------------------------------------------------------------------

def utc_now_iso():
    return (
        datetime.now(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def log(msg):
    print(msg, flush=True)


# ------------------------------------------------------------------
# VALIDATION
# ------------------------------------------------------------------

def validate_event(event):

    if not isinstance(event, dict):
        raise ValueError("event must be a JSON object")

    required_fields = [
        "eventId",
        "claimId",
        "customerId",
        "amount",
        "contractDate",
        "claimDate",
        "timestamp",
        "eventRes",
    ]

    for field in required_fields:
        if field not in event:
            raise ValueError(f"missing field {field}")

        if event[field] in (None, ""):
            raise ValueError(f"empty field {field}")

    if event["eventRes"] != "ClaimCreated":
        raise ValueError("eventRes must be ClaimCreated")

    amount = event["amount"]

    if not isinstance(amount, (int, float)):
        raise ValueError("amount must be numeric")

    if amount <= 0:
        raise ValueError("amount must be greater than zero")

    contract = datetime.fromisoformat(
        event["contractDate"].replace("Z", "+00:00")
    )

    claim = datetime.fromisoformat(
        event["claimDate"].replace("Z", "+00:00")
    )

    if claim < contract:
        raise ValueError(
            "claimDate cannot be earlier than contractDate"
        )


# ------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------

def main():

    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        acks="all",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8")
    )

    log(
        f"validator started "
        f"input={INPUT_TOPIC} "
        f"valid={VALID_TOPIC} "
        f"dlq={DLQ_TOPIC}"
    )

    try:

        for message in consumer:

            event = message.value

            try:

                validate_event(event)

                customer_id = str(event["customerId"])

                producer.send(
                    VALID_TOPIC,
                    key=customer_id,
                    value=event
                ).get(timeout=10)

                log(
                    f"VALID "
                    f"eventId={event['eventId']} "
                    f"claimId={event['claimId']}"
                )

            except Exception as e:

                dlq_event = {
                    "error": str(e),
                    "failedAt": utc_now_iso(),
                    "originalEvent": event
                }

                producer.send(
                    DLQ_TOPIC,
                    value=dlq_event
                ).get(timeout=10)

                log(
                    f"INVALID "
                    f"eventId={event.get('eventId', 'unknown')} "
                    f"reason={e}"
                )

            finally:
                consumer.commit()

    except KeyboardInterrupt:
        log("validator stopped")

    finally:
        producer.flush()
        producer.close()
        consumer.close()


if __name__ == "__main__":
    main()