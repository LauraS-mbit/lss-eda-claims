import json
import os
from datetime import datetime, timezone

import redis
from kafka import KafkaConsumer, KafkaProducer


# ---------------- CONFIG ----------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "claims-reported")

OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "payment-decisions")
FRAUD_TOPIC = os.getenv("FRAUD_TOPIC", "fraud-events")

KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "blocker-group")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

HIGH_RISK_AMOUNT = int(os.getenv("HIGH_RISK_AMOUNT", "10000"))
PROCESSED_EVENTS_KEY = "processed_claim_events"


# ---------------- UTILS ----------------

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def log(msg):
    print(msg, flush=True)


# ---------------- VALIDATION ----------------

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

    for f in required_fields:
        if f not in event or event[f] in (None, ""):
            raise ValueError(f"missing field {f}")

    if event["eventRes"] != "ClaimCreated":
        raise ValueError("eventRes must be ClaimCreated")

    if event["amount"] is None:
        raise ValueError("amount is required")


# ---------------- IDEMPOTENCY ----------------

def mark_if_new(redis_client, event_id):
    return redis_client.set(
        f"{PROCESSED_EVENTS_KEY}:{event_id}",
        "1",
        nx=True,
        ex=86400
    )


# ---------------- DECISION ENGINE ----------------

def evaluate_payment(event, threshold):

    reasons = []

    amount = event.get("amount")
    if amount is not None and float(amount) > threshold:
        reasons.append("high_amount")

    contract = event.get("contractDate")
    claim = event.get("claimDate")

    if contract and claim:
        c = datetime.fromisoformat(contract.replace("Z", "+00:00"))
        d = datetime.fromisoformat(claim.replace("Z", "+00:00"))

        if (d - c).days < 7:
            reasons.append("early_claim")

    return ("BLOCKED" if reasons else "APPROVED"), reasons


# ---------------- MAIN ----------------

def main():

    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
    )

    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        acks="all",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    log(f"blocker started input={INPUT_TOPIC}")

    try:

        for message in consumer:

            event = message.value
            should_commit = False

            try:

                validate_event(event)

                event_id = str(event["eventId"])
                claim_id = str(event["claimId"])

                if not mark_if_new(redis_client, event_id):
                    log(f"duplicate ignored eventId={event_id}")
                    should_commit = True
                    continue

                decision, reasons = evaluate_payment(event, HIGH_RISK_AMOUNT)

                output_event = {
                    **event,
                    "decision": decision,
                    "reasons": reasons,
                }

                # ✔ SIEMPRE publicar decisión final
                producer.send(OUTPUT_TOPIC, value=output_event).get(timeout=10)

                # ✔ SOLO si es fraude, duplicar a stream de fraude
                if decision == "BLOCKED":
                    producer.send(FRAUD_TOPIC, value=output_event).get(timeout=10)

                log(f"{decision} claimId={claim_id} reasons={reasons}")

                should_commit = True

            except Exception as e:
                log(f"error processing event: {e}")
                should_commit = True

            if should_commit:
                consumer.commit()

    except KeyboardInterrupt:
        log("stopped")

    finally:
        producer.flush()
        producer.close()
        consumer.close()


if __name__ == "__main__":
    main()