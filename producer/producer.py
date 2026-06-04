import json
import os
import random
import time
import uuid

from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer


# ------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------

BROKER = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092"
)

TOPIC_MAIN = os.getenv(
    "CLAIMS_TOPIC",
    "claims-reported"
)

MAX_EVENTS = 50
SLEEP_SECONDS = 2

INVALID_RATE = 0.20
FRAUD_RATE = 0.10

START_DATE = datetime(2023, 1, 1)
END_DATE = datetime.now()


# ------------------------------------------------------------------
# KAFKA PRODUCER
# ------------------------------------------------------------------

producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: v.encode("utf-8"),
    acks="all",
    retries=5
)


# ------------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------------

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def generate_amount():

    invalid = random.random() < INVALID_RATE

    if invalid:
        return None, True

    return random.randint(500, 30000), False


def generate_dates():

    contract_date = START_DATE + timedelta(
        seconds=random.randint(
            0,
            int((END_DATE - START_DATE).total_seconds())
        )
    )

    fraud = random.random() < FRAUD_RATE

    if fraud:
        days_after = random.randint(0, 6)
    else:
        days_after = random.randint(7, 365)

    claim_date = contract_date + timedelta(days=days_after)

    return contract_date, claim_date, fraud


def build_event():

    amount, invalid = generate_amount()

    contract_date, claim_date, fraud = generate_dates()

    customer_id = f"CUST-{random.randint(1, 50)}"

    event = {
        "eventId": str(uuid.uuid4()),
        "eventRes": "ClaimCreated",
        "claimId": f"CLM-{random.randint(1000, 9999)}",
        "customerId": customer_id,
        "amount": amount,
        "contractId": f"CON-{random.randint(5000, 99999)}",
        "contractDate": contract_date.isoformat(),
        "claimDate": claim_date.isoformat(),
        "timestamp": utc_now_iso()
    }

    return event, customer_id, invalid, fraud


# ------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------

def main():

    print(
        f"producer started "
        f"broker={BROKER} "
        f"topic={TOPIC_MAIN}",
        flush=True
    )

    sent = 0
    invalid_count = 0
    fraud_count = 0

    try:

        while sent < MAX_EVENTS:

            event, customer_id, invalid, fraud = build_event()

            future = producer.send(
                topic=TOPIC_MAIN,
                key=customer_id,
                value=event
            )

            result = future.get(timeout=10)

            sent += 1
            invalid_count += int(invalid)
            fraud_count += int(fraud)

            print(
                f"sent "
                f"eventId={event['eventId']} "
                f"customer={customer_id} "
                f"partition={result.partition} "
                f"offset={result.offset} "
                f"invalid={invalid} "
                f"fraud_pattern={fraud}",
                flush=True
            )

            time.sleep(SLEEP_SECONDS)

    except Exception as e:
        print(f"producer error: {e}", flush=True)

    finally:

        producer.flush()
        producer.close()

        print(
            f"finished "
            f"sent={sent} "
            f"invalid_generated={invalid_count} "
            f"fraud_patterns={fraud_count}",
            flush=True
        )


if __name__ == "__main__":
    main()