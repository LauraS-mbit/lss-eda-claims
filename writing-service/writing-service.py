import os
import json
import time
import psycopg2
from kafka import KafkaConsumer

# ---------------- CONFIG ----------------

TOPIC = os.getenv("TOPIC", "claims-valid")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")


# ---------------- DB CONNECTION ----------------

def create_db_connection():
    while True:
        try:
            print("[DB-WRITER] connecting to Postgres...")
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                port=POSTGRES_PORT
            )
            print("[DB-WRITER] connected to PostgreSQL ✔")
            return conn

        except Exception as e:
            print("[DB-WRITER] waiting for PostgreSQL...", e)
            time.sleep(5)


# ---------------- INIT TABLE ----------------

def init_table(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS claims (
        id SERIAL PRIMARY KEY,
        claimId TEXT,
        contractId TEXT,
        customerId TEXT,
        amount FLOAT,
        contractDate TEXT,
        claimDate TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)


# ---------------- INSERT ----------------

def insert_claim(cur, data):
    cur.execute("""
        INSERT INTO claims (
            claimId,
            contractId,
            customerId,
            amount,
            contractDate,
            claimDate
        )
        VALUES (%s,%s,%s,%s,%s,%s)
    """, (
        data.get("claimId"),
        data.get("contractId"),
        data.get("customerId"),
        data.get("amount"),
        data.get("contractDate"),
        data.get("claimDate")
    ))


# ---------------- MAIN ----------------

def main():

    print("[DB-WRITER] starting service...")
    print(f"[DB-WRITER] Kafka topic = {TOPIC}")
    print(f"[DB-WRITER] Kafka broker = {KAFKA_BOOTSTRAP_SERVERS}")

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id="db-writer-group"
    )

    print("[DB-WRITER] Kafka consumer initialized ✔")

    conn = create_db_connection()
    cur = conn.cursor()

    init_table(cur)
    conn.commit()

    print("[DB-WRITER] listening for events...")

    try:
        for msg in consumer:

            print("\n[DB-WRITER] ------------------------------")
            print("[DB-WRITER] message received from Kafka ✔")

            data = msg.value

            print("[DB-WRITER] payload:", data)

            try:
                insert_claim(cur, data)
                conn.commit()

                print("[DB-WRITER] INSERTED:", data.get("claimId"))
                print("[DB-WRITER] DB commit ✔")

                consumer.commit()
                print("[DB-WRITER] Kafka offset committed ✔")

            except Exception as e:
                conn.rollback()
                print("[DB-WRITER] ERROR inserting:", e)

    except KeyboardInterrupt:
        print("[DB-WRITER] stopping service...")

    finally:
        cur.close()
        conn.close()
        consumer.close()
        print("[DB-WRITER] closed connections")


if __name__ == "__main__":
    main()