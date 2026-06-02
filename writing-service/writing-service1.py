import os
import json
import time
import psycopg2
from kafka import KafkaConsumer


# ---------------- CONFIG ----------------

TOPIC = os.getenv("TOPIC", "claims-reported")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")


# ---------------- DB ----------------

def create_db_connection():

    while True:
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                port=POSTGRES_PORT
            )

            print("Conectado a PostgreSQL")
            return conn

        except Exception as e:
            print("Esperando PostgreSQL...", e)
            time.sleep(5)


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

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id="db-writer-group"
    )

    conn = create_db_connection()
    cur = conn.cursor()

    init_table(cur)
    conn.commit()

    print(f"[DB-WRITER] listening on topic={TOPIC}")

    try:

        for msg in consumer:
            data = msg.value

            try:
                insert_claim(cur, data)
                conn.commit()

                print("INSERTED:", data.get("claimId"))

                consumer.commit()

            except Exception as e:
                conn.rollback()
                print("ERROR inserting:", e)

    except KeyboardInterrupt:
        print("stopping...")

    finally:
        cur.close()
        conn.close()
        consumer.close()


if __name__ == "__main__":
    main()