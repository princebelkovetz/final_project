import os
import json
import logging
import psycopg2
from confluent_kafka import Consumer
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/db_writer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCORING_TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "scoring")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "scoring-db-writer")

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "fraud_db")
DB_USER = os.getenv("DB_USER", "fraud_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "fraud_pass")

def create_table_if_not_exists(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS scoring_results (
                id VARCHAR(255) PRIMARY KEY,
                score FLOAT NOT NULL,
                satisfaction_flag INTEGER NOT NULL CHECK (satisfaction_flag IN (0, 1)),
                gender TEXT,
                class TEXT,
                customer_type TEXT,
                type_of_travel TEXT,
                timestamp TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        conn.commit()
    logger.info("Ensured scoring_results table exists with all columns.")

def insert_record(conn, record):
    with conn.cursor() as cur:
        for row in record:
            cur.execute("""
                INSERT INTO scoring_results (
                    id, score, satisfaction_flag,
                    gender, class, customer_type, type_of_travel
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, (
                row.get("id"),
                row.get("score"),
                row.get("satisfaction_flag"),
                row.get("Gender"),
                row.get("Class"),
                row.get("Customer Type"),
                row.get("Type of Travel")
            ))
        conn.commit()

def main():
    logger.info("Starting Scoring DB Writer service...")

    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        create_table_if_not_exists(conn)
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise

    consumer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([SCORING_TOPIC])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue

            try:
                data_str = msg.value().decode('utf-8')
                records = json.loads(data_str) 

                if isinstance(records, dict):
                    records = [records] 

                insert_record(conn, records)
                logger.info(f"Inserted {len(records)} record(s) into DB.")
            except Exception as e:
                logger.error(f"Error processing message: {e}")

    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        consumer.close()
        conn.close()

if __name__ == "__main__":
    main()