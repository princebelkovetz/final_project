import os
import sys
import pandas as pd
import time
import logging
import json
from datetime import datetime

from confluent_kafka import Consumer, Producer, KafkaError

sys.path.append(os.path.abspath('./src'))
from preprocessing import load_train_data, run_preproc
from scorer import make_pred

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/service.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Set kafka configuration file
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
FLIGHTS_TOPIC = os.getenv("KAFKA_FLIGHTS_TOPIC", "flights")
SCORING_TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "scoring")


class ProcessingService:
    def __init__(self):
        self.consumer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'ml-scorer',
            'auto.offset.reset': 'earliest'
        }
        self.producer_config = {
             'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS
             }
        self.consumer = Consumer(self.consumer_config)
        self.consumer.subscribe([FLIGHTS_TOPIC])
        self.producer = Producer(self.producer_config)
        
        # Загрузка данных для препроцессинга
        self.train = load_train_data()

    def process_messages(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue
            try:
                # Десериализация JSON
                data = json.loads(msg.value().decode('utf-8'))

                # Извлекаем ID и данные
                flight_id = data['id']
                input_df = pd.DataFrame([data['data']])

                # Препроцессинг и предсказание
                processed_df = run_preproc(self.train, input_df)
                submission = make_pred(processed_df, "kafka_stream")

                # Extract original features from input (before preprocessing)
                original_data = data['data']  # this is the dict from Kafka

                # Build enriched result
                result = {
                    "id": flight_id,
                    "score": float(submission["score"].iloc[0]),
                    "satisfaction_flag": int(submission["satisfaction_flag"].iloc[0]),
                    # Include original categorical features for analytics
                    "Gender": original_data.get("Gender"),
                    "Class": original_data.get("Class"),
                    "Customer Type": original_data.get("Customer Type"),
                    "Type of Travel": original_data.get("Type of Travel")
                }

                self.producer.produce(
                    'scoring',
                    value=json.dumps(result)
                )
                self.producer.flush()
            except Exception as e:
                logger.error(f"Error processing message: {e}")


if __name__ == "__main__":
    logger.info('Starting Kafka ML scoring service...')
    service = ProcessingService()
    try:
        service.process_messages()
    except KeyboardInterrupt:
        logger.info('Service stopped by user')