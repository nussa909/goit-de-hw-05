from kafka import KafkaProducer
from configs import kafka_config
import json
import uuid
import time
import random


MY_NAME = "nussa"


# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)


# Назва топіку
topic_name = f'{MY_NAME}_building_sensors'
sensor_id = str(uuid.uuid4())

for i in range(10):
    # Відправлення повідомлення в топік
    try:
        data = {
            "timestamp": time.time(),  # Часова мітка
            "sensor_id": sensor_id, #  id датчика
            "temperature":random.randint(25, 45),  # температура
            "humidity": random.randint(15, 85)  # вологість
        }
        producer.send(topic_name, key=str(uuid.uuid4()), value=data)
        producer.flush()  # Очікування, поки всі повідомлення будуть відправлені
        print(f"Message {data} sent to topic '{topic_name}' successfully.")
        time.sleep(2)
    except Exception as e:
        print(f"An error occurred: {e}")


producer.close()