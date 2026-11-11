from kafka import KafkaConsumer
from configs import kafka_config
import json


MY_NAME = "nussa"


# Створення Kafka Consumer
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',  # Зчитування повідомлень з початку
    enable_auto_commit=True,       # Автоматичне підтвердження зчитаних повідомлень
    group_id=f'{MY_NAME}_alerts_consumer_group'   # Ідентифікатор групи споживачів
)


# Назва топіку
temperature_topic_name = f'{MY_NAME}_temperature_alerts'
humidity_topic_name = f'{MY_NAME}_humidity_alerts'


# Підписка на тему
consumer.subscribe([temperature_topic_name, humidity_topic_name])


print(f"Subscribed to topic {temperature_topic_name}, {humidity_topic_name}")


# Обробка повідомлень з топіку
try:
    for message in consumer:
        print(f"Message: {message.value}")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()  # Закриття consumer