from kafka import KafkaConsumer
from kafka import KafkaProducer
from configs import kafka_config
import json
import uuid


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
    group_id=f'{MY_NAME}_processing_group'   # Ідентифікатор групи споживачів
)

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
sensors_topic_name = f'{MY_NAME}_building_sensors'
temperature_topic_name = f'{MY_NAME}_temperature_alerts'
humidity_topic_name = f'{MY_NAME}_humidity_alerts'


# Підписка на тему
consumer.subscribe([sensors_topic_name])


print(f"Subscribed to topic '{sensors_topic_name}'")


# Обробка повідомлень з топіку
try:
    for message in consumer:
        print(f"Received message: {message.value}")
        
        timestamp = message.value['timestamp']
        sensor_id = message.value['sensor_id']
        temperature = message.value['temperature']
        humidity = message.value['humidity']
        
        if temperature > 40:
            # генерує сповіщення і відправляє його в топік temperature_alerts
            data = {
                "sensor_id": sensor_id, #  id датчика
                "timestamp": timestamp,  # Часова мітка
                "temperature":temperature,  # температура
                'message' : f"Alert: temperature is too high ({temperature}°C)!"
            }
            producer.send( temperature_topic_name, key=str(uuid.uuid4()), value=data)
            producer.flush()  # Очікування, поки всі повідомлення будуть відправлені
            print(f"Message sent to topic '{temperature_topic_name}' successfully.")

        if humidity > 80 or humidity < 20:
            #генерує сповіщення і відправляє його в топік humidity_alerts
            data = {
                "sensor_id": sensor_id, #  id датчика
                "timestamp": timestamp,  # Часова мітка
                "humidity": humidity,  # вологість
                'message' : f"Alert: humidity is out of order({humidity}%)!"
            }
            producer.send( humidity_topic_name, key=str(uuid.uuid4()), value=data)
            producer.flush()  # Очікування, поки всі повідомлення будуть відправлені
            print(f"Message sent to topic '{humidity_topic_name}' successfully.")
        
         
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()  # Закриття consumer
    producer.close() 