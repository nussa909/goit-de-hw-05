from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config


MY_NAME = "nussa"


# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)


def create_new_topic(topic_name:str, num_partitions:int, replication_factor:int): 
    new_topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)

    # Створення нового топіку
    try:
        admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        print(f"Topic '{topic_name}' created successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")


num_partitions = 2
replication_factor = 1
create_new_topic(f'{MY_NAME}_building_sensors', num_partitions, replication_factor)
create_new_topic(f'{MY_NAME}_temperature_alerts', num_partitions, replication_factor)
create_new_topic(f'{MY_NAME}_humidity_alerts', num_partitions, replication_factor)

# Перевіряємо список існуючих топіків
my_topics = [topic for topic in admin_client.list_topics() if "nussa_" in topic]
print(f"My topics: {my_topics}")


# Закриття зв'язку з клієнтом
admin_client.close()