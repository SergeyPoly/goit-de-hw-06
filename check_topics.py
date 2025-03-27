from kafka.admin import KafkaAdminClient, NewTopic
from configs import (
    kafka_config,
    my_name,
)

# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
)

# Перевіряємо список створених топіків
[print(topic) for topic in admin_client.list_topics() if my_name in topic]

# Закриття зв'язку з клієнтом
admin_client.close()
