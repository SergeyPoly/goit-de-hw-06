from kafka import KafkaProducer
from configs import kafka_config, building_sensors_topic_name
import json
import uuid
import time
import random
from datetime import datetime

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: str(v).encode("utf-8"),
)

# Генерація ID датчика (унікальний для кожного запуску)
sensor_id = random.randint(1000, 9999)

# Імітація роботи датчика
try:
    while True:
        data = {
            "sensor_id": sensor_id,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "temperature": random.randint(25, 45),  # Температура від 25 до 45
            "humidity": random.randint(15, 85),  # Вологість від 15 до 85
        }

        producer.send(building_sensors_topic_name, key=str(uuid.uuid4()), value=data)
        producer.flush()  # Очікування, поки всі повідомлення будуть відправлені
        print(
            f"Message sent to topic '{building_sensors_topic_name}' successfully.\nData: {data}\n"
        )

        time.sleep(2)  # Затримка у 2 секунди
except KeyboardInterrupt:
    print("Simulation stopped.")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    producer.close()  # Закриття producer
