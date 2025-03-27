from kafka import KafkaConsumer
from configs import (
    kafka_config,
    humidity_alerts_topic_name,
    temperature_alerts_topic_name,
)
import json

consumer = KafkaConsumer(
    humidity_alerts_topic_name,
    temperature_alerts_topic_name,  # Підписка відразу на обидва топіка
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="alert_listener",
)

print(
    f"Subscribed to {temperature_alerts_topic_name} and {humidity_alerts_topic_name}. Waiting for messages...\n"
)

try:
    for message in consumer:
        print(f"ALERT from topic '{message.topic}': {message.value}")

except KeyboardInterrupt:
    print("Simulation stopped.")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()
