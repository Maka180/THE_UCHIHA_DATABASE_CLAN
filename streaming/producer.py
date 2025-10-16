from kafka import KafkaProducer
import json
import time

# Connect to your local Kafka broker
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'my-first-topic'  # make sure this topic exists

print("🚀 Sending messages to Kafka... Press Ctrl+C to stop.")
try:
    for i in range(5):
        message = {"id": i, "msg": f"Hello from Python {i}"}
        producer.send(topic, message)
        print(f"✅ Sent: {message}")
        time.sleep(1)
    producer.flush()
except KeyboardInterrupt:
    print("🛑 Stopped sending.")
finally:
    producer.close()
