from kafka import KafkaConsumer
import json

def safe_deserialize(message):
    try:
        if message is None or message.strip() == b"":
            return None
        return json.loads(message.decode('utf-8'))
    except Exception as e:
        print(f"⚠️ Could not decode message: {message} ({e})")
        return None

consumer = KafkaConsumer(
    'my-first-topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-python-group',
    value_deserializer=safe_deserialize
)

print("🔎 Listening for messages...")
for message in consumer:
    if message.value is not None:
        print(f"📩 Received: {message.value}")

