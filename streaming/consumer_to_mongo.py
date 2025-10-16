from kafka import KafkaConsumer
from pymongo import MongoClient
import json

# === MongoDB Connection ===
MONGO_URI = "mongodb+srv://lacyfarasi09_db_user:DBMaka08@cluster0.tff7boz.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(MONGO_URI)
db = client["ClearVueBI"]
collection = db["KafkaMessages"]

print("✅ Connected to MongoDB Atlas and ready to store messages!")

# === Safe JSON decoding ===
def safe_deserialize(message):
    try:
        return json.loads(message.decode('utf-8'))
    except Exception:
        print(f"⚠️ Non-JSON message skipped: {message}")
        return {"raw_message": message.decode('utf-8')}

# === Kafka Consumer Setup ===
consumer = KafkaConsumer(
    'my-first-topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='mongo-consumer-group',
    value_deserializer=safe_deserialize
)

print("🔎 Listening for Kafka messages...")

for message in consumer:
    data = message.value
    if data:
        collection.insert_one(data)
        print(f"💾 Saved to MongoDB: {data}")
    else:
        print("⚠️ Skipped empty message")
