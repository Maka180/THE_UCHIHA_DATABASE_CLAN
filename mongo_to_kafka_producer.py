from kafka import KafkaProducer
from pymongo import MongoClient
import json
import time

# === MongoDB Connection ===
MONGO_URI = "mongodb+srv://lacyfarasi09_db_user:DBMaka08@cluster0.tff7boz.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(MONGO_URI)
db = client["ClearVueBI"]

# Choose the MongoDB collection to stream from
source_collection = db["cleaned_Customer"]  # change this to any collection name you imported earlier

# === Kafka Producer Setup ===
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("✅ Connected to MongoDB and Kafka")
print(f"📤 Streaming documents from '{source_collection.name}' into Kafka...")

# === Stream documents from Mongo to Kafka ===
for doc in source_collection.find():  # you can remove .limit(10) to stream all
    # Convert ObjectId to string (JSON can’t handle ObjectId)
    doc["_id"] = str(doc["_id"])
    producer.send('my-first-topic', doc)
    print(f"🚀 Sent to Kafka: {doc}")
    time.sleep(1)  # slow down the stream for demo purposes

producer.flush()
print("✅ Finished streaming data to Kafka.")
