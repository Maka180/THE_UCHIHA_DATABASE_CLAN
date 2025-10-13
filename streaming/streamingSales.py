
import time
import json
from datetime import datetime
from pymongo import MongoClient
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from bson import json_util

# ======================================
# MongoDB Connection Setup
# ======================================
MONGO_URI = "mongodb+srv://lacyfarasi09_db_user:DBMaka08@cluster0.tff7boz.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
MONGO_DATABASE = "ClearVueBI"
SALES_HEADER_COLLECTION = "sales_header"
SALES_LINE_COLLECTION = "sales_line"

# ======================================
# Kafka Connection Setup
# ======================================
KAFKA_BROKER = "localhost:9092"
SALES_TOPIC = "clearvue_sales_orders"

# ======================================
# Stream Sales Data from MongoDB
# ======================================
def stream_sales_data():
    producer = None
    mongo_client = None

    try:
        # MongoDB Setup
        mongo_client = MongoClient(MONGO_URI, tls=True, tlsAllowInvalidCertificates=True)
        mongo_db = mongo_client[MONGO_DATABASE]
        header_col = mongo_db[SALES_HEADER_COLLECTION]
        line_col = mongo_db[SALES_LINE_COLLECTION]
        print(f"Connected to MongoDB database: '{MONGO_DATABASE}'")

        # Kafka Setup
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v, default=json_util.default).encode('utf-8'),
            api_version=(0, 10, 1)
        )
        print(f" Connected to Kafka broker at {KAFKA_BROKER}")
        print(f"Streaming sales data to topic: {SALES_TOPIC}\n")

        # Stream each sales header with its matching lines
        for header_doc in header_col.find():
            trans_no = header_doc.get("transNo")
            if not trans_no:
                continue

            # Find matching sales lines
            lines = list(line_col.find({"transNo": trans_no}))
            header_doc["sales_lines"] = lines
            header_doc["_id"] = str(header_doc["_id"])  # Convert ObjectId

            for line in lines:
                line["_id"] = str(line["_id"])  # Convert ObjectId

            # Send to Kafka
            producer.send(SALES_TOPIC, value=header_doc)
            producer.flush()

            print(f"[{datetime.now().strftime('%H:%M:%S')}] Streamed SALE: {trans_no} with {len(lines)} lines")

            time.sleep(1)  # Optional pacing

    except NoBrokersAvailable:
        print(f"\nNo Kafka brokers available at {KAFKA_BROKER}.")
    except Exception as e:
        print(f"\n Unexpected error: {e}")
    except KeyboardInterrupt:
        print("\nStream manually stopped.")
    finally:
        if producer:
            producer.close()
            print("Kafka producer connection closed.")
        if mongo_client:
            mongo_client.close()
            print("MongoDB connection closed.")

# ======================================
# Run
# ======================================
if __name__ == "__main__":
    stream_sales_data()