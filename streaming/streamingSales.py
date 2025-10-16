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
# Stream Existing Sales Data (One-Time)
# ======================================
def stream_existing_sales_data(producer, header_col, line_col):
    print(f"Streaming existing sales data to topic: {SALES_TOPIC}\n")
    for header_doc in header_col.find():
        trans_no = header_doc.get("transNo")
        if not trans_no:
            continue

        lines = list(line_col.find({"transNo": trans_no}))
        header_doc["sales_lines"] = lines
        header_doc["_id"] = str(header_doc["_id"])
        for line in lines:
            line["_id"] = str(line["_id"])

        producer.send(SALES_TOPIC, value=header_doc)
        producer.flush()

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Streamed SALE: {trans_no} with {len(lines)} lines")
        time.sleep(1)

# ======================================
# Stream New Inserts in Real-Time
# ======================================
def stream_new_sales_data(producer, header_col, line_col):
    print(f"Listening for new inserts on collection: {SALES_HEADER_COLLECTION}\n")
    with header_col.watch([{'$match': {'operationType': 'insert'}}]) as stream:
        for change in stream:
            full_doc = change['fullDocument']
            trans_no = full_doc.get("transNo")
            if not trans_no:
                continue

            lines = list(line_col.find({"transNo": trans_no}))
            full_doc["sales_lines"] = lines
            full_doc["_id"] = str(full_doc["_id"])
            for line in lines:
                line["_id"] = str(line["_id"])

            producer.send(SALES_TOPIC, value=full_doc)
            producer.flush()

            print(f"[{datetime.now().strftime('%H:%M:%S')}] Streamed NEW SALE: {trans_no} with {len(lines)} lines")

# ======================================
# Unified Streaming Runner
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
        print(f"Connected to Kafka broker at {KAFKA_BROKER}")

        # Stream existing data once
        stream_existing_sales_data(producer, header_col, line_col)

        # Then stream new inserts in real-time
        stream_new_sales_data(producer, header_col, line_col)

    except NoBrokersAvailable:
        print(f"\nNo Kafka brokers available at {KAFKA_BROKER}.")
    except Exception as e:
        print(f"\nUnexpected error: {e}")
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