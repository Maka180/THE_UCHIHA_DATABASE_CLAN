import time
import json
from datetime import datetime
from pymongo import MongoClient
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from bson import json_util

# ======================================
# Configuration
# ======================================
MONGO_URI = "mongodb+srv://lacyfarasi09_db_user:DBMaka08@cluster0.tff7boz.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
PRODUCT_COLLECTION = "Inventory" 
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "clearvue_product_updates"

# ======================================
# Change Stream Logic
# ======================================
def stream_product_updates():
    producer = None
    mongo_client = None

    try:
        mongo_client = MongoClient(MONGO_URI, tls=True)
        collection = mongo_client['ClearVueBI'][PRODUCT_COLLECTION]
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v, default=json_util.default).encode('utf-8'),
            api_version=(0, 10, 1)
        )

        print(f"✅ Started Product Stream. Topic: {KAFKA_TOPIC}")

        # Listen for Inserts/Updates and retrieve the full document
        with collection.watch(full_document='updateLookup') as stream:
            for change in stream:
                operation = change['operationType']
                
                if operation in ['insert', 'update', 'replace']:
                    document_data = change.get('fullDocument', {})
                    product_id = document_data.get('inventoryCode', 'N/A')

                    # Extract ALL Dimension Fields needed for BI filtering/grouping
                    payload = {
                        "operation": operation,
                        "product_id": product_id,
                        "timestamp": datetime.now().isoformat(),
                        "data": {
                            "INVENTORY_CODE": document_data.get("inventoryCode"),
                            "DESCRIPTION": document_data.get("description"),
                            "BRAND": document_data.get("BRAND"), # Key for KPI
                            "CATEGORY": document_data.get("CATEGORY"), # Key for KPI
                            "STYLE": document_data.get("STYLE"), # Key for KPI
                            "UNIT_PRICE": document_data.get("unitSellPrice"),
                            "LAST_COST": document_data.get("lastCost")
                        }
                    }

                    producer.send(KAFKA_TOPIC, value=payload)
                    producer.flush()
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] 🔄 PRODUCT {operation.upper():<8} | CODE: {product_id}")

    except NoBrokersAvailable:
        print(f"\n❌ FATAL ERROR: No Kafka brokers available at {KAFKA_BROKER}.")
    except Exception as e:
        print(f"\n❌ UNEXPECTED ERROR: {e}")
    finally:
        if producer: producer.close()
        if mongo_client: mongo_client.close()

if __name__ == "__main__":
    stream_product_updates()