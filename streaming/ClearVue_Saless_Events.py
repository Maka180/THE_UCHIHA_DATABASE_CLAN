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
SALES_COLLECTION = "sales_header" 
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "clearvue_sales_events"

# ======================================
# Change Stream Logic
# ======================================
def stream_sales_events():
    producer = None
    mongo_client = None

    try:
        mongo_client = MongoClient(MONGO_URI, tls=True)
        collection = mongo_client['ClearVueBI'][SALES_COLLECTION]
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v, default=json_util.default).encode('utf-8'),
            api_version=(0, 10, 1)
        )

        print(f"✅ Started Sales Stream. Topic: {KAFKA_TOPIC}")

        # Listen for Inserts/Updates and retrieve the full document
        with collection.watch(full_document='updateLookup') as stream:
            for change in stream:
                operation = change['operationType']
                
                # We only process new sales (insert) or modifications (update/replace)
                if operation in ['insert', 'update', 'replace']:
                    document_data = change.get('fullDocument', {})
                    doc_id = document_data.get('DOC_NUMBER', 'N/A')

                    # Extract Key Fields for BI Requirements
                    payload = {
                        "operation": operation,
                        "doc_id": doc_id,
                        "timestamp": datetime.now().isoformat(),
                        "data": {
                            "DOC_NUMBER": document_data.get("DOC_NUMBER"),
                            "CUSTOMER_NUMBER": document_data.get("customerNumber"), # Based on Deliverable 1
                            "REP_CODE": document_data.get("REP_CODE"),
                            "TRANS_DATE": document_data.get("TRANS_DATE"),
                            "TOTAL_REVENUE": document_data.get("totalLinePrice"), # Assuming this is the total amount field
                            # Include FIN_PERIOD explicitly for Weekly/Monthly Analysis KPI
                            "FIN_PERIOD": document_data.get("FIN_PERIOD") 
                        }
                    }

                    producer.send(KAFKA_TOPIC, value=payload)
                    producer.flush()
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] ➡️ SALES {operation.upper():<8} | DOC: {doc_id}")

    except NoBrokersAvailable:
        print(f"\n❌ FATAL ERROR: No Kafka brokers available at {KAFKA_BROKER}. Ensure Kafka is running.")
    except Exception as e:
        print(f"\n❌ UNEXPECTED ERROR: {e}")
    finally:
        if producer: producer.close()
        if mongo_client: mongo_client.close()

if __name__ == "__main__":
    stream_sales_events()