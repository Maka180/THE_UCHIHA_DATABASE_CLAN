import time
import random
import json
from datetime import datetime
# --- NEW IMPORTS FOR MONGODB ---
from pymongo import MongoClient
# -------------------------------
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ======================================
# 🔧 MongoDB Connection Setup
# ======================================
# *** IMPORTANT: REPLACE WITH YOUR ACTUAL MONGO_URI ***
MONGO_URI = "mongodb+srv://lacyfarasi09_db_user:DBMaka08@cluster0.tff7boz.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
MONGO_DATABASE = "ClearVueBI"
MONGO_COLLECTION = "simulated_payments_stream" # Target collection for the generated data

mongo_client = None
mongo_db = None

# ======================================
# 🔧 Kafka Connection Setup
# ======================================
KAFKA_BROKER = "localhost:9092" 
KAFKA_TOPIC = "clearvue_payments"

# ======================================
# Sample data for simulation
# ======================================
PAYMENT_METHODS = ["EFT", "Credit Card", "Cash"]
SAMPLE_CUSTOMER_NUMBERS = [f"CUST_{i:04}" for i in range(1000, 1010)] 

# ======================================
# ⚙️ Stream Generation Logic (Unchanged)
# ======================================

def generate_payment_document(sequence_id):
    """Creates a new, realistic payment document."""
    
    payment_time = datetime.now()
    
    customer_num = random.choice(SAMPLE_CUSTOMER_NUMBERS)
    method = random.choice(PAYMENT_METHODS)
    
    amount = round(random.uniform(100.00, 15000.00), 2)

    document = {
        "payment_id": f"P-{payment_time.strftime('%Y%m%d%H%M%S')}-{sequence_id}",
        "customer_number": customer_num,
        "payment_date": payment_time.isoformat(), 
        "total_paid": amount,
        "payment_method": method,
        "processing_time_ms": random.randint(10, 500),
        "status": "Processed"
    }
    return document

def stream_payments_to_kafka_and_mongo():
    """Connects to Kafka and MongoDB, then streams simulated payment data to both."""
    global mongo_client, mongo_db
    producer = None
    
    try:
        # --- 1. Setup Connections ---
        
        # MongoDB Setup
        mongo_client = MongoClient(MONGO_URI)
        mongo_db = mongo_client[MONGO_DATABASE]
        mongo_collection = mongo_db[MONGO_COLLECTION]
        print(f"✅ Connected to MongoDB database: '{MONGO_DATABASE}'")

        # Kafka Setup
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=(0, 10, 1) 
        )

        print(f"✅ Connected to Kafka broker at {KAFKA_BROKER}.")
        print(f"Sending data to Kafka topic: {KAFKA_TOPIC} AND MongoDB collection: {MONGO_COLLECTION}\n")

        # --- 2. Start Streaming Loop ---
        document_counter = 1
        
        while True:
            # 1. Generate the new document
            new_payment = generate_payment_document(document_counter)
            
            # 2. Save the document to MongoDB (Simulate application saving data)
            # .copy() ensures the original dictionary is inserted, preventing unexpected changes
            mongo_collection.insert_one(new_payment.copy()) 
            
            # 3. Send the message to the Kafka topic
            producer.send(KAFKA_TOPIC, value=new_payment)
            producer.flush() # Ensure message is sent promptly

            print(f"[{datetime.now().strftime('%H:%M:%S')}] Produced #{document_counter}: R{new_payment['total_paid']:.2f} | Sent to Kafka & Mongo")
            
            document_counter += 1
            
            # Pause for a short period to simulate real-time stream flow
            time.sleep(random.uniform(1, 3))

    except NoBrokersAvailable:
        print(f"\n❌ FATAL ERROR: No Kafka brokers available at {KAFKA_BROKER}. Please ensure Kafka is running.")
    except Exception as e:
        print(f"\n❌ AN UNEXPECTED ERROR OCCURRED: {e}")
    except KeyboardInterrupt:
        print("\n🛑 Producer manually stopped.")
        
    finally:
        # --- 3. Close Connections ---
        if producer:
            producer.close()
            print("Kafka producer connection closed.")
        if mongo_client:
            mongo_client.close()
            print("MongoDB connection closed.")


# ======================================
# 🚀 Run
# ======================================
if __name__ == "__main__":
    stream_payments_to_kafka_and_mongo()