import time
import random
import json
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ======================================
# 🔧 Kafka Connection Setup (Phase 2)
# ======================================
# NOTE: You MUST have a running Kafka Broker at this address.
KAFKA_BROKER = "localhost:9092" 
KAFKA_TOPIC = "clearvue_payments"

# ======================================
# Sample data for simulation
# ======================================
PAYMENT_METHODS = ["EFT", "Credit Card", "Cash"]
# Assuming these customer numbers exist in your 'Customer' collection for realism
SAMPLE_CUSTOMER_NUMBERS = [f"CUST_{i:04}" for i in range(1000, 1010)] 

# ======================================
# ⚙️ Stream Generation Logic
# ======================================

def generate_payment_document(sequence_id):
    """Creates a new, realistic payment document."""
    
    payment_time = datetime.now()
    
    customer_num = random.choice(SAMPLE_CUSTOMER_NUMBERS)
    method = random.choice(PAYMENT_METHODS)
    
    # Generate a realistic amount (between 100 and 15,000)
    amount = round(random.uniform(100.00, 15000.00), 2)

    document = {
        # Use a unique ID incorporating the current time and sequence number
        "payment_id": f"P-{payment_time.strftime('%Y%m%d%H%M%S')}-{sequence_id}",
        "customer_number": customer_num,
        # Convert datetime object to ISO 8601 string for JSON serialization
        "payment_date": payment_time.isoformat(), 
        "total_paid": amount,
        "payment_method": method,
        "processing_time_ms": random.randint(10, 500),
        "status": "Processed"
    }
    return document

def stream_payments_to_kafka():
    """Connects to Kafka and streams simulated payment data."""
    producer = None
    try:
        # Initialize Kafka Producer
        # value_serializer converts the Python dictionary into a JSON byte string
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=(0, 10, 1) # Specify a supported Kafka API version
        )

        print(f"Connected to Kafka broker at {KAFKA_BROKER}. Starting payment stream producer...")
        print(f"Sending data to topic: {KAFKA_TOPIC}\n")

        document_counter = 1
        
        while True:
            # 1. Generate the new document
            new_payment = generate_payment_document(document_counter)
            
            # 2. Send the message to the Kafka topic
            # send() is asynchronous; it returns a future object
            future = producer.send(KAFKA_TOPIC, value=new_payment)
            producer.flush() # Ensure message is sent promptly

            print(f"[{datetime.now().strftime('%H:%M:%S')}] Produced #{document_counter}: ID {new_payment['payment_id']} | Cust {new_payment['customer_number']} | R{new_payment['total_paid']:.2f}")
            
            # Check for success/failure (optional, but good practice)
            # record_metadata = future.get(timeout=10)
            
            document_counter += 1
            
            # Pause for a short period to simulate real-time stream flow (1 to 3 seconds)
            time.sleep(random.uniform(1, 3))

    except NoBrokersAvailable:
        print(f"\n❌ FATAL ERROR: No Kafka brokers available at {KAFKA_BROKER}. Please ensure Kafka is running.")
    except KeyboardInterrupt:
        print("\n🛑 Kafka Producer manually stopped.")
    except Exception as e:
        print(f"\n❌ AN UNEXPECTED ERROR OCCURRED: {e}")
    finally:
        if producer:
            producer.close()
            print("Kafka producer connection closed.")

# ======================================
# 🚀 Run
# ======================================
if __name__ == "__main__":
    stream_payments_to_kafka()
