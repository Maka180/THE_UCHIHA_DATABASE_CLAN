import json
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from datetime import datetime

# ======================================
# 🔧 Kafka Connection Setup
# ======================================
KAFKA_BROKER = "localhost:9092" 
KAFKA_TOPIC = "clearvue_payments"
# Use a unique group ID to ensure this consumer receives all new messages 
GROUP_ID = "clearvue_kafka_tester_group" 

def consume_payments_from_kafka():
    """Connects to Kafka and continuously consumes messages from the clearvue_payments topic."""
    consumer = None
    try:
        # Initialize Kafka Consumer
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            group_id=GROUP_ID,
            # Deserializer converts the JSON byte string back into a Python dictionary
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            # Start reading at the latest offset (only new messages produced after this consumer starts)
            auto_offset_reset='latest', 
            api_version=(0, 10, 1)
        )

        print(f"✅ Connected to Kafka broker at {KAFKA_BROKER}.")
        print(f"Listening for messages on topic: {KAFKA_TOPIC}\n")
        print("Waiting for messages... (Run your producer script in another terminal)")
        print("=" * 60)

        # Loop to continuously read messages
        for message in consumer:
            payment_data = message.value
            
            # Extract key information for a clean printout
            p_id = payment_data.get("payment_id", "N/A")
            cust_num = payment_data.get("customer_number", "N/A")
            amount = payment_data.get("total_paid", 0.0)
            method = payment_data.get("payment_method", "N/A")

            print(f"[{datetime.now().strftime('%H:%M:%S')}] Message received on Partition {message.partition}, Offset {message.offset}:")
            print(f"  > ID: {p_id} | Cust: {cust_num} | Amount: R{amount:.2f} | Method: {method}")
            print("-" * 60)
            
            # The consumer automatically commits the offset periodically

    except NoBrokersAvailable:
        print(f"\n❌ FATAL ERROR: No Kafka brokers available at {KAFKA_BROKER}. Please ensure Kafka is running.")
    except KeyboardInterrupt:
        print("\n🛑 Kafka Consumer manually stopped.")
    except Exception as e:
        print(f"\n❌ AN UNEXPECTED ERROR OCCURRED: {e}")
    finally:
        if consumer:
            consumer.close()
            print("Kafka consumer connection closed.")

# ======================================
# 🚀 Run
# ======================================
if __name__ == "__main__":
    consume_payments_from_kafka()