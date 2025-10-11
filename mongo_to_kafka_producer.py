from kafka import KafkaProducer
from pymongo import MongoClient
import json
import time

# === MongoDB Connection ===
MONGO_URI = "mongodb+srv://lacyfarasi09_db_user:DBMaka08@cluster0.tff7boz.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(MONGO_URI)
db = client["ClearVueBI"]

# === LIST OF ALL CLEANED COLLECTIONS TO STREAM ===
# Add ALL the names of your cleaned data collections here.
cleaned_collections_to_stream = [
    "Cleaned_Age",
    "cleaned_Customer",
    "cleaned_CustomerAccountParameters",
    "cleaned_CustomerCategories",
    "cleaned_CustomerRegions",
    "Cleaned_Suppliers",
    "Cleaned_Trans Types",
    "Payment_header",
    "Payment_lines",
    "Product_Categories_Header",
    "Product_Ranges_Header",
    "product_styles",
    "products_brands",
    "products",
    "Purchases_Headers_header",
    "Purchases_Lines_Header",
    "representatives',"
    "sales_header",
    "sales_line"

     # Example of another cleaned collection
    # Add any other cleaned collections you have
]
# =================================================

# === Kafka Producer Setup ===
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("✅ Connected to MongoDB and Kafka")

# === Stream documents from ALL collections to Kafka ===
for collection_name in cleaned_collections_to_stream:
    source_collection = db[collection_name]
    print(f"\n=======================================================")
    print(f"📤 Starting stream from collection: '{collection_name}'...")
    print(f"=======================================================")

    # Stream documents from the current collection
    for doc in source_collection.find():
        # Convert ObjectId to string (JSON can’t handle ObjectId)
        doc["_id"] = str(doc["_id"])
        
        # Optionally, add a field to the document to indicate its original source
        # This is useful for the consumer on the other side.
        doc["source_collection"] = collection_name 
        
        producer.send('my-first-topic', doc)
        print(f"🚀 Sent from {collection_name} to Kafka: {doc['_id']}")
        time.sleep(0.1)  # A faster sleep interval might be better for large datasets

producer.flush()
print("\n✅ Finished streaming all specified cleaned data collections to Kafka.")