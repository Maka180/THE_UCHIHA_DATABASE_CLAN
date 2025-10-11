import json
import os
from pymongo import MongoClient
# FIXED: Replaced ConnectionError with the more specific and correct PyMongo error for connection issues
from pymongo.errors import ServerSelectionTimeoutError, OperationFailure, BulkWriteError
from datetime import datetime

# ======================================
# 🔧 MongoDB Atlas Connection Setup
# ======================================
MONGO_URI = "mongodb+srv://lacyfarasi09_db_user:DBMaka08@cluster0.tff7boz.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DATABASE_NAME = "ClearVueBI"

# Path to your data folder (Ensure this path is correct before running)
# Note: For execution on different machines, this path may need updating.
DATA_FOLDER = r"C:\Users\USER\Desktop\CMPG321ROJECT\THE_UCHIHA_DATABASE_CLAN\Cleaned data"

# ======================================
# 🧩 Helper Function to Import JSON
# ======================================
def import_json_to_mongo(client, collection_name, file_path):
    """Loads and inserts data from a single JSON file into MongoDB."""
    db = client[DATABASE_NAME]
    collection = db[collection_name]

    print(f"\n--- Processing {collection_name} ---")
    print(f"⏳ Importing {file_path}...")

    try:
        # Clear existing data for a clean import (Crucial for iterative prototyping)
        collection.delete_many({})
        print(f"🧹 Existing documents cleared from {collection_name}.")

        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, list):
            data = [data]

        if len(data) == 0:
            print(f"⚠️ Skipped {collection_name}: Empty file.")
            return

        # Insert data
        result = collection.insert_many(data, ordered=False) # ordered=False allows partial insertion on errors
        print(f"✅ Inserted {len(result.inserted_ids)} documents.")

    except BulkWriteError as bwe:
        print(f"❌ Warning: Partial import for {collection_name}. Write errors occurred: {bwe.details}")
    except Exception as e:
        print(f"❌ Error importing {collection_name}: {e}")

# ======================================
# 🔑 Indexing Logic (Step 1.4: Critical for BI Queries)
# ======================================
def create_indexes(client):
    """Creates essential indexes on key fields for BI query performance."""
    db = client[DATABASE_NAME]
    
    print("\n=== Creating Performance Indexes ===")

    # Define indexes for core collections based on analytical requirements:
    
    # 1. Sales Header (Primary time dimension and linking table)
    # Note: Assuming your cleaned data uses snake_case column names based on your previous script output.
    db["SalesHeader"].create_index([("trans_date", 1)], name="idx_trans_date")
    db["SalesHeader"].create_index([("customer_number", 1)], name="idx_customer_number")
    db["SalesHeader"].create_index([("rep_code", 1), ("trans_date", 1)], name="idx_rep_time") # For rep performance over time
    print("⭐ Indexes created for SalesHeader.")
    
    # 2. Sales Line (Detailed sales metrics)
    db["SalesLine"].create_index([("doc_number", 1)], name="idx_doc_number") # For linking to SalesHeader
    db["SalesLine"].create_index([("inventory_code", 1)], name="idx_inventory_code") # For product analysis
    print("⭐ Indexes created for SalesLine.")

    # 3. Products (Product dimension lookups)
    db["Products"].create_index([("inventory_code", 1)], unique=True, name="idx_inventory_code_unique")
    print("⭐ Indexes created for Products.")

    # 4. Customer (Customer dimension lookups)
    db["Customer"].create_index([("customer_number", 1)], unique=True, name="idx_customer_number_unique")
    print("⭐ Indexes created for Customer.")

    # 5. Age Analysis (Financial Period and Customer Aging)
    db["AgeAnalysis"].create_index([("fin_period", 1), ("customer_number", 1)], name="idx_fin_cust")
    print("⭐ Indexes created for AgeAnalysis.")
    
    # 6. Payments (Used for real-time streaming validation)
    db["PaymentHeader"].create_index([("payment_date", 1)], name="idx_payment_date")
    print("⭐ Indexes created for PaymentHeader.")

    print("✅ All essential indexes created.")

# ======================================
# 🚀 Main Logic
# ======================================
def main():
    client = None
    try:
        print("=== MongoDB Data Import and Indexing Script ===")
        
        # Connect to MongoDB
        client = MongoClient(MONGO_URI)
        client.admin.command('ping') # Check connection health
        print(f"Connected successfully to cluster, targeting database: {DATABASE_NAME}\n")
        
        # Iterate through data folder and import
        for filename in os.listdir(DATA_FOLDER):
            if filename.endswith(".json"):
                # Use the filename without extension as the collection name
                collection_name = filename.replace(".json", "")
                file_path = os.path.join(DATA_FOLDER, filename)
                import_json_to_mongo(client, collection_name, file_path)
        
        # Create indexes after all data is loaded
        create_indexes(client)

        print("\n🎉 Phase 1 Complete: All data imported and indexed successfully!")

    # CATCH: Use the imported ServerSelectionTimeoutError for connection issues
    except ServerSelectionTimeoutError:
        print("❌ FATAL ERROR: Failed to connect to MongoDB. Check MONGO_URI and network/firewall settings.")
    except Exception as e:
        print(f"❌ AN UNEXPECTED ERROR OCCURRED: {e}")
    finally:
        if client:
            client.close()
            print("\nMongoDB connection closed.")

if __name__ == "__main__":
    main()
