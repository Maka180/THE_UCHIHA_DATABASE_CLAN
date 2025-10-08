import json
import os
from pymongo import MongoClient
from datetime import datetime

# ======================================
# 🔧 MongoDB Atlas Connection Setup
# ======================================
MONGO_URI = "mongodb+srv://lacyfarasi09_db_user:DBMaka08@cluster0.tff7boz.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DATABASE_NAME = "ClearVueBI"

client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]

# Path to your data folder
DATA_FOLDER = r"C:\Users\USER\Desktop\CMPG321ROJECT\THE_UCHIHA_DATABASE_CLAN\Cleaned data"

# ======================================
# 🧩 Helper Function to Import JSON
# ======================================
def import_json_to_mongo(collection_name, file_path):
    print(f"⏳ Importing {file_path} → {collection_name} ...")
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # If not a list, wrap it
        if not isinstance(data, list):
            data = [data]

        if len(data) == 0:
            print(f"⚠️ Skipped {collection_name}: Empty file.")
            return

        # Insert data
        result = db[collection_name].insert_many(data)
        print(f"✅ Inserted {len(result.inserted_ids)} documents into {collection_name}")
    except Exception as e:
        print(f"❌ Error importing {collection_name}: {e}")

# ======================================
# 🚀 Main Logic
# ======================================
def main():
    print("=== MongoDB Data Import Script ===")
    print(f"Connected to database: {DATABASE_NAME}\n")

    for filename in os.listdir(DATA_FOLDER):
        if filename.endswith(".json"):
            collection_name = filename.replace(".json", "")
            file_path = os.path.join(DATA_FOLDER, filename)
            import_json_to_mongo(collection_name, file_path)

    print("\n🎉 All data imported successfully!")

if __name__ == "__main__":
    main()
