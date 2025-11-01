"""
Migration script to populate processed_ids collection with existing message IDs.

This script:
1. Reads all message IDs from the interview_processes collection
2. Creates a new processed_ids collection (if it doesn't exist)
3. Populates it with all existing message IDs
4. Creates an index on msg_id for fast lookups
"""

from pymongo import MongoClient, ASCENDING
from pymongo.server_api import ServerApi
import time

# MongoDB connection
client = MongoClient(uri, server_api=ServerApi('1'))

# Test connection
try:
    client.admin.command('ping')
    print("✅ Connected to MongoDB!")
except Exception as e:
    print(f"❌ Connection failed: {e}")
    exit(1)

db = client["JobStats"]
interview_collection = db["interview_processes"]
processed_collection = db["processed_ids"]

print("\n📊 Starting migration...")
print("=" * 60)

# Step 1: Get count of existing documents
existing_count = interview_collection.count_documents({})
print(f"Found {existing_count} documents in interview_processes collection")

# Step 2: Check if processed_ids collection already has data
processed_count = processed_collection.count_documents({})
if processed_count > 0:
    print(f"\n⚠️  Warning: processed_ids collection already has {processed_count} documents")
    response = input("Do you want to clear it and start fresh? (yes/no): ")
    if response.lower() == 'yes':
        processed_collection.delete_many({})
        print("✅ Cleared processed_ids collection")
    else:
        print("ℹ️  Will skip existing message IDs")

# Step 3: Extract unique message IDs from interview_processes
print("\n📝 Extracting message IDs from interview_processes...")
pipeline = [
    {
        "$group": {
            "_id": "$msg_id",
            "first_seen": {"$min": "$timestamp"}
        }
    }
]

unique_msgs = list(interview_collection.aggregate(pipeline))
print(f"Found {len(unique_msgs)} unique message IDs")

# Step 4: Prepare documents for insertion
docs_to_insert = []
for msg in unique_msgs:
    if msg["_id"]:  # Skip any null IDs
        doc = {
            "msg_id": msg["_id"],
            "processed_at": msg["first_seen"],
            "source": "migration"
        }
        docs_to_insert.append(doc)

print(f"Prepared {len(docs_to_insert)} documents for insertion")

# Step 5: Batch insert into processed_ids
if docs_to_insert:
    print("\n💾 Inserting into processed_ids collection...")
    BATCH_SIZE = 1000
    inserted_count = 0

    for i in range(0, len(docs_to_insert), BATCH_SIZE):
        batch = docs_to_insert[i:i + BATCH_SIZE]
        try:
            # Use ordered=False to continue on duplicate key errors
            result = processed_collection.insert_many(batch, ordered=False)
            inserted_count += len(result.inserted_ids)
            print(f"  Inserted batch {i//BATCH_SIZE + 1}: {len(result.inserted_ids)} docs")
        except Exception as e:
            # Handle duplicate key errors gracefully
            if "duplicate key" in str(e).lower():
                print(f"  ⚠️  Some duplicates skipped in batch {i//BATCH_SIZE + 1}")
            else:
                print(f"  ❌ Error in batch {i//BATCH_SIZE + 1}: {e}")
        time.sleep(0.1)  # Small delay to avoid overwhelming DB

    print(f"\n✅ Successfully inserted {inserted_count} message IDs")

# Step 6: Create index for fast lookups
print("\n🔍 Creating index on msg_id...")
try:
    processed_collection.create_index([("msg_id", ASCENDING)], unique=True)
    print("✅ Index created successfully")
except Exception as e:
    print(f"⚠️  Index creation: {e}")

# Step 7: Verify results
final_count = processed_collection.count_documents({})
print("\n" + "=" * 60)
print("📊 Migration Summary:")
print(f"  - Documents in interview_processes: {existing_count}")
print(f"  - Unique message IDs found: {len(unique_msgs)}")
print(f"  - Documents in processed_ids: {final_count}")
print("=" * 60)

if final_count == len(unique_msgs):
    print("\n✅ Migration completed successfully!")
else:
    print(f"\n⚠️  Warning: Count mismatch. Expected {len(unique_msgs)}, got {final_count}")

# Step 8: Show sample documents
print("\n📋 Sample documents from processed_ids:")
for doc in processed_collection.find().limit(3):
    print(f"  - msg_id: {doc['msg_id']}, processed_at: {doc.get('processed_at', 'N/A')}")

print("\n✅ Done!")