from pymongo import MongoClient
from pprint import pprint

# ---- Connect to MongoDB ----
client = MongoClient(uri)
db = client["JobStats"]
collection = db["interview_processes_backfilled"]

# ---- Step 1: Find all messages containing "prayer" (case-insensitive) ----
cursor = collection.find({"text": {"$regex": "prayer", "$options": "i"}})

count = 0
for doc in cursor:
    print("\n----------------------------------------")
    pprint(doc)
    print("----------------------------------------")

    ans = input("Mark this message as spam=True? (y/n/q): ").strip().lower()
    if ans == "y":
        result = collection.update_one({"_id": doc["_id"]}, {"$set": {"spam": True}})
        if result.modified_count > 0:
            print("✅ Marked as spam")
        else:
            print("⚠️ No change (maybe already spam=True)")
        count += 1
    elif ans == "q":
        print("\n⏹️ Quitting review.")
        break
    else:
        print("⏭️ Skipped.")

print(f"\nFinished. Total marked as spam=True: {count}")

