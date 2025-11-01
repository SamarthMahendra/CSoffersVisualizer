import os




from pymongo import MongoClient
import os


mongo_client = MongoClient(uri)
db = mongo_client["JobStats"]
collection = db["interview_processes_backfilled"]

# === Step 1: Backup Interview records (optional but recommended) ===
backup_docs = list(collection.find({"stage": "Interview"}))
if backup_docs:
    db["backup_interview_2"].insert_many(backup_docs)
    print(f"[BACKUP] {len(backup_docs)} 'Interview' records backed up to 'backup_interview' collection.")
else:
    print("[INFO] No 'Interview' records found to back up.")

# === Step 2: Count current Interview records ===
interview_count = collection.count_documents({"stage": "Interview"})
print(f"[INFO] Found {interview_count} 'Interview' records to merge into 'Phone/R1'.")

# === Step 3: Perform merge ===
result = collection.update_many(
    {"stage": "Interview"},
    {"$set": {"stage": "Phone/R1"}}
)

# === Step 4: Verify ===
print(f"[SUCCESS] Updated {result.modified_count} records from 'Interview' → 'Phone/R1'.")

# === Step 5: Sanity check counts ===
new_phone_r1_count = collection.count_documents({"stage": "Phone/R1"})
print(f"[INFO] Total 'Phone/R1' records after merge: {new_phone_r1_count}")

# === Optional Step 6: Log summary ===
with open("merge_log.txt", "w") as log:
    log.write(f"Merged {result.modified_count} 'Interview' → 'Phone/R1'\n")
    log.write(f"Total 'Phone/R1' after merge: {new_phone_r1_count}\n")

print("[DONE] Merge operation complete.")
