from pymongo import MongoClient, UpdateOne, IndexModel
# ---- MongoDB Config ----
MONGO_URI = ""
DB_NAME = "JobStats"
COLLECTION_NAME = "interview_processes_backfilled"

# ---- Stage mapping ----
STAGE_MAP = {
    "R2": "Onsite",
    "Tech": "Phone/R1",
    "Behavioral": "HM",
    "VO": "Onsite"
}

def update_stages():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    coll = db[COLLECTION_NAME]

    total_updates = 0
    for old_stage, new_stage in STAGE_MAP.items():
        result = coll.update_many(
            {"stage": old_stage},
            {"$set": {"stage": new_stage}}
        )
        print(f"Updated {result.modified_count} documents: {old_stage} → {new_stage}")
        total_updates += result.modified_count

    print(f"\n✅ Total stages updated: {total_updates}")

if __name__ == "__main__":
    update_stages()
