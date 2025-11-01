from pymongo import MongoClient, UpdateOne, IndexModel
from datetime import datetime, timedelta, timezone

# ---- MongoDB Config ----
DB_NAME = "JobStats"
SRC_COLLECTION = "interview_processes"
DST_COLLECTION = "interview_processes_backfilled"

# ---- Stage Order ----
STAGE_ORDER = ["OA", "Phone/R1", "Onsite", "HM", "Offer", "Reject"]
BASE_NEVER_AUTOGEN = {"App", "Offer"}


def to_dt(x):
    if not x:
        return None
    if isinstance(x, datetime):
        return x if x.tzinfo else x.replace(tzinfo=timezone.utc)
    s = str(x)
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def deterministic_auto_id(company, author, stage, anchor_dt):
    epoch = int(anchor_dt.timestamp()) if anchor_dt else 0
    return f"auto_{company}_{author}_{stage}_{epoch}"


def ensure_indexes(coll):
    print("[Backfill] Creating indexes...")
    coll.create_indexes([
        IndexModel([("spam", 1), ("stage", 1), ("company", 1), ("new_grad", 1), ("timestamp", -1)]),
        IndexModel([("msg_id", 1)], unique=True)
    ])


def build_backfilled():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    src = db[SRC_COLLECTION]
    dst = db[DST_COLLECTION]

    ensure_indexes(dst)

    print("[Backfill] Loading real messages from source collection...")
    base_query = {"spam": False, "msg_id": {"$not": {"$regex": "^auto_"}}}
    projection = {
        "_id": 0, "msg_id": 1, "text": 1, "timestamp": 1,
        "author": 1, "company": 1, "stage": 1, "new_grad": 1, "category": 1
    }

    cursor = src.find(base_query, projection)
    journeys = {}

    for doc in cursor:
        company = (doc.get("company") or "").strip()
        author = (doc.get("author") or "").strip()
        new_grad = bool(doc.get("new_grad", False))
        ts = to_dt(doc.get("timestamp"))
        doc["timestamp"] = ts
        key = (company, author, new_grad)
        journeys.setdefault(key, []).append(doc)

    print(f"[Backfill] Found {len(journeys)} unique journeys.")

    ops = []
    synthetic_total = 0

    for (company, author, new_grad), docs in journeys.items():
        present = {str(x.get("stage", "")).strip() for x in docs if x.get("stage")}
        real_ts = [to_dt(x.get("timestamp")) for x in docs if x.get("timestamp")]
        real_ts = [x for x in real_ts if x]
        latest_real = max(real_ts) if real_ts else None

        has_reject = "Reject" in present
        has_offer = "Offer" in present

        valid = [s for s in present if s in STAGE_ORDER]
        if not valid:
            continue

        stage_pos = {s: i for i, s in enumerate(STAGE_ORDER)}
        if not has_reject:
            latest_idx = max(stage_pos[s] for s in valid)
        else:
            if present == {"Reject"}:
                latest_idx = stage_pos["OA"]
            else:
                reject_idx = stage_pos["Reject"]
                prev_real = [stage_pos[s] for s in valid if stage_pos[s] < reject_idx]
                latest_idx = max(prev_real) if prev_real else stage_pos["OA"]

        offset_days = 0
        for i in range(latest_idx):
            st = STAGE_ORDER[i]
            if st in BASE_NEVER_AUTOGEN or (st == "Interview" and not has_offer):
                continue
            if st in present:
                continue

            offset_days += 3
            ts = latest_real - timedelta(days=offset_days) if latest_real else None
            auto_id = deterministic_auto_id(company, author, st, latest_real or datetime.utcnow())

            new_doc = {
                "msg_id": auto_id,
                "text": "[Auto-generated since the user submitted next stage]",
                "timestamp": ts.isoformat() if isinstance(ts, datetime) else ts,
                "author": author,
                "company": company,
                "stage": st,
                "spam": False,
                "new_grad": new_grad,
                "category": None,
                "auto": True
            }
            ops.append(UpdateOne({"msg_id": auto_id}, {"$set": new_doc}, upsert=True))
            synthetic_total += 1

        # Add all real docs as well
        for d in docs:
            out = {
                "msg_id": d["msg_id"],
                "text": d.get("text"),
                "timestamp": (
                    d["timestamp"].isoformat() if isinstance(d["timestamp"], datetime) else d["timestamp"]
                ),
                "author": d["author"],
                "company": d["company"],
                "stage": d.get("stage"),
                "spam": False,
                "new_grad": bool(d.get("new_grad", False)),
                "category": d.get("category"),
                "auto": False
            }
            ops.append(UpdateOne({"msg_id": out["msg_id"]}, {"$set": out}, upsert=True))

    print(f"[Backfill] Prepared {len(ops)} upserts ({synthetic_total} synthetic).")

    # Bulk write in batches
    if ops:
        for i in range(0, len(ops), 5000):
            chunk = ops[i:i+5000]
            dst.bulk_write(chunk, ordered=False)
        print("[Backfill] ✅ Backfilled collection updated successfully.")

    print(f"[Backfill] Synthetic stages added: {synthetic_total}")


if __name__ == "__main__":
    build_backfilled()
