import json

from openai import OpenAI
import csv
from pydantic import BaseModel
from typing import List
from pydantic import BaseModel
from typing import List
import json
from pymongo import MongoClient
import time


from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
uri = "uri"
# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))
# Test connection
try:
    client.admin.command('ping')
    print("✅ Pinged your deployment. Successfully connected to MongoDB!")
except Exception as e:
    print("❌ Connection failed:", e)

# --- Select database and collection ---
db = client["JobStats"]
collection = db["interview_processes"]
try:
    # Basic connectivity check
    client.admin.command('ping')
    print("✅ Pinged your deployment. Successfully connected to MongoDB!")

    # --- Try reading one document ---
    sample_doc = collection.find_one()
    if sample_doc:
        print("✅ Read access verified! Example document:")
        import json
        print(json.dumps(sample_doc, indent=2, default=str))
    else:
        print("⚠️ Connected, but collection is empty or has no readable documents.")
    collection.insert_one({"test": "ping"})
    print("✅ Write access verified.")
    collection.delete_one({"test": "ping"})
except Exception as e:
    print("❌ Connection or read failed:", e)





OPENAI_API_KEY="api"
client = OpenAI(api_key=OPENAI_API_KEY)



# ---- Safe insert helpers ----
def safe_insert_many(docs, retries=3):
    """Insert multiple documents safely with retry logic."""
    for attempt in range(retries):
        try:
            if not docs:
                return None
            result = collection.insert_many(docs, ordered=False)
            print(f"✅ Batch inserted {len(result.inserted_ids)} docs.")
            return result
        except Exception as e:
            print(f"⚠️ Insert batch failed (attempt {attempt+1}/{retries}): {e}")
            time.sleep(2)
    print("❌ Failed to insert batch after retries.")
    return None


# ---- Structured output schema ----
class InterviewProcess(BaseModel):
    msg_id: str
    company: str
    stage: str
    spam: bool


class InterviewProcessList(BaseModel):
    classifications: List[InterviewProcess]


# ---- Classification function (batch version) ----
def classify_batch(text_block: str):
    """
    Input: text_block — multiple lines, each in the format:
           msg_id:: message_text
    Output: list of InterviewProcess objects
    """
    response = client.responses.parse(
        model="gpt-5-mini-2025-08-07",
        input=[
            {
                "role": "system",
                "content": """
                    You are an interview data extractor.
                    Each line has the format: msg_id:: text
                    Each text may contain multiple interview updates or none.

                    For each valid update, return:
                    {msg_id, company (Capitalized), stage (one of [App, OA, Interview, Phone/R1, Onsite, HM, Offer, Reject]), spam: false}

                    If the line is irrelevant, casual chat, starts with !stats, 
                    is a question, or does not follow '!process' format → return [{msg_id, spam: true}].

                    Company abbreviations: db→Databricks, ca→Capital One, imc→IMC, hrt→HRT, gs→Goldman Sachs, ms→Microsoft.

                    Example input:
                    12345:: !process Google OA done -> not spam
                    12346:: how to apply to Google? -> spam
                    12347:: !process db onsite -> not spam

                    Example output:
                    [
                        {msg_id: "12345", company: "Google", stage: "OA", spam: false},
                        {msg_id: "12346", spam: true},
                        {msg_id: "12347", company: "Databricks", stage: "Onsite", spam: false}
                    ]
                    """
            },
            {"role": "user", "content": text_block.strip()},
        ],
        text_format=InterviewProcessList,
    )
    return response.output_parsed.classifications if response.output_parsed else []



# ---- Load messages ----
with open("discord_messages_2026.json", "r") as f:
    data = json.load(f)

# ---- Batch parameters ----
BATCH_SIZE = 15
INSERT_BATCH_SIZE = 50  # Number of docs to insert at once
csv_output = []
pending_docs = []

# ---- Process ----
for i in range(0, len(data[:500]), BATCH_SIZE):
    batch = data[i:i + BATCH_SIZE]

    # 🔹 Create lookup map
    id_map = {
        d["id"]: {
            "author": d["author"]["username"],
            "text": d["content"],
            "timestamp": d["timestamp"]
        }
        for d in batch
        if d["author"]["username"] != "leetbot"
    }

    # 🔹 Prepare text block
    text_block = "\n".join(f"{msg_id}:: {meta['text']}" for msg_id, meta in id_map.items())
    if not text_block.strip():
        continue

    classifications = classify_batch(text_block)

    for c in classifications:
        if getattr(c, "spam", False):
            continue

        meta = id_map.get(c.msg_id)
        if not meta:
            continue

        author = meta["author"]
        text = meta["text"]
        timestamp = meta["timestamp"]

        # Check for duplicates
        exists = collection.find_one({
            "author": author,
            "company": c.company,
            "stage": c.stage
        })
        if exists:
            print(f"⚠️ Skipping duplicate ({author}, {c.company}, {c.stage})")
            continue

        sample_doc = {
            "msg_id": c.msg_id,
            "text": text,
            "timestamp": timestamp,
            "author": author,
            "company": c.company,
            "stage": c.stage,
            "spam": c.spam,
        }

        pending_docs.append(sample_doc)

        # 🔹 Insert in batches
        if len(pending_docs) >= INSERT_BATCH_SIZE:
            safe_insert_many(pending_docs, 1)
            pending_docs = []
            time.sleep(1)

# 🔹 Insert any remaining docs
if pending_docs:
    safe_insert_many(pending_docs, 1)

print("✅ Batch processing complete.")