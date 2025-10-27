import json

from openai import OpenAI
import csv
from pydantic import BaseModel
from typing import List


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

# --- Database and collection ---
db = client["JobStats"]
collection = db["interview_processes"]




OPENAI_API_KEY="key"
client = OpenAI(api_key=OPENAI_API_KEY)


# ---- Define structured output schema ----
class InterviewProcess(BaseModel):
    company: str
    stage: str
    spam: bool

class InterviewProcessList(BaseModel):
    classifications: List[InterviewProcess]

# ---- Classification function ----
def classify(input_text: str):
    response = client.responses.parse(
        model="gpt-5-nano",
        input=[
            {
                "role": "system",
                "content": """
                    You are an interview data extractor.\nEach message may contain multiple interview updates or none.\n
                    If the message contains one or more valid interview process updates,
                    return a list of {company, stage, spam=False} for each one.\n
                    If the message is casual chat, question, stats request, or irrelevant,
                    return one item with spam=True.\n\n
                    company name : first letter capital : like Google
                    stages : App, OA, phone, Onsite, HM, Offer, Reject
                    Example:\n
                    Input: '!process Google OA done! !process Meta Onsite scheduled.'\n
                    Output: [\n
                      {company: 'Google', stage: 'OA', spam: False},\n
                      {company: 'Meta', stage: 'Onsite', spam: False}\n
                    ]\n\n
                    If input looks like a normal conversation (e.g., 'how was your interview?'),
                    "return [ {spam: True} ].
                    """
            },
            {"role": "user", "content": input_text},
        ],
        text_format=InterviewProcessList,
    )

    output = response.output_parsed
    return output.classifications if output else []

# ---- Load messages and classify ----
with open("discord_messages_2026.json", "r") as f:
    data = json.load(f)

csv_output = []

for d in data[:500]:
    text = d["content"]
    timestamp = d["timestamp"]
    author = d["author"]["username"]
    msg_id = d["id"]

    exists_msg_id = collection.find_one({
        "msg_id": msg_id,
        "author": author,
    })

    if exists_msg_id:
        print(f"⚠️ Skipping duplicate ({msg_id}, {author})")
        continue



    classifications = classify(text)
    if not classifications:
        continue

    for c in classifications:
        # ✅ TODO completed — check duplicates before insert
        exists = collection.find_one({
            "author": author,
            "company": c.company,
            "stage": c.stage
        })

        if exists:
            print(f"⚠️ Skipping duplicate ({author}, {c.company}, {c.stage})")
            continue
        sample_doc = {
            "msg_id": msg_id,
            "text": text,
            "timestamp": timestamp,  # ISO string form
            "author": author,
            "company": c.company,
            "stage": c.stage,
            "spam": c.spam,
        }
        result = collection.insert_one(sample_doc)
        print(f"✅ Inserted document with _id: {result.inserted_id}")


