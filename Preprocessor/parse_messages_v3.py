"""
Auto message parser - classifies and archives Discord messages.

This script:
1. Reads messages from unprocessed_messages
2. Classifies them using OpenAI (spam / valid interview updates)
3. Inserts valid updates into interview_processes
4. Archives all processed messages
5. Runs automatically for all channels
"""

import time
from typing import List
from openai import OpenAI
from pydantic import BaseModel
from main.Preprocessor.db_utils import get_db_manager

# ✅ OpenAI API config
# ✅ Channel keys to auto-parse
CHANNELS = ["grad_25", "grad_26", "intern_25", "intern_26"]


# ---------- Models ----------
class InterviewProcess(BaseModel):
    msg_id: str
    company: str
    stage: str
    spam: bool


class InterviewProcessList(BaseModel):
    classifications: List[InterviewProcess]



def classify_batch(client: OpenAI, text_block: str) -> List[InterviewProcess]:
    """
    Classify a batch of messages using OpenAI structured outputs.

    Args:
        client: OpenAI client
        text_block: Multiple lines in format "msg_id:: message_text"

    Returns:
        List of InterviewProcess objects
    """
    try:
        response = client.beta.chat.completions.parse(
            model="gpt-4o-2024-08-06",
            messages=[
                {
                    "role": "system",
                    "content": """
                        You are an interview data extractor.
                        Each line has the format: msg_id:: text
                        Each text may contain multiple interview updates or none.

                        For each valid update, return:
                        {msg_id, company (Capitalized), stage (one of [App, OA, Phone/R1, Onsite, HM, Offer, Reject]), spam: false}

                        If the line is irrelevant, casual chat, starts with !stats,
                        is a question, or does not follow '!process' format → return [{msg_id, spam: true}].
                        ex does anyone how how long does it take to get stripe VO -> spam 

                        Company abbreviations: db→Databricks, ca→Capital One, imc→IMC, hrt→HRT, gs→Goldman Sachs, ms→Microsoft.

                        Example input:
                        12345:: !process Google OA done
                        12346:: how to apply to Google? ->soam
                        12347:: !process db onsite
                        12378:: !stats google -> spam
                        12375:: !process google oa prayer-> spam if prayer is not answered
                        

                        Rules:
                        - If starts with !stats → spam = True
                        - If starts with !process but is a question or conversation → spam = True
                        - Only messages with valid interview updates should have spam = False

                        Example output:
                        [
                            {msg_id: "12345", company: "Google", stage: "OA", spam: false},
                            {msg_id: "12346", company: "", stage: "", spam: true},
                            {msg_id: "12347", company: "Databricks", stage: "Onsite", spam: false},
                            {msg_id: "12378", company: "", stage: "", spam: true}
                        ]
                        ✅ Rule: Always write the official company name in Title Case exactly as it appears on the company’s Careers or LinkedIn page — no abbreviations, no locations, no extra words.
                       Example: Use “JPMorgan Chase”, “HubSpot”, “Procter & Gamble”,
                        """
                },
                {"role": "user", "content": text_block.strip()},
            ],
            response_format=InterviewProcessList,
        )
        return response.choices[0].message.parsed.classifications if response.choices[0].message.parsed else []
    except Exception as e:
        print(f"❌ OpenAI API error: {e}")
        return []


# ---------- Parser ----------
def parse_unprocessed_messages(
    channel: str = None,
    batch_size: int = 30,
    insert_batch_size: int = 5
):
    is_new_grad = channel and "grad" in channel.lower()
    client = OpenAI(api_key=OPENAI_API_KEY)
    db = get_db_manager()
    if not db.test_connection():
        print("❌ DB connection failed.")
        return

    print(f"\n📂 Fetching unprocessed messages for {channel} ...")
    unprocessed = db.get_unprocessed_messages(channel=channel)

    if not unprocessed:
        print(f"✅ No unprocessed messages found for {channel}")
        return

    print(f"🚀 Parsing {len(unprocessed)} messages from {channel}...\n")

    processed_count = spam_count = valid_count = archived_count = 0
    duplicate_entries = skipped_leetbot = inserted_count = 0
    pending_docs, processed_msg_ids = [], []

    for i in range(0, len(unprocessed), batch_size):
        batch = unprocessed[i:i + batch_size]
        id_map = {}

        # Filter messages
        for msg in batch:
            author = msg.get("author", {})
            username = author.get("username") if isinstance(author, dict) else str(author)
            if username == "leetbot":
                skipped_leetbot += 1
                processed_msg_ids.append(msg["msg_id"])
                continue

            id_map[msg["msg_id"]] = {
                "author": username,
                "text": msg.get("content", ""),
                "timestamp": msg.get("timestamp", ""),
                "channel": msg.get("channel", channel)
            }

        if not id_map:
            if processed_msg_ids:
                db.archive_messages_batch(processed_msg_ids, spam=True)
                archived_count += len(processed_msg_ids)
                processed_msg_ids = []
            continue

        text_block = "\n".join(f"{mid}:: {meta['text']}" for mid, meta in id_map.items())
        print(f"🤖 Classifying batch {i//batch_size + 1} ({len(id_map)} msgs)...")

        classifications = classify_batch(client, text_block)
        if not classifications:
            batch_msg_ids = list(id_map.keys())
            db.mark_messages_processed(batch_msg_ids, spam=True, source="parsing")
            db.archive_messages_batch(batch_msg_ids, spam=True)
            archived_count += len(batch_msg_ids)
            continue

        for c in classifications:
            meta = id_map.get(c.msg_id)
            if not meta:
                continue

            db.mark_message_processed(c.msg_id, spam=c.spam, source="parsing")
            processed_count += 1
            processed_msg_ids.append(c.msg_id)

            if c.spam:
                spam_count += 1
                continue

            valid_count += 1
            if db.check_duplicate_entry(meta["author"], c.company, c.stage):
                duplicate_entries += 1
                continue

            doc = {
                "msg_id": c.msg_id,
                "text": meta["text"],
                "timestamp": meta["timestamp"],
                "author": meta["author"],
                "company": c.company,
                "stage": c.stage,
                "spam": False,
                "new_grad": is_new_grad,
                "category": meta["channel"]
            }
            pending_docs.append(doc)

            if len(pending_docs) >= insert_batch_size:
                db.safe_insert_many(pending_docs)
                inserted_count += len(pending_docs)
                pending_docs = []
                time.sleep(0.5)

        if processed_msg_ids:
            db.archive_messages_batch(processed_msg_ids, spam=False)
            archived_count += len(processed_msg_ids)
            processed_msg_ids = []

        print(f"✅ Batch {i//batch_size + 1} done | "
              f"Processed: {processed_count} | Valid: {valid_count} | Spam: {spam_count}")

    if pending_docs:
        db.safe_insert_many(pending_docs)
        inserted_count += len(pending_docs)

    print(f"\n📊 Summary for {channel}:")
    print(f"  - Processed: {processed_count}")
    print(f"  - Valid: {valid_count}")
    print(f"  - Spam: {spam_count}")
    print(f"  - Duplicates: {duplicate_entries}")
    print(f"  - Inserted: {inserted_count}")
    print(f"  - Archived: {archived_count}")
    print(f"  - Skipped leetbot: {skipped_leetbot}\n")


# ---------- Auto Runner ----------
def main():
    for channel in CHANNELS:
        parse_unprocessed_messages(channel=channel)
        time.sleep(2)  # avoid rate spike between channels

    print("\n✅ All channels parsed successfully!")


if __name__ == "__main__":
    main()
