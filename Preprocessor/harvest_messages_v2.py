"""
Efficient Discord message harvesting - saves directly to MongoDB.

Auto version:
1. Harvests all configured channels
2. Uses cutoff date = 2 days before today
3. Saves to MongoDB automatically without CLI args
"""

import requests
import time
import random
from datetime import datetime, timezone, timedelta
from main.Preprocessor.db_utils import get_db_manager

# Channel configurations
CHANNELS = {
    "grad_25": {"id": "1242309460689424504"},
    "grad_26": {"id": "1395661511950729308"},
    "intern_25": {"id": "1245256033530548268"},
    "intern_26": {"id": "1395661226507505745"},
}

BASE_URL_TEMPLATE = "https://discord.com/api/v9/channels/{channel_id}/messages"
HEADERS = {
    "Cookie": "",
    "Authorization": ""
}


def harvest_channel(channel_key, target=10000, cutoff_date=None, batch_save_size=100):
    if channel_key not in CHANNELS:
        print(f"❌ Unknown channel: {channel_key}")
        return

    channel_id = CHANNELS[channel_key]["id"]
    base_url = BASE_URL_TEMPLATE.format(channel_id=channel_id)

    db = get_db_manager()
    if not db.test_connection():
        print("❌ Cannot connect to DB")
        return

    if cutoff_date is None:
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=2)

    existing_unprocessed = db.count_unprocessed_messages(channel=channel_key)
    print(f"\n{'='*60}\n🚀 Harvesting {channel_key} | Cutoff: {cutoff_date}\n{'='*60}")

    last_id = None
    new_messages, total_new_count = [], 0
    skipped_processed = skipped_before_cutoff = api_calls = 0

    while total_new_count < target:
        params = {"limit": 50}
        if last_id:
            params["before"] = last_id

        time.sleep(random.randint(1, 3))
        api_calls += 1
        try:
            resp = requests.get(base_url, headers=HEADERS, params=params)
            if resp.status_code != 200:
                print(f"❌ {resp.status_code}: {resp.text}")
                break
            batch = resp.json()
            if not batch:
                break
        except Exception as e:
            print(f"❌ Request failed: {e}")
            break

        batch_ids = [m["id"] for m in batch]
        processed_status = db.are_messages_processed(batch_ids)
        new_in_batch, stop_harvest = 0, False

        for msg in batch:
            msg_id = msg["id"]
            if processed_status.get(msg_id):
                skipped_processed += 1
                if skipped_processed > 100:
                    stop_harvest = True
                    break
                continue

            timestamp = datetime.fromisoformat(msg["timestamp"])
            if timestamp < cutoff_date:
                skipped_before_cutoff += 1
                db.mark_message_processed(msg_id, source="harvesting")
                if skipped_before_cutoff > 50:
                    stop_harvest = True
                    break
                continue

            msg["msg_id"] = msg_id
            new_messages.append(msg)
            db.mark_message_processed(msg_id, spam=False, source="harvesting")
            total_new_count += 1
            new_in_batch += 1

            if len(new_messages) >= batch_save_size:
                db.add_unprocessed_messages(new_messages, channel=channel_key)
                new_messages = []

        if stop_harvest:
            break
        last_id = batch[-1]["id"]
        print(f"📊 {channel_key}: {total_new_count} new | {api_calls} calls | Skipped {skipped_processed}")
        time.sleep(1.5)

    if new_messages:
        db.add_unprocessed_messages(new_messages, channel=channel_key)

    print(f"\n✅ Done {channel_key}: {total_new_count} new, {skipped_processed} dupes, {api_calls} calls\n")

def main():
    cutoff = datetime.now(timezone.utc) - timedelta(days=2)
    for ch in CHANNELS.keys():
        harvest_channel(ch, cutoff_date=cutoff)

if __name__ == "__main__":
    main()
