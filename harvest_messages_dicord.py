import requests
import json
import time

# === Your original request data ===
base_url = "https://discord.com/api/v9/channels/id/messages"
headers = {
    "Cookie": "cookie",
    "Authorization": "auth",
}

# === Harvesting ===
all_messages = []
last_id = None
target = 25000

# todo read from discord_messages.josn if any and then extend
with open('discord_messages_2025.json', 'r') as f:
    all_messages = json.load(f)


import random
while len(all_messages) < target:
    params = {"limit": 50}
    if last_id:
        params["before"] = last_id


    random_sleep = random.randint(1, 5)
    time.sleep(random_sleep)

    r = requests.get(base_url, headers=headers, params=params)
    if r.status_code != 200:
        print("Error:", r.status_code, r.text)
        break

    batch = r.json()
    if not batch:
        print("No more messages.")
        break

    all_messages.extend(batch)
    last_id = batch[-2]["id"]
    print(f"Fetched {len(all_messages)} messages so far... last ID: {last_id}")

    time.sleep(1.5)  # avoid rate limit

print(f"\n✅ Done. Total: {len(all_messages)} messages")

# save to file
with open("discord_messages_2025.json", "w", encoding="utf-8") as f:
    json.dump(all_messages, f, indent=2, ensure_ascii=False)
    print("💾 Saved to discord_messages_2025.json")
