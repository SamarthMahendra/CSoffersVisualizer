# =============================================
# File: app.py
# Ultra-thin Flask backend — filters only
# Schema: [msg_id, text, timestamp, author, company, stage]
# =============================================
from flask import Flask, jsonify, request, send_from_directory
from datetime import datetime, timedelta
from flask_cors import CORS
import random
import os


app = Flask(__name__)
CORS(app)

STAGE_ORDER = [
    "Applied",
    "OA",
    "Phone",
    "Onsite",
    "HM",
    "Offer",
    "Accept",
]
COMPANIES = ["Google", "Stripe", "Amazon", "Netflix"]
AUTHORS = ["sam", "jane", "adam", "paul", "neetu", "Jacob"]
TEXTS = [
    "Applied via portal.", "Completed OA.", "Phone screen done.",
    "Onsite finished.", "Manager chat scheduled.",
    "Offer received!", "Accepted offer.",
]

# ---- Mock dataset (20 messages) ----
# ---- Mock dataset (chronologically consistent per candidate–company) ----
base = datetime.now() - timedelta(days=100)
random.seed(17)

messages = []
msg_id = 1

for author in AUTHORS:
    for company in COMPANIES:
        # Randomly decide how many stages this candidate reached
        max_stage_idx = random.randint(2, len(STAGE_ORDER))
        stages = STAGE_ORDER[:max_stage_idx]

        # Starting date for this candidate-company pipeline
        current_time = base + timedelta(days=random.randint(0, 40))

        for stage in stages:
            messages.append({
                "msg_id": msg_id,
                "text": random.choice(TEXTS),
                "timestamp": current_time.strftime('%Y-%m-%dT%H:%M:%S'),
                "author": author,
                "company": company,
                "stage": stage,
            })
            msg_id += 1
            # Ensure strictly increasing time between stages
            current_time += timedelta(days=random.randint(2, 7), hours=random.randint(0, 23))

# Example: messages now have correct temporal ordering per author-company


# ---- Helpers ----

def parse_date(s):
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None

def filter_msgs(msgs, start=None, end=None, companies=None, stages=None):
    out = []
    for m in msgs:
        ts = datetime.strptime(m["timestamp"], "%Y-%m-%dT%H:%M:%S")
        if start and ts < start: continue
        if end and ts > end: continue
        if companies and m["company"] not in companies: continue
        if stages and m["stage"] not in stages: continue
        out.append(m)
    return out

# ---- Routes ----
@app.route('/')
def index():
    return send_from_directory('.', 'index.html')

@app.route('/api/meta')
def meta():
    ts = [datetime.strptime(m['timestamp'], '%Y-%m-%dT%H:%M:%S') for m in messages]
    return jsonify({
        'companies': sorted({m['company'] for m in messages}),
        'stages': STAGE_ORDER,
        'min_timestamp': min(ts).strftime('%Y-%m-%dT%H:%M:%S'),
        'max_timestamp': max(ts).strftime('%Y-%m-%dT%H:%M:%S'),
        'count': len(messages)
    })

@app.route('/api/messages')
def api_messages():
    start = parse_date(request.args.get('start'))
    end = parse_date(request.args.get('end'))
    companies = set(filter(None, (request.args.get('companies') or '').split(','))) or None
    stages = set(filter(None, (request.args.get('stages') or '').split(','))) or None
    res = filter_msgs(messages, start, end, companies, stages)
    # Sort newest first
    res = sorted(res, key=lambda m: m['timestamp'], reverse=True)
    return jsonify({'items': res, 'total': len(res)})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)

