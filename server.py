# =============================================
# File: app.py
# Flask + MongoDB backend (JobStats)
# Schema in MongoDB: [msg_id, text, timestamp, author, company, stage]
# =============================================
from flask import Flask, jsonify, request, send_from_directory
from datetime import datetime
from flask_cors import CORS
from pymongo import MongoClient
import os

# ---- Flask App ----
app = Flask(__name__)
CORS(app)

# ---- MongoDB Setup ----


uri = os.getenv("MONGO_URI", '')


mongo_client = MongoClient(uri)
db = mongo_client["JobStats"]
collection = db["interview_processes"]

# ---- Constants ----
STAGE_ORDER = [
    "OA", "Phone/R1", "Interview", "Onsite", "HM", "Offer", "Reject"
]

# ---- Helpers ----
def parse_date(s):
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None

def fill_missing_stages(messages):
    """
    Postprocessing: For each (company, author) combination, optionally add
    earlier missing stages *only if there is NO Reject* in the journey.

    Notes:
    - Never add anything if Reject is present.
    - Never autogenerate "App" or "Interview" (as in your code).
    - Never autogenerate "Offer" (extra guard added).
    """
    if not messages:
        return messages

    # Group messages by (company, author)
    grouped = {}
    for msg in messages:
        key = (msg.get('company', ''), msg.get('author', ''))
        grouped.setdefault(key, []).append(msg)

    augmented = []
    for (company, author), msgs in grouped.items():
        present_stages = {m.get('stage') for m in msgs if m.get('stage')}

        # If Reject exists, just pass through original messages – do NOT autogen.
        if 'Reject' in present_stages:
            augmented.extend(msgs)
            continue

        # If only 'App' exists, also pass through messages as-is.
        if present_stages == {'App'}:
            augmented.extend(msgs)
            continue

        # Find the earliest present stage in STAGE_ORDER
        earliest_idx = len(STAGE_ORDER)
        for st in present_stages:
            if st in STAGE_ORDER:
                earliest_idx = min(earliest_idx, STAGE_ORDER.index(st))

        # Add earlier missing stages (guards: no App, no Interview, no Offer)
        to_add = []
        for i in range(earliest_idx):
            st = STAGE_ORDER[i]
            if st not in present_stages and st not in {'App', 'Offer'}:
                to_add.append({
                    'company': company,
                    'author': author,
                    'stage': st,
                    'timestamp': None,  # synthetic
                    'text': '[Auto-generated since the user submitted next stage on discord]',
                    'msg_id': f'auto_{company}_{author}_{st}',
                    'spam': False
                })

        augmented.extend(to_add)
        augmented.extend(msgs)

    return augmented


# ---- Routes ----
@app.route('/')
def index():
    return send_from_directory('.', 'index.html')


@app.route('/api/meta')
def meta():
    """Return meta information: companies, stages, date range, and author count."""
    query = {"spam": False}
    docs = list(collection.find(query, {"timestamp": 1, "company": 1, "author": 1}))
    if not docs:
        return jsonify({
            'companies': [],
            'stages': STAGE_ORDER,
            'min_timestamp': None,
            'max_timestamp': None,
            'count': 0,
            'author_count': 0
        })

    timestamps = [
        datetime.fromisoformat(d['timestamp']) if isinstance(d['timestamp'], str) else d['timestamp']
        for d in docs if d.get('timestamp')
    ]
    companies = sorted({d.get('company', '') for d in docs if d.get('company')})
    authors = {d.get('author', '') for d in docs if d.get('author')}

    return jsonify({
        'companies': companies,
        'stages': STAGE_ORDER,
        'min_timestamp': min(timestamps).strftime('%Y-%m-%dT%H:%M:%S') if timestamps else None,
        'max_timestamp': max(timestamps).strftime('%Y-%m-%dT%H:%M:%S') if timestamps else None,
        'count': len(docs),
        'author_count': len(authors)
    })


@app.route('/api/messages')
def api_messages():
    """Return filtered messages based on query params."""
    start = parse_date(request.args.get('start'))
    end = parse_date(request.args.get('end'))
    companies = [c for c in (request.args.get('companies') or '').split(',') if c]
    stages = [s for s in (request.args.get('stages') or '').split(',') if s]

    query = {"spam": False, "stage": {"$ne": "App"}}

    # Apply company filter (OR logic with $in operator)
    if companies:
        query['company'] = {'$in': companies}

    # Apply stage filter
    if stages:
        query['stage'] = {'$in': stages}

    # Apply date filters
    if start or end:
        query['timestamp'] = {}
        if start:
            query['timestamp']['$gte'] = start.isoformat()
        if end:
            query['timestamp']['$lte'] = end.isoformat()

    # Query MongoDB
    cursor = collection.find(query, {"_id": 0}).sort("timestamp", -1)
    results = list(cursor)

    # Apply postprocessing: add missing stages
    augmented_results = fill_missing_stages(results)

    return jsonify({'items': augmented_results, 'total': len(augmented_results)})

# ---- Entry ----
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
