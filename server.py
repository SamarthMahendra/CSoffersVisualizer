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
    "App", "OA", "Phone/R1", "Interview", "Onsite", "HM", "Offer"
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

# ---- Routes ----
@app.route('/')
def index():
    return send_from_directory('.', 'index.html')


@app.route('/api/meta')
def meta():
    """Return meta information: companies, stages, and date range."""
    query = {"spam": False}
    docs = list(collection.find(query, {"timestamp": 1, "company": 1}))
    if not docs:
        return jsonify({'companies': [], 'stages': STAGE_ORDER, 'min_timestamp': None, 'max_timestamp': None, 'count': 0})

    timestamps = [
        datetime.fromisoformat(d['timestamp']) if isinstance(d['timestamp'], str) else d['timestamp']
        for d in docs if d.get('timestamp')
    ]
    companies = sorted({d.get('company', '') for d in docs if d.get('company')})

    return jsonify({
        'companies': companies,
        'stages': STAGE_ORDER,
        'min_timestamp': min(timestamps).strftime('%Y-%m-%dT%H:%M:%S') if timestamps else None,
        'max_timestamp': max(timestamps).strftime('%Y-%m-%dT%H:%M:%S') if timestamps else None,
        'count': len(docs)
    })


@app.route('/api/messages')
def api_messages():
    """Return filtered messages based on query params."""
    start = parse_date(request.args.get('start'))
    end = parse_date(request.args.get('end'))
    companies = [c for c in (request.args.get('companies') or '').split(',') if c]
    stages = [s for s in (request.args.get('stages') or '').split(',') if s]

    query = {"spam": False}

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

    return jsonify({'items': results, 'total': len(results)})

# ---- Entry ----
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
