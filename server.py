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
    """Return meta information: companies, stages, date range, author count, and total submissions."""
    query = {"spam": False, "stage": {"$ne": "App"}}
    docs = list(collection.find(query, {"timestamp": 1, "company": 1, "author": 1}))
    if not docs:
        return jsonify({
            'companies': [],
            'stages': STAGE_ORDER,
            'min_timestamp': None,
            'max_timestamp': None,
            'count': 0,
            'author_count': 0,
            'submission_count': 0
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
        'author_count': len(authors),
        'submission_count': len(docs)  # Total number of submissions (same as count)
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


@app.route('/api/funnel')
def api_funnel():
    """Return stage counts for funnel chart."""
    start = parse_date(request.args.get('start'))
    end = parse_date(request.args.get('end'))
    companies = [c for c in (request.args.get('companies') or '').split(',') if c]

    query = {"spam": False, "stage": {"$ne": "App"}}

    if companies:
        query['company'] = {'$in': companies}

    if start or end:
        query['timestamp'] = {}
        if start:
            query['timestamp']['$gte'] = start.isoformat()
        if end:
            query['timestamp']['$lte'] = end.isoformat()

    cursor = collection.find(query, {"stage": 1, "_id": 0})
    results = list(cursor)

    # Count stages
    stage_counts = {stage: 0 for stage in STAGE_ORDER}
    for doc in results:
        stage = doc.get('stage')
        if stage in stage_counts:
            stage_counts[stage] += 1

    return jsonify({
        'stages': STAGE_ORDER,
        'counts': stage_counts
    })


@app.route('/api/heatmap')
def api_heatmap():
    """Return conversion matrix data for heatmap visualization."""
    start = parse_date(request.args.get('start'))
    end = parse_date(request.args.get('end'))
    companies = [c for c in (request.args.get('companies') or '').split(',') if c]
    top_n = int(request.args.get('top_n', 8))  # Number of top companies to show

    query = {"spam": False, "stage": {"$ne": "App"}}

    if companies:
        query['company'] = {'$in': companies}

    if start or end:
        query['timestamp'] = {}
        if start:
            query['timestamp']['$gte'] = start.isoformat()
        if end:
            query['timestamp']['$lte'] = end.isoformat()

    cursor = collection.find(query, {"company": 1, "author": 1, "stage": 1, "timestamp": 1, "_id": 0})
    results = list(cursor)

    # Get top N companies by activity
    company_counts = {}
    for doc in results:
        company = doc.get('company')
        if company:
            company_counts[company] = company_counts.get(company, 0) + 1

    top_companies = sorted(company_counts.items(), key=lambda x: x[1], reverse=True)[:top_n]
    top_company_names = [c[0] for c in top_companies]

    # Build conversion matrix
    conv_matrix = {}
    applications = {}  # key: company|author -> [{stage, timestamp}]

    for doc in results:
        company = doc.get('company')
        author = doc.get('author')
        stage = doc.get('stage')
        timestamp = doc.get('timestamp')

        if not company or not author:
            continue

        key = f"{company}|{author}"
        if key not in applications:
            applications[key] = []

        ts = None
        if timestamp:
            try:
                ts = datetime.fromisoformat(timestamp) if isinstance(timestamp, str) else timestamp
                ts = ts.timestamp() * 1000  # Convert to milliseconds
            except:
                pass

        applications[key].append({'stage': stage, 'timestamp': ts})

    # Calculate stage counts per company
    per_company_stage_counts = {}
    for company in top_company_names:
        per_company_stage_counts[company] = {stage: 0 for stage in STAGE_ORDER}

    for doc in results:
        company = doc.get('company')
        stage = doc.get('stage')
        if company in per_company_stage_counts and stage in per_company_stage_counts[company]:
            per_company_stage_counts[company][stage] += 1

    # Calculate stage-to-stage conversions (skip ...→Reject)
    for company in top_company_names:
        conv_matrix[company] = {}
        for i in range(len(STAGE_ORDER) - 1):
            from_stage = STAGE_ORDER[i]
            to_stage = STAGE_ORDER[i + 1]

            if to_stage.lower() == "reject":
                continue

            from_count = per_company_stage_counts[company][from_stage]
            to_count = per_company_stage_counts[company][to_stage]

            pct = (to_count / from_count * 100) if from_count > 0 else 0
            conv_matrix[company][f"{from_stage}→{to_stage}"] = round(pct, 1)

    # Calculate Overall→Reject
    apps_by_company = {company: set() for company in top_company_names}
    for key in applications.keys():
        company = key.split('|')[0]
        if company in apps_by_company:
            apps_by_company[company].add(key)

    for company in top_company_names:
        keys = list(apps_by_company[company])
        if not keys:
            conv_matrix[company]["Overall→Reject"] = 0
            continue

        rejected = 0
        for key in keys:
            stages_seen = {app['stage'] for app in applications[key] if app.get('stage')}
            if 'Reject' in stages_seen:
                rejected += 1

        conv_matrix[company]["Overall→Reject"] = round((rejected / len(keys) * 100), 1)

    # Build transitions list (for ordering)
    transitions = []
    for i in range(len(STAGE_ORDER) - 1):
        to_stage = STAGE_ORDER[i + 1]
        if to_stage.lower() != "reject":
            transitions.append(f"{STAGE_ORDER[i]}→{to_stage}")
    transitions.append("Overall→Reject")

    return jsonify({
        'companies': top_company_names,
        'transitions': transitions,
        'conversion_matrix': conv_matrix
    })


@app.route('/api/timeline')
def api_timeline():
    """Return average days between stage transitions."""
    start = parse_date(request.args.get('start'))
    end = parse_date(request.args.get('end'))
    companies = [c for c in (request.args.get('companies') or '').split(',') if c]

    query = {"spam": False, "stage": {"$ne": "App"}}

    if companies:
        query['company'] = {'$in': companies}

    if start or end:
        query['timestamp'] = {}
        if start:
            query['timestamp']['$gte'] = start.isoformat()
        if end:
            query['timestamp']['$lte'] = end.isoformat()

    cursor = collection.find(query, {"company": 1, "author": 1, "stage": 1, "timestamp": 1, "_id": 0})
    results = list(cursor)

    # Build applications
    applications = {}  # key: company|author -> [{stage, timestamp}]

    for doc in results:
        company = doc.get('company')
        author = doc.get('author')
        stage = doc.get('stage')
        timestamp = doc.get('timestamp')

        if not company or not author or not stage:
            continue

        key = f"{company}|{author}"
        if key not in applications:
            applications[key] = []

        ts = None
        if timestamp:
            try:
                ts = datetime.fromisoformat(timestamp) if isinstance(timestamp, str) else timestamp
                ts = ts.timestamp() * 1000  # Convert to milliseconds
            except:
                pass

        applications[key].append({'stage': stage, 'timestamp': ts})

    # Build earliest timestamps per stage for each application
    app_earliest = {}
    for key, msgs in applications.items():
        stage_map = {}
        for msg in msgs:
            stage = msg.get('stage')
            ts = msg.get('timestamp')
            if not stage or ts is None:
                continue
            if stage not in stage_map or ts < stage_map[stage]:
                stage_map[stage] = ts
        app_earliest[key] = stage_map

    # Calculate transition days
    transition_days = {}
    for i in range(len(STAGE_ORDER) - 1):
        from_stage = STAGE_ORDER[i]
        to_stage = STAGE_ORDER[i + 1]
        transition_days[f"{from_stage}→{to_stage}"] = []

    for stage_map in app_earliest.values():
        for i in range(len(STAGE_ORDER) - 1):
            from_stage = STAGE_ORDER[i]
            to_stage = STAGE_ORDER[i + 1]

            if from_stage in stage_map and to_stage in stage_map:
                days = (stage_map[to_stage] - stage_map[from_stage]) / (1000 * 60 * 60 * 24)
                if days >= 0:
                    transition_days[f"{from_stage}→{to_stage}"].append(days)

    # Calculate HM→Reject (Overall→Reject)
    hm_to_reject_days = []
    for stage_map in app_earliest.values():
        if "HM" in stage_map and "Reject" in stage_map:
            days = (stage_map["Reject"] - stage_map["HM"]) / (1000 * 60 * 60 * 24)
            if days >= 0:
                hm_to_reject_days.append(days)

    # Calculate averages
    stage_times = {}
    for transition, days_list in transition_days.items():
        avg = sum(days_list) / len(days_list) if days_list else 0
        stage_times[transition] = round(avg, 1)

    overall_reject_avg = sum(hm_to_reject_days) / len(hm_to_reject_days) if hm_to_reject_days else 0
    stage_times["Overall→Reject"] = round(overall_reject_avg, 1)

    # Build transitions list for ordering
    transitions = []
    for i in range(len(STAGE_ORDER) - 1):
        transitions.append(f"{STAGE_ORDER[i]}→{STAGE_ORDER[i + 1]}")

    # Filter to only include transitions that aren't ...→Reject (except Overall→Reject)
    filtered_transitions = [t for t in transitions if not t.endswith("→Reject")]
    filtered_transitions.append("Overall→Reject")

    return jsonify({
        'transitions': filtered_transitions,
        'stage_times': stage_times
    })


@app.route('/api/companies/search')
def api_companies_search():
    """Return companies with counts for search/autocomplete."""
    search_term = request.args.get('q', '').lower()
    start = parse_date(request.args.get('start'))
    end = parse_date(request.args.get('end'))
    limit = int(request.args.get('limit', 50))

    query = {"spam": False, "stage": {"$ne": "App"}}

    if start or end:
        query['timestamp'] = {}
        if start:
            query['timestamp']['$gte'] = start.isoformat()
        if end:
            query['timestamp']['$lte'] = end.isoformat()

    cursor = collection.find(query, {"company": 1, "_id": 0})
    results = list(cursor)

    # Count companies
    company_counts = {}
    for doc in results:
        company = doc.get('company')
        if company:
            company_counts[company] = company_counts.get(company, 0) + 1

    # Filter by search term
    filtered_companies = []
    for company, count in company_counts.items():
        if search_term in company.lower():
            filtered_companies.append({'name': company, 'count': count})

    # Sort by count (descending) and limit
    filtered_companies.sort(key=lambda x: x['count'], reverse=True)
    filtered_companies = filtered_companies[:limit]

    return jsonify({
        'companies': filtered_companies,
        'total': len(company_counts)
    })


@app.route('/api/dashboard')
def api_dashboard():
    """
    Comprehensive dashboard API that returns all data in one call.
    This reduces the number of requests and improves performance.
    """
    start = parse_date(request.args.get('start'))
    end = parse_date(request.args.get('end'))
    companies = [c for c in (request.args.get('companies') or '').split(',') if c]
    top_n = int(request.args.get('top_n', 8))

    query = {"spam": False, "stage": {"$ne": "App"}}

    if companies:
        query['company'] = {'$in': companies}

    if start or end:
        query['timestamp'] = {}
        if start:
            query['timestamp']['$gte'] = start.isoformat()
        if end:
            query['timestamp']['$lte'] = end.isoformat()

    # Fetch all data once
    cursor = collection.find(query, {"company": 1, "author": 1, "stage": 1, "timestamp": 1, "_id": 0})
    results = list(cursor)

    # ===== FUNNEL DATA =====
    stage_counts = {stage: 0 for stage in STAGE_ORDER}
    for doc in results:
        stage = doc.get('stage')
        if stage in stage_counts:
            stage_counts[stage] += 1

    # ===== COMPANY COUNTS FOR HEATMAP =====
    company_counts = {}
    for doc in results:
        company = doc.get('company')
        if company:
            company_counts[company] = company_counts.get(company, 0) + 1

    top_companies = sorted(company_counts.items(), key=lambda x: x[1], reverse=True)[:top_n]
    top_company_names = [c[0] for c in top_companies]

    # ===== BUILD APPLICATIONS =====
    applications = {}  # key: company|author -> [{stage, timestamp}]

    for doc in results:
        company = doc.get('company')
        author = doc.get('author')
        stage = doc.get('stage')
        timestamp = doc.get('timestamp')

        if not company or not author:
            continue

        key = f"{company}|{author}"
        if key not in applications:
            applications[key] = []

        ts = None
        if timestamp:
            try:
                ts = datetime.fromisoformat(timestamp) if isinstance(timestamp, str) else timestamp
                ts = ts.timestamp() * 1000  # Convert to milliseconds
            except:
                pass

        applications[key].append({'stage': stage, 'timestamp': ts})

    # ===== HEATMAP CONVERSION MATRIX =====
    per_company_stage_counts = {}
    for company in top_company_names:
        per_company_stage_counts[company] = {stage: 0 for stage in STAGE_ORDER}

    for doc in results:
        company = doc.get('company')
        stage = doc.get('stage')
        if company in per_company_stage_counts and stage in per_company_stage_counts[company]:
            per_company_stage_counts[company][stage] += 1

    conv_matrix = {}
    for company in top_company_names:
        conv_matrix[company] = {}
        for i in range(len(STAGE_ORDER) - 1):
            from_stage = STAGE_ORDER[i]
            to_stage = STAGE_ORDER[i + 1]

            if to_stage.lower() == "reject":
                continue

            from_count = per_company_stage_counts[company][from_stage]
            to_count = per_company_stage_counts[company][to_stage]

            pct = (to_count / from_count * 100) if from_count > 0 else 0
            conv_matrix[company][f"{from_stage}→{to_stage}"] = round(pct, 1)

    # Calculate Overall→Reject
    apps_by_company = {company: set() for company in top_company_names}
    for key in applications.keys():
        company = key.split('|')[0]
        if company in apps_by_company:
            apps_by_company[company].add(key)

    for company in top_company_names:
        keys = list(apps_by_company[company])
        if not keys:
            conv_matrix[company]["Overall→Reject"] = 0
        else:
            rejected = 0
            for key in keys:
                stages_seen = {app['stage'] for app in applications[key] if app.get('stage')}
                if 'Reject' in stages_seen:
                    rejected += 1
            conv_matrix[company]["Overall→Reject"] = round((rejected / len(keys) * 100), 1)

    # ===== TIMELINE DATA =====
    app_earliest = {}
    for key, msgs in applications.items():
        stage_map = {}
        for msg in msgs:
            stage = msg.get('stage')
            ts = msg.get('timestamp')
            if not stage or ts is None:
                continue
            if stage not in stage_map or ts < stage_map[stage]:
                stage_map[stage] = ts
        app_earliest[key] = stage_map

    transition_days = {}
    for i in range(len(STAGE_ORDER) - 1):
        from_stage = STAGE_ORDER[i]
        to_stage = STAGE_ORDER[i + 1]
        transition_days[f"{from_stage}→{to_stage}"] = []

    for stage_map in app_earliest.values():
        for i in range(len(STAGE_ORDER) - 1):
            from_stage = STAGE_ORDER[i]
            to_stage = STAGE_ORDER[i + 1]

            if from_stage in stage_map and to_stage in stage_map:
                days = (stage_map[to_stage] - stage_map[from_stage]) / (1000 * 60 * 60 * 24)
                if days >= 0:
                    transition_days[f"{from_stage}→{to_stage}"].append(days)

    hm_to_reject_days = []
    for stage_map in app_earliest.values():
        if "HM" in stage_map and "Reject" in stage_map:
            days = (stage_map["Reject"] - stage_map["HM"]) / (1000 * 60 * 60 * 24)
            if days >= 0:
                hm_to_reject_days.append(days)

    stage_times = {}
    for transition, days_list in transition_days.items():
        avg = sum(days_list) / len(days_list) if days_list else 0
        stage_times[transition] = round(avg, 1)

    overall_reject_avg = sum(hm_to_reject_days) / len(hm_to_reject_days) if hm_to_reject_days else 0
    stage_times["Overall→Reject"] = round(overall_reject_avg, 1)

    # Build transitions list
    transitions = []
    for i in range(len(STAGE_ORDER) - 1):
        to_stage = STAGE_ORDER[i + 1]
        if to_stage.lower() != "reject":
            transitions.append(f"{STAGE_ORDER[i]}→{to_stage}")
    transitions.append("Overall→Reject")

    filtered_transitions = [t for t in transition_days.keys() if not t.endswith("→Reject")]
    filtered_transitions.append("Overall→Reject")

    # ===== RETURN ALL DATA =====
    return jsonify({
        'funnel': {
            'stages': STAGE_ORDER,
            'counts': stage_counts
        },
        'heatmap': {
            'companies': top_company_names,
            'transitions': transitions,
            'conversion_matrix': conv_matrix
        },
        'timeline': {
            'transitions': filtered_transitions,
            'stage_times': stage_times
        },
        'summary': {
            'total_records': len(results),
            'unique_companies': len(company_counts),
            'unique_candidates': len({k.split('|')[1] for k in applications.keys() if '|' in k})
        }
    })


# ---- Entry ----
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
