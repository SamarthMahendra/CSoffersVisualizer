# =============================================
# File: app.py
# Flask + MongoDB backend (JobStats)
# Schema in MongoDB: [msg_id, text, timestamp, author, company, stage]
# =============================================
from flask import Flask, jsonify, request, send_from_directory
from datetime import datetime, timedelta
from flask_cors import CORS
from pymongo import MongoClient
import os

# ---- Flask App ----
app = Flask(__name__)
CORS(app)

# ---- MongoDB Setup ----
MONGO_URI = ""


uri = os.getenv("MONGO_URI", MONGO_URI)

mongo_client = MongoClient(uri)
db = mongo_client["JobStats"]
collection = db["interview_processes_backfilled"]
sessions_collection = db["active_sessions"]
feedback_collection = db["feedback"]

# ---- Constants ----
STAGE_ORDER = [
    "OA", "Phone/R1", "Onsite", "HM", "Offer", "Reject"
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
from datetime import timedelta

# def fill_missing_stages(messages):
#     """
#     Postprocessing: For each (company, author, new_grad) journey, optionally add earlier missing stages.
#
#     Improvements:
#     - Synthetic timestamps: each earlier backfilled stage gets a timestamp 3 days before the nearest real one.
#     - Prevents None timestamps and keeps analytics timelines consistent.
#     """
#
#     print(f"[Backfill] Starting backfilling process with {len(messages)} messages")
#     if not messages:
#         print("[Backfill] No messages to process, returning empty list")
#         return messages
#
#     stage_pos = {st: i for i, st in enumerate(STAGE_ORDER)}
#     BASE_NEVER_AUTOGEN = {"App", "Offer"}
#
#     grouped = {}
#     for msg in messages:
#         key = (msg.get("company", ""), msg.get("author", ""), msg.get("new_grad", False))
#         grouped.setdefault(key, []).append(msg)
#     print(f"[Backfill] Grouped messages into {len(grouped)} unique user journeys")
#
#     # ---- Fetch real stage submissions once
#     or_conditions = [
#         {
#             "company": c,
#             "author": a,
#             "new_grad": ng,
#             "spam": False,
#             "msg_id": {"$not": {"$regex": "^auto_"}}
#         }
#         for (c, a, ng) in grouped.keys()
#     ]
#
#     real_stage_submissions = {}
#     if or_conditions:
#         print(f"[Backfill] Querying database for {len(or_conditions)} journeys")
#         cursor = collection.find(
#             {"$or": or_conditions},
#             {"company": 1, "author": 1, "new_grad": 1, "stage": 1, "timestamp": 1, "_id": 0}
#         )
#         for doc in cursor:
#             key = (doc.get("company", ""), doc.get("author", ""), doc.get("new_grad", False))
#             real_stage_submissions.setdefault(key, []).append(doc)
#         print(f"[Backfill] Found real stage submissions for {len(real_stage_submissions)} journeys")
#
#     augmented = []
#     total_backfilled_stages = 0
#     skipped_because_real = []
#
#     for (company, author, new_grad_status), msgs in grouped.items():
#         present_stages = {str(m.get("stage")).strip() for m in msgs if m.get("stage")}
#         job_type = "new_grad" if new_grad_status else "intern"
#
#         if present_stages == {"App"}:
#             augmented.extend(msgs)
#             continue
#
#         has_reject = "Reject" in present_stages
#         has_offer = "Offer" in present_stages
#
#         # Get latest real timestamp for synthetic offsets
#         real_docs = real_stage_submissions.get((company, author, new_grad_status), [])
#         from datetime import datetime
#
#         real_timestamps = []
#         for d in real_docs:
#             ts = d.get("timestamp")
#             if not ts:
#                 continue
#             if isinstance(ts, str):
#                 try:
#                     # Handles timezone-aware ISO strings like "2025-10-27T04:32:52.039000+00:00"
#                     ts = datetime.fromisoformat(ts)
#                 except ValueError:
#                     # fallback: remove 'Z' if present (UTC suffix)
#                     ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
#             real_timestamps.append(ts)
#
#         latest_real_ts = max(real_timestamps) if real_timestamps else None
#
#         valid_stages = [s for s in present_stages if s in stage_pos]
#         if not valid_stages:
#             print(f"[Backfill][Skip] {company} | {author} ({job_type}): No valid stages in STAGE_ORDER — skipping.")
#             augmented.extend(msgs)
#             continue
#
#
#         if not has_reject:
#             latest_idx = max(stage_pos[s] for s in valid_stages)
#         else:
#             if present_stages == {"Reject"}:
#                 latest_idx = stage_pos["OA"]
#             else:
#                 reject_idx = stage_pos["Reject"]
#                 prev_real_stages = [stage_pos[s] for s in valid_stages if stage_pos[s] < reject_idx]
#                 latest_idx = max(prev_real_stages) if prev_real_stages else stage_pos["OA"]
#
#         to_add = []
#         offset_days = 0
#
#         for i in range(latest_idx):
#             st = STAGE_ORDER[i]
#
#             # Never autogen "App"/"Offer"; autogen Interview only if Offer exists
#             if st in BASE_NEVER_AUTOGEN or (st == "Interview" and not has_offer):
#                 continue
#             if st in present_stages:
#                 continue
#             if any(d.get("stage") == st for d in real_docs):
#                 skipped_because_real.append({
#                     "company": company,
#                     "author": author,
#                     "new_grad": new_grad_status,
#                     "stage": st
#                 })
#                 continue
#
#             # --- Synthetic timestamp ---
#             offset_days += 3
#             ts = None
#             if latest_real_ts:
#                 ts = latest_real_ts - timedelta(days=offset_days)
#
#             to_add.append({
#                 "company": company,
#                 "author": author,
#                 "stage": st,
#                 "timestamp": ts,
#                 "text": "[Auto-generated since the user submitted next stage on discord]",
#                 "msg_id": f"auto_{company}_{author}_{st}",
#                 "spam": False,
#                 "new_grad": new_grad_status
#             })
#
#         if to_add:
#             bf_list = [x["stage"] for x in to_add]
#             print(f"[Backfill] {company} | {author} ({job_type}): Backfilled {bf_list}")
#             total_backfilled_stages += len(to_add)
#
#         augmented.extend(to_add)
#         augmented.extend(msgs)
#
#     print(f"[Backfill] ✅ Done. Total backfilled: {total_backfilled_stages}. Output: {len(augmented)} messages")
#
#     # ---- Log skipped due to real submissions ----
#     if skipped_because_real:
#         from collections import defaultdict
#         print(f"[Backfill][Skip-Real] Skipped {len(skipped_because_real)} stages because real submissions already exist.")
#         by_journey = defaultdict(list)
#         for rec in skipped_because_real:
#             by_journey[(rec["company"], rec["author"], rec["new_grad"])].append(rec["stage"])
#         print("[Backfill][Skip-Real] Examples (up to 10 journeys):")
#         for i, ((comp, auth, ng), stages) in enumerate(by_journey.items()):
#             print(f"  - {comp} | {auth} | {'new_grad' if ng else 'intern'}: {sorted(set(stages))}")
#             if i >= 9:
#                 break
#     else:
#         print("[Backfill][Skip-Real] No stages skipped due to real submissions.")
#
#     return augmented





# ---- Routes ----
@app.route('/')
def index():
    return send_from_directory('.', 'index.html')

@app.route('/beta')
def index2():
    return send_from_directory('.', 'index2.html')


@app.route('/sitemap.xml')
def sitemap():
    return send_from_directory('.', 'sitemap.xml', mimetype='application/xml')


@app.route('/robots.txt')
def robots():
    return send_from_directory('.', 'robots.txt', mimetype='text/plain')


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
    print(f"[API /api/messages] Request received with params: start={request.args.get('start')}, end={request.args.get('end')}, companies={request.args.get('companies')}, stages={request.args.get('stages')}, job_types={request.args.get('job_types')}")

    start = parse_date(request.args.get('start'))
    end = parse_date(request.args.get('end'))
    companies = [c for c in (request.args.get('companies') or '').split(',') if c]
    stages = [s for s in (request.args.get('stages') or '').split(',') if s]
    job_types = [j for j in (request.args.get('job_types') or '').split(',') if j]

    query = {"spam": False, "stage": {"$ne": "App"}}

    # Apply company filter (OR logic with $in operator)
    if companies:
        query['company'] = {'$in': companies}
        print(f"[API /api/messages] Filtering by companies: {companies}")

    # Apply stage filter
    if stages:
        query['stage'] = {'$in': stages}
        print(f"[API /api/messages] Filtering by stages: {stages}")

    # Apply job type filter (new_grad vs intern)
    if job_types and len(job_types) == 1:
        if 'new_grad' in job_types:
            query['new_grad'] = True
            print(f"[API /api/messages] Filtering by job type: new_grad")
        elif 'intern' in job_types:
            # Intern records either don't have new_grad field or have it set to false
            query['$or'] = [{'new_grad': False}, {'new_grad': {'$exists': False}}]
            print(f"[API /api/messages] Filtering by job type: intern")

    # Apply date filters
    if start or end:
        query['timestamp'] = {}
        if start:
            query['timestamp']['$gte'] = start.isoformat()
        if end:
            # Make end date inclusive by adding 1 day and using $lt
            end_inclusive = end + timedelta(days=1)
            query['timestamp']['$lt'] = end_inclusive.isoformat()
        print(f"[API /api/messages] Date filter applied: start={start}, end={end}")

    print(f"[API /api/messages] MongoDB query: {query}")

    # Query MongoDB
    cursor = collection.find(query, {"_id": 0}).sort("timestamp", -1)
    results = list(cursor)

    print(f"[API /api/messages] Retrieved {len(results)} messages from MongoDB")

    # Apply postprocessing: add missing stages
    print(f"[API /api/messages] Applying backfilling to messages...")
    # augmented_results = fill_missing_stages(results)

    # backfilled_count = len(augmented_results) - len(results)
    # print(f"[API /api/messages] Backfilling complete. Added {backfilled_count} auto-generated stages. Total messages: {len(augmented_results)}")

    return jsonify({'items': results, 'total': len(results)})


@app.route('/api/funnel')
def api_funnel():
    """Return stage counts for funnel chart."""
    start = parse_date(request.args.get('start'))
    end = parse_date(request.args.get('end'))
    companies = [c for c in (request.args.get('companies') or '').split(',') if c]
    job_types = [j for j in (request.args.get('job_types') or '').split(',') if j]

    query = {"spam": False, "stage": {"$ne": "App"}}

    if companies:
        query['company'] = {'$in': companies}

    # Apply job type filter
    if job_types and len(job_types) == 1:
        if 'new_grad' in job_types:
            query['new_grad'] = True
        elif 'intern' in job_types:
            query['$or'] = [{'new_grad': False}, {'new_grad': {'$exists': False}}]

    if start or end:
        query['timestamp'] = {}
        if start:
            query['timestamp']['$gte'] = start.isoformat()
        if end:
            # Make end date inclusive by adding 1 day and using $lt
            end_inclusive = end + timedelta(days=1)
            query['timestamp']['$lt'] = end_inclusive.isoformat()

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
            # Make end date inclusive by adding 1 day and using $lt
            end_inclusive = end + timedelta(days=1)
            query['timestamp']['$lt'] = end_inclusive.isoformat()

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

    # Track user journeys per company (key: company|author -> set of stages)
    user_stages = {}
    for key, msgs in applications.items():
        stages_seen = {msg['stage'] for msg in msgs if msg.get('stage')}
        user_stages[key] = stages_seen

    # Calculate stage-to-stage conversions (skip ...→Reject)
    for company in top_company_names:
        conv_matrix[company] = {}
        # Get all users for this company
        company_users = [k for k in user_stages.keys() if k.startswith(f"{company}|")]

        for i in range(len(STAGE_ORDER) - 1):
            from_stage = STAGE_ORDER[i]
            to_stage = STAGE_ORDER[i + 1]

            if to_stage.lower() == "reject":
                continue

            # Count users who had from_stage
            from_count = sum(1 for k in company_users if from_stage in user_stages[k])

            # Count users who had BOTH from_stage AND to_stage (actual progression)
            to_count = sum(1 for k in company_users
                          if from_stage in user_stages[k] and to_stage in user_stages[k])

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
            # Make end date inclusive by adding 1 day and using $lt
            end_inclusive = end + timedelta(days=1)
            query['timestamp']['$lt'] = end_inclusive.isoformat()

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
    """
    Return filtered company suggestions (with counts),
    respecting date range and job type filters, but ignoring currently selected companies.
    """
    search_term = (request.args.get('q') or request.args.get('search') or '').strip().lower()
    start = parse_date(request.args.get('start'))
    end = parse_date(request.args.get('end'))
    job_types = [j for j in (request.args.get('job_types') or '').split(',') if j]

    match_query = {"spam": False, "stage": {"$ne": "App"}}

    # Apply date filters
    if start or end:
        match_query["timestamp"] = {}
        if start:
            match_query["timestamp"]["$gte"] = start.isoformat()
        if end:
            end_inclusive = end + timedelta(days=1)
            match_query["timestamp"]["$lt"] = end_inclusive.isoformat()

    # Apply job type filter
    if job_types and len(job_types) == 1:
        if "new_grad" in job_types:
            match_query["new_grad"] = True
        elif "intern" in job_types:
            match_query["$or"] = [{"new_grad": False}, {"new_grad": {"$exists": False}}]

    # Mongo aggregation (efficient count)
    pipeline = [
        {"$match": match_query},
        {"$group": {"_id": "$company", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]

    results = list(collection.aggregate(pipeline))

    companies = []
    for r in results:
        name = (r.get("_id") or "").strip()
        if not name:
            continue
        if search_term and search_term not in name.lower():
            continue
        companies.append({"name": name, "count": r.get("count", 0)})

    companies.sort(key=lambda x: x["count"], reverse=True)
    return jsonify({"companies": companies, "total": len(companies)})



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
            # Make end date inclusive by adding 1 day and using $lt
            end_inclusive = end + timedelta(days=1)
            query['timestamp']['$lt'] = end_inclusive.isoformat()

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
    # Track user journeys per company (key: company|author -> set of stages)
    user_stages = {}
    for key, msgs in applications.items():
        stages_seen = {msg['stage'] for msg in msgs if msg.get('stage')}
        user_stages[key] = stages_seen

    conv_matrix = {}
    for company in top_company_names:
        conv_matrix[company] = {}
        # Get all users for this company
        company_users = [k for k in user_stages.keys() if k.startswith(f"{company}|")]

        for i in range(len(STAGE_ORDER) - 1):
            from_stage = STAGE_ORDER[i]
            to_stage = STAGE_ORDER[i + 1]

            if to_stage.lower() == "reject":
                continue

            # Count users who had from_stage
            from_count = sum(1 for k in company_users if from_stage in user_stages[k])

            # Count users who had BOTH from_stage AND to_stage (actual progression)
            to_count = sum(1 for k in company_users
                          if from_stage in user_stages[k] and to_stage in user_stages[k])

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


@app.route('/api/session/start', methods=['POST'])
def session_start():
    """Register a new active session."""
    data = request.get_json() or {}
    session_id = data.get('session_id')

    if not session_id:
        return jsonify({'error': 'session_id required'}), 400

    now = datetime.utcnow()
    sessions_collection.update_one(
        {'session_id': session_id},
        {
            '$set': {
                'session_id': session_id,
                'last_heartbeat': now,
                'created_at': now
            }
        },
        upsert=True
    )

    return jsonify({'success': True})


@app.route('/api/session/heartbeat', methods=['POST'])
def session_heartbeat():
    """Update session heartbeat to keep it active."""
    data = request.get_json() or {}
    session_id = data.get('session_id')

    if not session_id:
        return jsonify({'error': 'session_id required'}), 400

    now = datetime.utcnow()
    result = sessions_collection.update_one(
        {'session_id': session_id},
        {'$set': {'last_heartbeat': now}}
    )

    return jsonify({'success': result.modified_count > 0 or result.upserted_id is not None})


@app.route('/api/viewers/count')
def viewers_count():
    """Return count of active viewers (sessions active within last 5 minutes)."""
    cutoff_time = datetime.utcnow()
    # Subtract 5 minutes (300 seconds)
    from datetime import timedelta
    cutoff_time = cutoff_time - timedelta(minutes=5)

    # Count sessions with heartbeat within last 5 minutes
    count = sessions_collection.count_documents({
        'last_heartbeat': {'$gte': cutoff_time}
    })

    return jsonify({'count': count})


@app.route('/api/feedback', methods=['POST'])
def submit_feedback():
    """Save user feedback to database."""
    data = request.get_json() or {}

    feedback_text = data.get('feedback', '').strip()
    email = (data.get('email') or '').strip()
    rating = data.get('rating')

    if not feedback_text:
        return jsonify({'error': 'Feedback text is required'}), 400

    feedback_doc = {
        'feedback': feedback_text,
        'email': email if email else None,
        'rating': rating if rating else None,
        'timestamp': datetime.utcnow(),
        'session_id': data.get('session_id')
    }

    result = feedback_collection.insert_one(feedback_doc)

    return jsonify({
        'success': True,
        'feedback_id': str(result.inserted_id)
    })


@app.route('/api/submit', methods=['POST'])
def submit_data():
    """Accept new user submissions for interview process updates."""
    data = request.get_json() or {}

    # Extract fields
    username = (data.get('username') or '').strip()
    company = (data.get('company') or '').strip()
    stage = (data.get('stage') or '').strip()
    position_type = (data.get('position_type') or '').strip()
    submission_date = data.get('date')

    # Validate required fields
    if not all([username, company, stage, position_type, submission_date]):
        return jsonify({'error': 'All fields are required'}), 400

    # Validate stage
    if stage not in STAGE_ORDER:
        return jsonify({'error': 'Invalid stage'}), 400

    # Validate position type
    if position_type not in ['new_grad', 'intern']:
        return jsonify({'error': 'Invalid position type'}), 400

    # Validate date (must be after Oct 7, 2025)
    try:
        submit_dt = datetime.fromisoformat(submission_date)
        cutoff_date = datetime(2025, 10, 27)
        if submit_dt < cutoff_date:
            return jsonify({'error': 'Date must be after October 27, 2025'}), 400
        # Convert date to ISO format datetime string with time (noon UTC)
        timestamp_str = datetime.combine(submit_dt.date(), datetime.min.time().replace(hour=12)).isoformat()
    except (ValueError, TypeError):
        return jsonify({'error': 'Invalid date format'}), 400

    # Check for existing submissions from this user for this company and position type
    # Intern and new_grad are treated as separate journeys
    existing_query = {
        'author': username,
        'company': company,
        'new_grad': position_type == 'new_grad',
        'spam': False
    }

    existing_submissions = list(collection.find(existing_query, {"stage": 1, "timestamp": 1, "_id": 0}))

    if existing_submissions:
        existing_stages = {sub.get('stage') for sub in existing_submissions if sub.get('stage')}

        # Check if they already submitted this exact stage
        if stage in existing_stages:
            return jsonify({'error': f'You have already submitted the {stage} stage for {company} ({position_type})'}), 400

        # Check if they've submitted a later stage (prevent going backwards)
        stage_idx = STAGE_ORDER.index(stage)
        for existing_stage in existing_stages:
            if existing_stage in STAGE_ORDER:
                existing_idx = STAGE_ORDER.index(existing_stage)
                # Allow submitting later stages, but not earlier ones (except Reject can come anytime)
                if stage != 'Reject' and existing_idx > stage_idx:
                    return jsonify({
                        'error': f'You have already submitted {existing_stage} for {company}. Cannot submit earlier stage {stage}.'
                    }), 400

    # Create submission document
    submission_doc = {
        'msg_id': f'submission_{username}_{company}_{stage}_{int(datetime.utcnow().timestamp())}',
        'text': f'{stage} update for {company} (submitted via dashboard)',
        'timestamp': timestamp_str,
        'author': username,
        'company': company,
        'stage': stage,
        'new_grad': position_type == 'new_grad',
        'spam': False,
        'submitted_at': datetime.utcnow()
    }

    # Insert into database
    try:
        result = collection.insert_one(submission_doc)
        return jsonify({
            'success': True,
            'submission_id': str(result.inserted_id)
        })
    except Exception as e:
        return jsonify({'error': f'Database error: {str(e)}'}), 500


@app.route('/api/top-oa-companies')
def top_oa_companies():
    """Get top companies sending out OAs this week."""
    # Get job_types filter from request
    job_types = [j for j in (request.args.get('job_types') or '').split(',') if j]

    # Get current date and calculate one week ago
    now = datetime.utcnow()
    one_week_ago = now - timedelta(days=7)

    # Build the match query
    match_query = {
        'stage': 'OA',
        'timestamp': {
            '$gte': one_week_ago.isoformat(),
            '$lte': now.isoformat()
        },
        'spam': {'$ne': True}
    }

    # Apply job type filter
    if job_types and len(job_types) == 1:
        if 'new_grad' in job_types:
            match_query['new_grad'] = True
        elif 'intern' in job_types:
            match_query['$or'] = [{'new_grad': False}, {'new_grad': {'$exists': False}}]

    # Query for OA stage entries in the last week
    pipeline = [
        {'$match': match_query},
        {
            '$group': {
                '_id': '$company',
                'count': {'$sum': 1}
            }
        },
        {
            '$sort': {'count': -1}
        },
        {
            '$limit': 10
        }
    ]

    results = list(collection.aggregate(pipeline))

    # Format the results
    top_companies = [
        {'company': item['_id'], 'count': item['count']}
        for item in results
    ]

    return jsonify({'companies': top_companies})


@app.route('/api/top-offer-companies')
def top_offer_companies():
    """Get top companies sending out offers this week."""
    # Get job_types filter from request
    job_types = [j for j in (request.args.get('job_types') or '').split(',') if j]

    # Get current date and calculate one week ago
    now = datetime.utcnow()
    one_week_ago = now - timedelta(days=7)

    # Build the match query
    match_query = {
        'stage': 'Offer',
        'timestamp': {
            '$gte': one_week_ago.isoformat(),
            '$lte': now.isoformat()
        },
        'spam': {'$ne': True}
    }

    # Apply job type filter
    if job_types and len(job_types) == 1:
        if 'new_grad' in job_types:
            match_query['new_grad'] = True
        elif 'intern' in job_types:
            match_query['$or'] = [{'new_grad': False}, {'new_grad': {'$exists': False}}]

    # Query for Offer stage entries in the last week
    pipeline = [
        {'$match': match_query},
        {
            '$group': {
                '_id': '$company',
                'count': {'$sum': 1}
            }
        },
        {
            '$sort': {'count': -1}
        },
        {
            '$limit': 10
        }
    ]

    results = list(collection.aggregate(pipeline))

    # Format the results
    top_companies = [
        {'company': item['_id'], 'count': item['count']}
        for item in results
    ]

    return jsonify({'companies': top_companies})

from datetime import datetime, timedelta

def fill_missing_dates(data):
    """Fills in missing days with count=0 to avoid straight line jumps."""
    if not data:
        return []
    fmt = "%Y-%m-%d"
    filled = []
    start = datetime.strptime(data[0]['date'], fmt)
    end = datetime.strptime(data[-1]['date'], fmt)
    existing = {d['date']: d['count'] for d in data}
    cur = start
    while cur <= end:
        date_str = cur.strftime(fmt)
        filled.append({'date': date_str, 'count': existing.get(date_str, 0)})
        cur += timedelta(days=1)
    return filled

@app.route('/api/hiring-trends')
def hiring_trends():
    """Get daily hiring activity (OA + Offer counts) for the past 6 months, grouped by top 5 companies."""
    # Get filters from request
    job_types = [j for j in (request.args.get('job_types') or '').split(',') if j]
    company_filter = request.args.get('company', '').strip()

    # Get current date and calculate 6 months ago
    now = datetime.utcnow()
    now = now - timedelta(days=2)
    six_months_ago = now - timedelta(days=180)

    # Build the match query
    match_query = {
        'stage': {'$in': ['OA', 'Offer']},
        'timestamp': {
            '$gte': six_months_ago.isoformat(),
            '$lte': now.isoformat()
        },
        'spam': {'$ne': True}
    }

    # Apply job type filter
    if job_types and len(job_types) == 1:
        if 'new_grad' in job_types:
            match_query['new_grad'] = True
        elif 'intern' in job_types:
            match_query['$or'] = [{'new_grad': False}, {'new_grad': {'$exists': False}}]

    # If company filter is applied, only get data for that company
    if company_filter:
        match_query['company'] = company_filter

        # Aggregate by date for single company
        pipeline = [
            {'$match': match_query},
            {
                '$addFields': {
                    'timestamp_date': {
                        '$dateFromString': {
                            'dateString': '$timestamp',
                            'onError': None
                        }
                    }
                }
            },
            {'$match': {'timestamp_date': {'$ne': None}}},
            {
                '$group': {
                    '_id': {
                        '$dateToString': {
                            'format': '%Y-%m-%d',
                            'date': '$timestamp_date'
                        }
                    },
                    'count': {'$sum': 1}
                }
            },
            {'$sort': {'_id': 1}}
        ]

        results = list(collection.aggregate(pipeline))

        # Apply 7-day moving average
        def apply_moving_avg(data, window=7):
            if len(data) < window:
                return data
            smoothed = []
            for i in range(len(data)):
                start = max(0, i - window // 2)
                end = min(len(data), i + window // 2 + 1)
                avg = sum(d['count'] for d in data[start:end]) / (end - start)
                smoothed.append({'date': data[i]['date'], 'count': round(avg, 2)})
            return smoothed

        daily_data = [{'date': item['_id'], 'count': item['count']} for item in results]
        daily_data = fill_missing_dates(daily_data)
        smoothed_data = apply_moving_avg(daily_data)

        return jsonify({
            'companies': {company_filter: smoothed_data}
        })

    # Helper function for moving average
    def apply_moving_avg(data, window=7):
        if len(data) < window:
            return data
        smoothed = []
        for i in range(len(data)):
            start = max(0, i - window // 2)
            end = min(len(data), i + window // 2 + 1)
            avg = sum(d['count'] for d in data[start:end]) / (end - start)
            smoothed.append({'date': data[i]['date'], 'count': round(avg, 2)})
        return smoothed

    # Get global average (all companies combined)
    global_pipeline = [
        {'$match': match_query},
        {
            '$addFields': {
                'timestamp_date': {
                    '$dateFromString': {
                        'dateString': '$timestamp',
                        'onError': None
                    }
                }
            }
        },
        {'$match': {'timestamp_date': {'$ne': None}}},
        {
            '$group': {
                '_id': {
                    '$dateToString': {
                        'format': '%Y-%m-%d',
                        'date': '$timestamp_date'
                    }
                },
                'count': {'$sum': 1}
            }
        },
        {'$sort': {'_id': 1}}
    ]

    global_results = list(collection.aggregate(global_pipeline))
    global_daily_data = [{'date': item['_id'], 'count': item['count']} for item in global_results]
    global_smoothed = apply_moving_avg(global_daily_data)

    # Get top 5 companies by total activity
    top_companies_pipeline = [
        {'$match': match_query},
        {
            '$group': {
                '_id': '$company',
                'total': {'$sum': 1}
            }
        },
        {'$sort': {'total': -1}},
        {'$limit': 5}
    ]

    top_companies = list(collection.aggregate(top_companies_pipeline))
    top_company_names = [item['_id'] for item in top_companies if item['_id']]

    if not top_company_names and not global_smoothed:
        return jsonify({'companies': {}})

    # Get daily data for each top company
    company_data = {}

    # Add global average first
    company_data['Global Average'] = global_smoothed

    for company in top_company_names:
        company_match = match_query.copy()
        company_match['company'] = company

        pipeline = [
            {'$match': company_match},
            {
                '$addFields': {
                    'timestamp_date': {
                        '$dateFromString': {
                            'dateString': '$timestamp',
                            'onError': None
                        }
                    }
                }
            },
            {'$match': {'timestamp_date': {'$ne': None}}},
            {
                '$group': {
                    '_id': {
                        '$dateToString': {
                            'format': '%Y-%m-%d',
                            'date': '$timestamp_date'
                        }
                    },
                    'count': {'$sum': 1}
                }
            },
            {'$sort': {'_id': 1}}
        ]

        results = list(collection.aggregate(pipeline))
        daily_data = [{'date': item['_id'], 'count': item['count']} for item in results]
        company_data[company] = apply_moving_avg(daily_data)

    return jsonify({'companies': company_data})


# ---- Entry ----
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 3050))
    app.run(host='0.0.0.0', port=port, debug=False)
