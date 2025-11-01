# =============================================
# File: scripts/update_company_names.py
# Purpose: Normalize company names in MongoDB (JobStats)
# =============================================
from pymongo import MongoClient
from datetime import datetime
import os


client = MongoClient(uri)
db = client["JobStats"]
collection = db["interview_processes_backfilled"]

# --- Unified Canonical Mapping ---

CANON = {
    "1Password": ["1Pass"],
    "2K Games": ["2K", "2k Games"],
    "AT&T": ["ATT", "At&T", "At&t", "Att"],
    "AWS": ["Aws", "Amazon Web Services", "Aws Ml"],
    "Abnormal AI": ["Abnormal.ai"],
    "Activision Blizzard": ["Activision"],
    "Adobe": ["Adboe"],
    "Akuna Capital": ["Akuna", "Akuna Trading"],
    "AMD": ["Amd"],
    "Amazon": [
        "Amazon Prime", "Amazon Robotics", "Amazon Science", "Amazon Warehouse",
        "ZON", "Zon", "Zon Annapurna"
    ],
    "American Express": ["Amex"],
    "AppFolio": ["Appfolio"],
    "AppLovin": ["Applovin"],
    "Arrowstreet Capital": ["Arrowstreet", "Arrow Street", "Arrowst"],
    "Aurora Innovation": ["Aurora"],
    "BAE Systems": ["BAE"],
    "BAM": ["Bam"],
    "Bank of America": ["Bank Of America", "Boa", "BofA", "Bofa"],
    "Bank of Ireland": ["Bank Of Ireland"],
    "BCG": ["Bcg", "Bcg X"],
    "BB": ["Bb"],
    "BBG": ["Bbg"],
    "Belvedere Trading": ["Belvedere"],
    "Bentley Systems": ["Bentley", "Bentley System"],
    "BitGo": ["Bitgo"],
    "Black Edge": ["Blackedge"],
    "BlackRock": ["Black Rock", "Blackrock"],
    "Blizzard Entertainment": ["Blizzard"],
    "Blue Yonder": ["Blueyonder"],
    "BNSF Railway": ["BNSF", "Bnsf", "BNSF Rail", "BNSF Railways"],
    "Booz Allen Hamilton": ["Booz", "Booz Allen"],
    "Bosch": ["Bosch Research"],
    "ByteDance": ["Bytedance"],
    "C3.ai": ["C3", "C3.AI", "C3.Ai"],
    "CBOE": ["Cboe"],
    "CBRE": ["Cb"],
    "Character AI": ["Character.ai"],
    "Chick Fil A": ["Chik Fil A"],
    "Citadel": ["Citadel Securities"],
    "CITI": ["Cit", "CitSec", "Citsec"],
    "CLEAR": ["Clear"],
    "Co-operators": ["Co-Operators"],
    "CoStar Group": ["Costar", "Costar Group", "CoStar"],
    "CTC": ["Ctc"],
    "CZI": ["Czi"],
    "CVS": ["Cvs"],
    "DE Shaw": ["Deshaw"],
    "DESRES": ["Desres"],
    "DL Trading": ["Dl Trading"],
    "Deloitte": ["Deloitte Consulting"],
    "Dick's Sport": ["Dick's", "Dicks", "Dicks Sporting"],
    "DoorDash": ["Doordash"],
    "DraftKings": ["Draftkings", "Draft Kings", "Draftking"],
    "DRW": ["Drw"],
    "Dsm Firmenich": ["Dsm-Firmenich"],
    "EA": ["Electronic Arts"],
    "Epic Games": ["Epic Game", "Epic"],
    "EvenUp": ["Evenup"],
    "ExtraHop": ["Extrahop"],
    "FedEx": ["Fedex"],
    "Five Rings": ["Five Gys"],
    "Flow Traders": ["Flow Trader"],
    "Future Force": ["Futureforce"],
    "GE": ["GE Aerospace", "GE Appliances"],
    "GitHub": ["Github"],
    "G-Research": ["GResearch", "Gresearch"],
    "Goldman Sachs": ["Gs"],
    "Google": ["Google AI Catalyst", "Google DeepMind", "Google AI Catalyst Program"],
    "Greylock": ["Greylock Techfair", "Grelock Techfair"],
    "GTS": ["Gts"],
    "Harvey AI": ["Harvey Ai"],
    "Headland": ["Headlands"],
    "HPE": ["Hpe"],
    "HPR": ["Hpr"],
    "HubSpot": ["Hubspot", "Hubbob"],
    "Hudson River Trading": ["HRT"],
    "IBM": ["Ibm"],
    "IMC Trading": ["IMC"],
    "Interactive Brokers": ["Interactive Broker"],
    "InterSystems": ["Intersystems"],
    "Intuitive": ["Intutive"],
    "IXL": ["Ixl"],
    "Jane Street": ["Janestreet"],
    "JPMorgan Chase": ["JP Morgan", "JPMC", "JPM", "Jpmorgan", "Jpm", "Jpmc", "JPMorganChase"],
    "Johnson & Johnson": ["J&J", "Jnj", "Johnson And Johnson", "Johnson and Johnson"],
    "JS": ["Js"],
    "Jump Trading": ["Jump"],
    "KKR": ["Kkr"],
    "Kohl’s": ["Kohls", "Kohl\u2019s"],
    "KPMG": ["Kpmg"],
    "KP Fellow": ["Kp Fellow"],
    "L3Harris Technologies": ["L3Harris"],
    "LinkedIn": ["Linkedin"],
    "LSEG": ["Lseg"],
    "Lowe’s": ["Lowes"],
    "Macy’s": ["Macys"],
    "McDonald’s": ["McDonalds", "Mcdonalds", "Mcdonald's", "Mcds", "Mcodnalds"],
    "Merge API": ["Merge Api"],
    "Meta": ["Facebook", "Meta Reality Labs"],
    "Microsoft": ["Micro", "Microstrategy", "MicroStrategy"],
    "Millennium": ["Millenium", "Milennium", "Millennium Management"],
    "MindGeek": ["Mindgeek"],
    "MongoDB": ["Mongodb", "Mangodb"],
    "Morgan Stanley": ["Ms"],
    "NASA": ["Nasa"],
    "NBCUniversal": ["NBCU", "NBC Universal"],
    "NCR Voyix": ["Ncr Voyix"],
    "Neo Scholar": ["Neo Scholars"],
    "NetApp": ["Netapp"],
    "Nexthop AI": ["Nexthop Ai"],
    "NimbleRx": ["Nimblerx"],
    "Northrop Grumman": ["Northslope", "General Dynamic", "General Dynamics"],
    "NVIDIA": ["Nvidia"],
    "OC&C": ["Oc&c"],
    "OKC": ["Okc"],
    "OKC Thunder": ["Okc Thunnder"],
    "OMC": ["Omc"],
    "OpenAI": ["Open Ai", "Open AI", "Openai", "Oai"],
    "Optiver": ["Optiver Trading"],
    "Palantir": ["Pltr"],
    "PANW": ["Panw"],
    "PayPal": ["Paypal"],
    "PDT Partners": ["PJT Partners"],
    "PNC": ["Pnc"],
    "PrizePicks": ["Prizepick", "Prizepicks"],
    "Procter & Gamble": ["Procter And Gamble", "Proctor And Gamble", "Procter and Gamble", "P&G"],
    "PwC": ["Pwc", "Pwc Cyber"],
    "Random Startup #1": ["Random Startup #2"],
    "Riot Games": ["Riot", "Riot Game"],
    "Robhinhood": ["Robinhood"],
    "RTX": ["Rtx"],
    "Salesforce": ["Sales Force"],
    "Samara": ["Samsara"],
    "Scale AI": ["Scale Ai", "Scaleai", "Scale.AI", "ScaleAI"],
    "SeatGeek": ["Seatgeek"],
    "ServiceNow": ["Servicenow"],
    "Series A Startup": ["SeriesA Startup"],
    "SIG": ["Sig"],
    "SMBC": ["Smbc"],
    "Snowflake": ["Snow"],
    "SpaceX": ["Spacex", "Spacex Starlink"],
    "Stripe": ["Stirpe"],
    "T-Mobile": ["Tmobile"],
    "Tesla": ["Tesla Optimus"],
    "Thinking Machine": ["Thinking Machines"],
    "TikTok": ["Tik Tok", "Tiktok"],
    "TGS": ["Tgs"],
    "TTD": ["Ttd"],
    "Two Sigma": ["2 Sigma", "2Sig"],
    "UBS": ["Ubs"],
    "Uber": ["Uber Freight"],
    "UKG": ["Ukg"],
    "USAA": ["Usaa"],
    "Virtu Financial": ["Virtu", "Virtu Qt"],
    "Walleye Capital": ["Walleye", "Walleye Capitol", "Walleye Technology"],
    "Walmart": ["Walmart Global Tech"],
    "Waymo": ["Waymo Ml Infra"],
    "Wells Fargo": ["Wells", "Welssfargo", "Wells Fargo St. Louis"],
    "WF": ["Wf"],
    "Zeiss": ["Zeuiss"],
    "Zebra Technologies": ["Zebra", "Zebra Tech"],
    "Zocdoc": ["Zodoc"],
    "ZoomInfo": ["Zoominfo"],
    "ZS Associates": ["ZS"],
}


# --- Normalize Helper ---
def normalize(s):
    return s.strip().lower() if isinstance(s, str) else ""

# --- Build Reverse Lookup ---
reverse_lookup = {}
for canon, variants in CANON.items():
    for v in variants:
        reverse_lookup[normalize(v)] = canon
    reverse_lookup[normalize(canon)] = canon  # include itself

# --- Update Loop ---
updated_count = 0
bulk_updates = []

for doc in collection.find({}, {"_id": 1, "company": 1}):
    company = doc.get("company")
    if not company:
        continue
    normalized = normalize(company)
    if normalized in reverse_lookup:
        canon_name = reverse_lookup[normalized]
        if company != canon_name:
            bulk_updates.append(
                {
                    "filter": {"_id": doc["_id"]},
                    "update": {"$set": {"company": canon_name}}
                }
            )

# Execute updates in bulk
if bulk_updates:
    for op in bulk_updates:
        print("updating ", op)
        collection.update_one(op["filter"], op["update"])
        updated_count += 1

print(f"[{datetime.utcnow().isoformat()}] ✅ Normalization complete.")
print(f"Total updated: {updated_count}")
