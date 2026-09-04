import sqlite3
import json
import logging
import requests
import time
import os
import csv
from urllib.parse import urlparse

# CONFIGURATION
DB_PATH = r"C:\Users\marvi\odysseus\data\clawbuildr.db"
API_URL = "http://localhost:8000/api/agent-zero/ingest"
DRY_RUN = False

CONFIG = {
    "target_industries": ["groothandel", "transport", "logistiek"],
    "min_employees": 10,
    "max_employees": 50,
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

class DomainNormalizer:
    @staticmethod
    def normalize(raw_domain):
        if not raw_domain:
            return ""
        d = raw_domain.strip().lower()
        if not d.startswith("http"):
            d = "http://" + d
        parsed = urlparse(d)
        netloc = parsed.netloc
        if netloc.startswith("www."):
            netloc = netloc[4:]
        return netloc.split('/')[0]

class CsvSourceAdapter:
    def __init__(self, filepath):
        self.filepath = filepath
        
    def discover(self):
        leads = []
        if not os.path.exists(self.filepath):
            # Create mock CSV if missing
            logging.warning(f"Source file {self.filepath} not found. Creating mock file.")
            with open(self.filepath, 'w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=["company", "domain", "industry", "employees", "b2b", "first_name", "last_name"])
                writer.writeheader()
                writer.writerow({"company": "Test Wholesale BV", "domain": "testwholesale.nl", "industry": "groothandel", "employees": "25", "b2b": "high", "first_name": "Jan", "last_name": "Jansen"})
                writer.writerow({"company": "Dutch Transport", "domain": "dutchtransport.nl", "industry": "transport", "employees": "15", "b2b": "high", "first_name": "Peter", "last_name": "De Vries"})
                writer.writerow({"company": "B2C Store", "domain": "b2cstore.nl", "industry": "retail", "employees": "5", "b2b": "low", "first_name": "Kees", "last_name": "Boer"})
            
        with open(self.filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                leads.append({
                    "company_name": row.get("company", ""),
                    "domain": row.get("domain", ""),
                    "industry": row.get("industry", ""),
                    "employee_range": int(row.get("employees", 0)) if row.get("employees", "").isdigit() else 0,
                    "b2b_confidence": row.get("b2b", "low"),
                    "first_name": row.get("first_name", "Unknown"),
                    "last_name": row.get("last_name", "Unknown"),
                    "source": "csv_export"
                })
        return leads

class Deduplicator:
    def __init__(self, db_path):
        self.db_path = db_path
        
    def is_duplicate(self, normalized_domain):
        if not normalized_domain:
            return False
            
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT 1 FROM companies WHERE domain = ?", (normalized_domain,))
            if cursor.fetchone():
                return True
        except sqlite3.OperationalError as e:
            logging.warning(f"DB Error checking duplicates: {e}")
        finally:
            conn.close()
            
        return False

class HardMetricQualifier:
    def qualify(self, lead):
        reasons = []
        qualified = True
        
        industry = lead.get('industry', '').lower()
        if industry in CONFIG['target_industries']:
            reasons.append(f"target_industry_{industry}")
        else:
            qualified = False
            reasons.append(f"unrelated_industry_{industry}")
            
        emp_count = lead.get('employee_range', 0)
        if CONFIG['min_employees'] <= emp_count <= CONFIG['max_employees']:
            reasons.append(f"employees_{emp_count}")
        else:
            qualified = False
            reasons.append(f"employee_count_out_of_range_{emp_count}")
            
        if lead.get('b2b_confidence') == "high":
            reasons.append("b2b_company")
        elif lead.get('b2b_confidence') == "low" and industry not in CONFIG['target_industries']:
            qualified = False
            reasons.append("b2c_signals_detected")
            
        return qualified, reasons

class PipelineClient:
    def __init__(self, url, dry_run=True):
        self.url = url
        self.dry_run = dry_run
        
    def handoff(self, lead, reasons):
        domain = DomainNormalizer.normalize(lead['domain'])
        payload = {
            "first_name": lead.get("first_name", "Unknown"),
            "last_name": lead.get("last_name", "Unknown"),
            "company_name": lead["company_name"],
            "domain": domain,
            "email": f"info@{domain}",
            "industry": lead.get("industry"),
            "role": "Founder"
        }
        
        if self.dry_run:
            logging.info(f"[DRY RUN] Would handoff: {payload['company_name']}")
            return True, "dry_run"
            
        try:
            logging.info(f"Submitting {payload['company_name']} to Odysseus Dashboard (Timeout: 10s)...")
            start_time = time.time()
            response = requests.post(self.url, json=payload, timeout=60)
            elapsed = time.time() - start_time
            
            if response.status_code in [200, 201]:
                logging.info(f"HANDOFF SUCCESS: {payload['company_name']} in {elapsed:.1f}s - {response.json()}")
                return True, elapsed
            else:
                logging.error(f"HANDOFF FAILED: {payload['company_name']} - HTTP {response.status_code} - {response.text}")
                return False, response.text
        except Exception as e:
            logging.error(f"API Error during handoff for {payload['company_name']}: {str(e)}")
            return False, str(e)

def run():
    logging.info("Initializing Agent Zero Pipeline v3 (Odysseus Dashboard Integration)...")
    
    adapter = CsvSourceAdapter("apollo_export_v3.csv")
    normalizer = DomainNormalizer()
    qualifier = HardMetricQualifier()
    deduplicator = Deduplicator(DB_PATH)
    client = PipelineClient(API_URL, dry_run=DRY_RUN)
    
    raw_leads = adapter.discover()
    
    stats = {
        "discovered": len(raw_leads),
        "rejected_industry": 0,
        "rejected_size": 0,
        "rejected_retail": 0,
        "duplicates": 0,
        "qualified": 0,
        "handoff_success": 0,
        "handoff_failed": 0,
    }
    
    for lead in raw_leads:
        norm_domain = normalizer.normalize(lead['domain'])
        
        # 1. Qualify
        qualified, reasons = qualifier.qualify(lead)
        if not qualified:
            if any("industry" in r for r in reasons):
                stats["rejected_industry"] += 1
            elif any("employee" in r for r in reasons):
                stats["rejected_size"] += 1
            else:
                stats["rejected_retail"] += 1
            continue
            
        # 2. Deduplicate against new companies table
        if deduplicator.is_duplicate(norm_domain):
            stats["duplicates"] += 1
            continue
            
        stats["qualified"] += 1
        
        # 3. Handoff
        success, info = client.handoff(lead, reasons)
        if success:
            stats["handoff_success"] += 1
        else:
            stats["handoff_failed"] += 1
            
    logging.info("--- PILOT RESULTS ---")
    for k, v in stats.items():
        logging.info(f"{k} : {v}")

if __name__ == "__main__":
    run()
