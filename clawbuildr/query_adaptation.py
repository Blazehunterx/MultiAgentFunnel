import json
import os
import sqlite3
import random
import logging
from typing import List, Dict, Tuple, Any
from collections import defaultdict

logger = logging.getLogger("query_adaptation")

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "clawbuildr.db")

# Base queries organized by category
BASE_QUERIES = {
    "groothandel": [
        "groothandel B2B Nederland",
        "groothandel MKB Nederland",
        "wholesale bedrijf Nederland",
        "groothandel export",
        "distributiebedrijf Nederland",
        "importeur Nederland",
    ],
    "logistiek": [
        "logistiek dienstverlener Nederland",
        "transport bedrijf Nederland MKB",
        "opslag warehouse Nederland",
        "versnijding transport",
        "logistiek supply chain",
    ],
    "it_saas": [
        "IT consultancy Nederland MKB",
        "SaaS bedrijf Nederland",
        "software ontwikkeling Nederland",
        "ICT dienstverlener",
        "digitalisering bedrijf",
    ],
    "dienstverlening": [
        "dienstverlener B2B Nederland",
        "adviesbureau Nederland",
        "consultancy bedrijf",
        "zakelijke dienstverlening",
    ],
}


def _get_db():
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    return conn


def get_query_weights() -> Dict[str, float]:
    """Get current weights for each query based on past performance."""
    try:
        conn = _get_db()
        rows = conn.execute("""
            SELECT query_text, query_category,
                   COUNT(*) as total,
                   SUM(CASE WHEN icp_score >= 50 THEN 1 ELSE 0 END) as icp_pass,
                   SUM(CASE WHEN pipeline_stage IN ('RESEARCHED', 'DELIVERABILITY_VERIFIED', 'OPPORTUNITY_MAPPED', 'PRE_QUALIFIED', 'OUTREACH_DRAFTED') THEN 1 ELSE 0 END) as qualified,
                   SUM(CASE WHEN outcome = 'converted' THEN 1 ELSE 0 END) as converted
            FROM scraper_learning
            WHERE created_at > datetime('now', '-30 days')
            GROUP BY query_text
        """).fetchall()
        conn.close()

        weights = {}
        for row in rows:
            q = row["query_text"]
            total = max(row["total"], 1)
            icp_rate = row["icp_pass"] / total
            qual_rate = row["qualified"] / total
            conv_rate = row["converted"] / total

            # Weight formula: ICP pass rate (40%) + qualification rate (40%) + conversion (20%)
            weight = (icp_rate * 0.4) + (qual_rate * 0.4) + (conv_rate * 0.2)

            # Boost if we have data, penalize if too few samples
            if total < 5:
                weight = max(weight, 0.3)  # minimum weight for new queries

            weights[q] = weight

        return weights
    except Exception as e:
        logger.warning(f"Could not load query weights: {e}")
        return {}


def get_adapted_queries(tenant_id: str = "injexion", limit: int = 30) -> List[Tuple[str, str]]:
    """Get queries adapted by past performance. Returns list of (query, category)."""
    # Load user-configured queries from tenant_config
    try:
        conn = _get_db()
        row = conn.execute(
            "SELECT search_queries FROM tenant_config WHERE tenant_id = ?",
            (tenant_id,)
        ).fetchone()
        conn.close()
        if row and row["search_queries"]:
            user_queries = json.loads(row["search_queries"])
        else:
            user_queries = []
    except Exception:
        user_queries = []

    # Combine with base queries
    all_queries = []
    for category, queries in BASE_QUERIES.items():
        for q in queries:
            all_queries.append((q, category))

    # Add user queries
    for q in user_queries:
        if not any(q == aq[0] for aq in all_queries):
            all_queries.append((q, "user"))

    # Get weights
    weights = get_query_weights()

    # Calculate final weights
    weighted = []
    for query, category in all_queries:
        w = weights.get(query, 0.5)  # default weight for new queries
        weighted.append((query, category, w))

    # Sort by weight (descending) with some randomness
    weighted.sort(key=lambda x: x[2] + random.uniform(0, 0.2), reverse=True)

    return [(q, c) for q, c, w in weighted[:limit]]


def record_query_result(
    query_text: str,
    query_category: str,
    source: str,
    company_domain: str,
    company_industry: str,
    company_size: int = None,
    icp_score: float = 0,
    pipeline_stage: str = None,
    outcome: str = None,
):
    """Record the result of a query for learning."""
    try:
        conn = _get_db()
        conn.execute("""
            INSERT INTO scraper_learning
            (query_text, query_category, source, company_domain, company_industry,
             company_size, icp_score, pipeline_stage, outcome)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            query_text, query_category, source, company_domain,
            company_industry, company_size, icp_score, pipeline_stage, outcome
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"Could not record query result: {e}")


def generate_new_queries() -> List[Tuple[str, str]]:
    """Generate new query variations from successful patterns."""
    try:
        conn = _get_db()

        # Find top-performing query categories
        top_categories = conn.execute("""
            SELECT query_category, AVG(icp_score) as avg_score, COUNT(*) as cnt
            FROM scraper_learning
            WHERE icp_score >= 50
            AND created_at > datetime('now', '-7 days')
            GROUP BY query_category
            HAVING cnt >= 3
            ORDER BY avg_score DESC
            LIMIT 3
        """).fetchall()
        conn.close()

        new_queries = []
        for cat in top_categories:
            category = cat["query_category"]
            if category in BASE_QUERIES:
                # Generate variations of successful queries
                base = random.choice(BASE_QUERIES[category])
                variations = _generate_variations(base)
                for v in variations[:2]:  # max 2 per category
                    new_queries.append((v, category))

        return new_queries
    except Exception as e:
        logger.warning(f"Could not generate new queries: {e}")
        return []


def _generate_variations(base_query: str) -> List[str]:
    """Generate query variations from a base query."""
    modifiers = [
        "MKB", "Klein", "Groot", "Startup", "Scale-up",
        "Noord-Holland", "Zuid-Holland", "Brabant", "Gelderland", "Overijssel",
        "B2B", "Zakelijk", "Professioneel",
        "2024", "2025", "2026",
    ]

    expansions = {
        "groothandel": ["import", "export", "distributie", "groothandel"],
        "logistiek": ["transport", "opslag", "warehousing", "supply chain"],
        "IT": ["software", "ict", "digitalisering", "automatisering"],
        "consultancy": ["advies", "dienstverlening", "begeleiding"],
    }

    variations = []
    words = base_query.split()

    # Add a modifier
    modifier = random.choice(modifiers)
    if modifier not in base_query:
        variations.append(f"{base_query} {modifier}")

    # Replace a word with an expansion
    for word in words:
        if word.lower() in expansions:
            expansion = random.choice(expansions[word.lower()])
            new_query = base_query.replace(word, expansion)
            if new_query != base_query:
                variations.append(new_query)

    # Add geographic variation
    if not any(loc in base_query for loc in ["Noord", "Zuid", "Brabant", "Gelderland"]):
        geo = random.choice(["Noord-Holland", "Zuid-Holland", "Brabant"])
        variations.append(f"{base_query} {geo}")

    return list(set(variations))[:3]


def get_learning_stats() -> Dict[str, Any]:
    """Get statistics about query performance."""
    try:
        conn = _get_db()

        total = conn.execute("SELECT COUNT(*) as cnt FROM scraper_learning").fetchone()["cnt"]
        icp_pass = conn.execute(
            "SELECT COUNT(*) as cnt FROM scraper_learning WHERE icp_score >= 50"
        ).fetchone()["cnt"]
        qualified = conn.execute(
            "SELECT COUNT(*) as cnt FROM scraper_learning WHERE pipeline_stage IN ('RESEARCHED', 'DELIVERABILITY_VERIFIED', 'OPPORTUNITY_MAPPED', 'PRE_QUALIFIED')"
        ).fetchone()["cnt"]

        by_category = conn.execute("""
            SELECT query_category,
                   COUNT(*) as total,
                   AVG(icp_score) as avg_score,
                   SUM(CASE WHEN icp_score >= 50 THEN 1 ELSE 0 END) as icp_pass
            FROM scraper_learning
            GROUP BY query_category
            ORDER BY avg_score DESC
        """).fetchall()

        by_source = conn.execute("""
            SELECT source, COUNT(*) as cnt, AVG(icp_score) as avg_score
            FROM scraper_learning
            GROUP BY source
        """).fetchall()

        conn.close()

        return {
            "total_discovered": total,
            "icp_passed": icp_pass,
            "qualified": qualified,
            "icp_pass_rate": round(icp_pass / max(total, 1) * 100, 1),
            "by_category": [dict(r) for r in by_category],
            "by_source": [dict(r) for r in by_source],
        }
    except Exception as e:
        logger.warning(f"Could not get learning stats: {e}")
        return {"total_discovered": 0, "icp_passed": 0, "qualified": 0}
