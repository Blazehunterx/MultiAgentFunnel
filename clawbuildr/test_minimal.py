import sqlite3, json, os, time, shutil
from playwright.sync_api import sync_playwright

FIREFOX_DB = r"C:\Users\marvi\AppData\Roaming\Mozilla\Firefox\Profiles\h1vl3oun.default-release\cookies.sqlite"

# Extract just the 4 critical session cookies
tmp_db = os.path.join(os.environ["TEMP"], "li_test.sqlite")
shutil.copy2(FIREFOX_DB, tmp_db)
db = sqlite3.connect(f"file:{tmp_db}?mode=ro", uri=True)
db.row_factory = sqlite3.Row
rows = db.execute("""
    SELECT name, value, host, path, expiry, isSecure, isHttpOnly
    FROM moz_cookies WHERE name IN ('li_at','liap','bscookie','JSESSIONID')
""").fetchall()
db.close()
os.remove(tmp_db)

print("Raw session cookies:")
cookies = []
for r in rows:
    domain = r["host"]
    if not domain.startswith("."):
        domain = "." + domain.lstrip(".")
    exp = r["expiry"] or 0
    if exp > 10000000000:
        exp = exp // 1000
    c = {
        "name": r["name"], "value": r["value"], "domain": domain,
        "path": r["path"] or "/", "secure": bool(r["isSecure"]),
        "httpOnly": bool(r["isHttpOnly"]),
        "sameSite": "Lax",
        "expires": int(exp) if exp > 0 else -1,
    }
    cookies.append(c)
    print(f"  {c['name']}: domain={c['domain']} secure={c['secure']} httpOnly={c['httpOnly']} expires={c['expires']}")

# Dedup
dedup = {}
for c in cookies:
    k = (c["name"], c["domain"])
    if k not in dedup or c["expires"] > dedup[k]["expires"]:
        dedup[k] = c
cookies = list(dedup.values())
print(f"\nDeduped: {len(cookies)} cookies")

# Test
with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=False)
    ctx = browser.new_context(viewport={"width": 1280, "height": 900})
    
    print("\nAdding cookies...")
    ctx.add_cookies(cookies)
    
    # Verify
    stored = ctx.cookies("https://www.linkedin.com")
    print(f"Stored: {len(stored)} cookies")
    for s in stored:
        if s["name"] in ["li_at", "liap"]:
            print(f"  {s['name']}: domain={s['domain']} httpOnly={s['httpOnly']}")
    
    page = ctx.new_page()
    page.goto("https://www.linkedin.com/feed/", wait_until="domcontentloaded", timeout=30000)
    time.sleep(5)
    print(f"\nURL: {page.url}")
    print(f"Title: {page.title()}")
    
    browser.close()
