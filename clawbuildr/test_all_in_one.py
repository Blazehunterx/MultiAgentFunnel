"""All-in-one: extract cookies + test in one shot to avoid stale reads."""
import sqlite3, json, os, time, shutil
from playwright.sync_api import sync_playwright

FIREFOX_DB = r"C:\Users\marvi\AppData\Roaming\Mozilla\Firefox\Profiles\h1vl3oun.default-release\cookies.sqlite"
COOKIES_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "linkedin_cookies.json")

# Step 1: Extract cookies directly
print("=== Extracting cookies ===")
tmp_db = os.path.join(os.path.dirname(COOKIES_PATH), "tmp_cookies.sqlite")
shutil.copy2(FIREFOX_DB, tmp_db)
db = sqlite3.connect(f"file:{tmp_db}?mode=ro", uri=True)
db.row_factory = sqlite3.Row
rows = db.execute("""
    SELECT name, value, host, path, expiry, isSecure, isHttpOnly
    FROM moz_cookies WHERE host LIKE '%linkedin.com%'
""").fetchall()
db.close()
os.remove(tmp_db)

cookies = []
for r in rows:
    domain = r["host"]
    if not domain.startswith("."):
        domain = "." + domain.lstrip(".")
    exp = r["expiry"] or 0
    if exp > 10000000000:
        exp = exp // 1000
    cookies.append({
        "name": r["name"], "value": r["value"], "domain": domain,
        "path": r["path"] or "/", "secure": bool(r["isSecure"]),
        "httpOnly": bool(r["isHttpOnly"]),
        "sameSite": "None" if bool(r["isSecure"]) else "Lax",
        "expires": int(exp) if exp > 0 else -1,
    })

# Dedup
dedup = {}
for c in cookies:
    k = (c["name"], c["domain"])
    if k not in dedup or c["expires"] > dedup[k]["expires"]:
        dedup[k] = c
cookies = list(dedup.values())

session = [c["name"] for c in cookies if c["name"] in ["li_at","liap","bscookie","JSESSIONID"]]
print(f"Cookies: {len(cookies)}, Session: {session}")

with open(COOKIES_PATH, "w") as f:
    json.dump(cookies, f, indent=2)

# Step 2: Test with Chromium
print("\n=== Testing with Chromium ===")
with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=False)
    ctx = browser.new_context(viewport={"width": 1280, "height": 900})
    ctx.add_cookies(cookies)
    page = ctx.new_page()
    page.set_default_timeout(30000)
    
    page.goto("https://www.linkedin.com/feed/", wait_until="domcontentloaded", timeout=30000)
    time.sleep(4)
    logged = "login" not in page.url and "authwall" not in page.url
    print(f"Feed logged: {logged}")
    
    if not logged:
        browser.close()
        exit(1)
    
    page.goto("https://www.linkedin.com/in/jan-boone-499310180/", wait_until="domcontentloaded", timeout=30000)
    time.sleep(6)
    print(f"Profile: {page.title()}")
    
    connect = page.evaluate("""() => {
        for (const el of document.querySelectorAll('a')) {
            const href = el.getAttribute('href') || '';
            const r = el.getBoundingClientRect();
            if (r.width > 0 && r.height > 0 && r.top > 200 && r.top < 700 && r.left < 500 && href.includes('custom-invite'))
                return href;
        }
        return null;
    }""")
    print(f"Connect: {connect}")
    
    if connect:
        invite_url = "https://www.linkedin.com" + connect if connect.startswith("/") else connect
        page.goto(invite_url, wait_until="domcontentloaded", timeout=30000)
        time.sleep(4)
        print(f"Invite URL: {page.url}")
        
        if "custom-invite" in page.url:
            info = page.evaluate("""() => ({
                h1: document.querySelector('h1')?.innerText?.trim() || '',
                textareas: Array.from(document.querySelectorAll('textarea')).map(t => ({
                    id: t.id, name: t.name, placeholder: t.placeholder,
                    visible: t.getBoundingClientRect().width > 0
                })),
                buttons: Array.from(document.querySelectorAll('button')).filter(b => {
                    const r = b.getBoundingClientRect();
                    return r.width > 0 && r.height > 0;
                }).map(b => ({
                    text: b.innerText.trim().substring(0, 50),
                    aria: b.getAttribute('aria-label') || ''
                })).filter(b => b.text || b.aria)
            })""")
            print(f"H1: {info['h1']}")
            print(f"Textareas: {json.dumps(info['textareas'], indent=2, ensure_ascii=False)}")
            print(f"Buttons: {json.dumps(info['buttons'], indent=2, ensure_ascii=False)}")
            
            note = "Hoi Jan, leuk om te verconnecten! Groet, ClawBuildr"
            textarea = page.locator("textarea").first
            if textarea.is_visible():
                textarea.fill(note)
                print(f"Typed: {note}")
            
            page.screenshot(path="data/invitation_page.png")
            print("Screenshot saved")
        else:
            print(f"NOT on invite page: {page.url[:80]}")
    
    browser.close()
    print("\nDONE")
