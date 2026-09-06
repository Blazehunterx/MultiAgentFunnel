import json, time
from playwright.sync_api import sync_playwright

with open("data/linkedin_cookies.json") as f:
    cookies = json.load(f)

print(f"Cookies loaded: {len(cookies)}")
for c in cookies:
    if c["name"] in ["li_at", "liap", "bscookie"]:
        print(f"  {c['name']}: {c['value'][:30]}... domain={c['domain']}")

with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=False)
    ctx = browser.new_context(viewport={"width": 1280, "height": 900})
    
    # Add cookies
    ctx.add_cookies(cookies)
    page = ctx.new_page()
    
    # Verify cookies are set
    stored = ctx.cookies("https://www.linkedin.com")
    print(f"\nStored cookies: {len(stored)}")
    for c in stored:
        if c["name"] in ["li_at", "liap", "bscookie"]:
            print(f"  {c['name']}: {c['value'][:30]}... domain={c['domain']}")
    
    print("\n=== Feed ===")
    page.goto("https://www.linkedin.com/feed/", wait_until="domcontentloaded", timeout=30000)
    time.sleep(5)
    print(f"URL: {page.url}")
    print(f"Title: {page.title()}")
    
    is_logged = "login" not in page.url and "authwall" not in page.url
    print(f"Logged in: {is_logged}")
    
    if is_logged:
        print("\n=== Profile ===")
        page.goto("https://www.linkedin.com/in/jan-boone-499310180/", wait_until="domcontentloaded", timeout=30000)
        time.sleep(5)
        print(f"URL: {page.url}")
        print(f"Title: {page.title()}")
        
        # Check for auth wall in page content
        body = page.evaluate("() => document.body.innerText.substring(0, 500)")
        print(f"Body text: {body[:200]}")
        
        connect = page.evaluate("""() => {
            const els = Array.from(document.querySelectorAll('a'));
            for (const el of els) {
                const href = el.getAttribute('href') || '';
                const r = el.getBoundingClientRect();
                if (r.width > 0 && r.height > 0 && r.top > 200 && r.top < 700 && r.left < 500) {
                    if (href.includes('custom-invite')) {
                        return {href: href, text: el.innerText};
                    }
                }
            }
            return null;
        }""")
        print(f"Connect: {connect}")
    
    browser.close()
    print("DONE")
