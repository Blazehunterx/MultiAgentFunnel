import json, time
from playwright.sync_api import sync_playwright

with open("data/linkedin_cookies.json") as f:
    cookies = json.load(f)

with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=False)
    ctx = browser.new_context(viewport={"width": 1280, "height": 900})
    ctx.add_cookies(cookies)
    page = ctx.new_page()
    
    # First warm up the session on feed
    print("Warming up on feed...")
    page.goto("https://www.linkedin.com/feed/", wait_until="domcontentloaded", timeout=30000)
    time.sleep(5)
    print(f"Feed URL: {page.url}")
    print(f"Feed Title: {page.title()}")
    
    logged = "login" not in page.url and "authwall" not in page.url
    print(f"Logged in: {logged}")
    
    if logged:
        # Try profile
        print("\nNavigating to profile...")
        page.goto("https://www.linkedin.com/in/jan-boone-499310180/", wait_until="domcontentloaded", timeout=30000)
        time.sleep(8)
        
        url = page.url
        title = page.title()
        print(f"URL: {url}")
        print(f"Title: {title}")
        
        if "authwall" in url or "login" in url or "Inschrijven" in title:
            print("AUTH WALL on profile page")
            # Try clicking through the auth wall
            body = page.evaluate("() => document.body.innerText.substring(0, 300)")
            print(f"Body: {body[:200]}")
        else:
            print("PROFILE LOADED")
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
