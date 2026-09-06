import json, time
from playwright.sync_api import sync_playwright

with open("data/linkedin_cookies.json") as f:
    cookies = json.load(f)

print(f"Loaded {len(cookies)} cookies")
session = [c["name"] for c in cookies if c["name"] in ["li_at","liap","bscookie","JSESSIONID"]]
print(f"Session: {session}")

with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=False)
    ctx = browser.new_context(viewport={"width": 1280, "height": 900})
    ctx.add_cookies(cookies)
    page = ctx.new_page()
    page.set_default_timeout(30000)
    
    print("\n=== Feed ===")
    page.goto("https://www.linkedin.com/feed/", wait_until="domcontentloaded", timeout=30000)
    time.sleep(4)
    logged = "login" not in page.url and "authwall" not in page.url
    print(f"Logged: {logged} URL: {page.url[:60]}")
    
    if not logged:
        browser.close()
        print("NOT LOGGED IN - abort")
        exit(1)
    
    print("\n=== Profile ===")
    page.goto("https://www.linkedin.com/in/jan-boone-499310180/", wait_until="domcontentloaded", timeout=30000)
    time.sleep(6)
    print(f"Title: {page.title()}")
    
    connect = page.evaluate("""() => {
        const els = Array.from(document.querySelectorAll('a'));
        for (const el of els) {
            const href = el.getAttribute('href') || '';
            const r = el.getBoundingClientRect();
            if (r.width > 0 && r.height > 0 && r.top > 200 && r.top < 700 && r.left < 500) {
                if (href.includes('custom-invite')) return href;
            }
        }
        return null;
    }""")
    print(f"Connect href: {connect}")
    
    if connect:
        print("\n=== Navigate to invitation page ===")
        invite_url = "https://www.linkedin.com" + connect if connect.startswith("/") else connect
        page.goto(invite_url, wait_until="domcontentloaded", timeout=30000)
        time.sleep(4)
        print(f"URL: {page.url}")
        
        if "custom-invite" in page.url:
            print("\n=== ON INVITATION PAGE ===")
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
                    aria: b.getAttribute('aria-label') || '',
                    type: b.type
                })).filter(b => b.text || b.aria)
            })""")
            print(f"H1: {info['h1']}")
            print(f"Textareas: {json.dumps(info['textareas'], indent=2, ensure_ascii=False)}")
            print(f"Buttons: {json.dumps(info['buttons'], indent=2, ensure_ascii=False)}")
            
            note = "Hoi Jan, leuk om te verconnecten! Groet, ClawBuildr"
            textarea = page.locator("textarea").first
            if textarea.is_visible():
                textarea.click()
                textarea.fill(note)
                print(f"\nTyped note: {note}")
                
                send_btns = [b for b in info["buttons"] if any(kw in (b["text"] + b["aria"]).lower() for kw in ["versturen","verzenden","send","gửi"])]
                if send_btns:
                    print(f"Send button: {send_btns[0]['text']}")
                    print("(DRY RUN - not clicking send)")
            
            page.screenshot(path="data/invitation_page.png")
            print("Screenshot saved")
        else:
            print(f"Not on invite page: {page.url[:80]}")
    else:
        print("No connect link found")
    
    browser.close()
    print("\nDONE")
