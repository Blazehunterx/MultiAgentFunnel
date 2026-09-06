import json, time
from playwright.sync_api import sync_playwright

with open("data/linkedin_cookies.json") as f:
    cookies = json.load(f)

with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=False)
    ctx = browser.new_context(viewport={"width": 1280, "height": 900})
    ctx.add_cookies(cookies)
    page = ctx.new_page()
    page.set_default_timeout(30000)
    
    print("Feed...")
    page.goto("https://www.linkedin.com/feed/", wait_until="domcontentloaded", timeout=30000)
    time.sleep(4)
    logged = "login" not in page.url and "authwall" not in page.url
    print(f"URL: {page.url}")
    print(f"Logged: {logged}")
    
    if logged:
        print("Profile...")
        page.goto("https://www.linkedin.com/in/jan-boone-499310180/", wait_until="domcontentloaded", timeout=30000)
        time.sleep(6)
        print(f"URL: {page.url}")
        print(f"Title: {page.title()}")
        
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
        
        if connect:
            # Dismiss any overlays/banners that might intercept clicks
            page.evaluate("""() => {
                document.querySelectorAll('.premium-upsell-link, .msg-overlay-list-bubble, [data-testid="premium-upsell"]').forEach(el => el.remove());
            }""")
            time.sleep(1)
            
            print("Clicking Connect via JS...")
            page.evaluate("""() => {
                const link = document.querySelector('a[href*="custom-invite"]');
                if (link) link.click();
            }""")
            time.sleep(4)
            print(f"URL: {page.url}")
            print(f"Title: {page.title()}")
            
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
                        aria: b.getAttribute('aria-label') || '',
                        type: b.type
                    })).filter(b => b.text || b.aria)
                })""")
                print(f"H1: {info['h1']}")
                print(f"Textareas: {json.dumps(info['textareas'], indent=2, ensure_ascii=False)}")
                print(f"Buttons: {json.dumps(info['buttons'], indent=2, ensure_ascii=False)}")
                
                page.screenshot(path="data/invitation_page.png")
                print("Screenshot saved")
    
    browser.close()
    print("DONE")
