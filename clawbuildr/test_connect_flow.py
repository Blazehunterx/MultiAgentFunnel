import json, time
from playwright.sync_api import sync_playwright

with open('data/linkedin_cookies.json') as f:
    cookies = json.load(f)

with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=False)
    ctx = browser.new_context(viewport={"width": 1280, "height": 900})
    ctx.add_cookies(cookies)
    page = ctx.new_page()
    page.set_default_timeout(30000)
    
    print("=== Profile ===")
    page.goto("https://www.linkedin.com/in/jan-boone-499310180/", wait_until="domcontentloaded", timeout=30000)
    time.sleep(5)
    print(f"Title: {page.title()}")
    
    print("=== Clicking Connect ===")
    connect_link = page.locator('a[href*="custom-invite"]').first
    if connect_link.is_visible():
        connect_link.click()
        time.sleep(4)
        url = page.url
        print(f"URL after click: {url}")
        
        if "custom-invite" in url:
            print("=== ON INVITATION PAGE ===")
            
            info = page.evaluate("""() => {
                return {
                    title: document.title,
                    h1: document.querySelector('h1')?.innerText?.trim() || '',
                    textareas: Array.from(document.querySelectorAll('textarea')).map(t => ({
                        id: t.id, name: t.name, placeholder: t.placeholder,
                        visible: t.getBoundingClientRect().width > 0
                    })),
                    buttons: Array.from(document.querySelectorAll('button')).filter(b => {
                        const r = b.getBoundingClientRect();
                        return r.width > 0 && r.height > 0;
                    }).map(b => ({
                        text: b.innerText.trim().substring(0,50),
                        aria: b.getAttribute('aria-label') || '',
                        type: b.type
                    })).filter(b => b.text || b.aria)
                };
            }""")
            
            print(f"H1: {info['h1']}")
            print(f"Textareas: {json.dumps(info['textareas'], indent=2, ensure_ascii=False)}")
            print(f"Buttons: {json.dumps(info['buttons'], indent=2, ensure_ascii=False)}")
            
            # Type a test note
            note = "Hoi Jan, leuk om te verbinden! Groet, ClawBuildr"
            textarea = page.locator("textarea").first
            if textarea.is_visible():
                textarea.click()
                textarea.fill(note)
                print(f"Typed note: {note}")
                time.sleep(1)
                
                # Find send button
                send_buttons = page.locator("button").all()
                for btn in send_buttons:
                    txt = btn.inner_text().strip().lower()
                    if any(kw in txt for kw in ["versturen", "verzenden", "send", "gửi", "senden"]):
                        print(f"Send button found: {btn.inner_text()}")
                        print("(NOT clicking send - dry run)")
                        break
                else:
                    print("No send button found via text search")
            
            page.screenshot(path="data/invitation_page.png")
            print("Screenshot saved to data/invitation_page.png")
    else:
        print("Connect link not found!")
    
    browser.close()
    print("DONE")
