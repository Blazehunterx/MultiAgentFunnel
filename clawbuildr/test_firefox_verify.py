import json, time
from playwright.sync_api import sync_playwright

with open("data/linkedin_cookies.json") as f:
    cookies = json.load(f)

with sync_playwright() as pw:
    browser = pw.firefox.launch(headless=True)
    ctx = browser.new_context(viewport={"width": 1280, "height": 900})
    ctx.add_cookies(cookies)
    page = ctx.new_page()
    page.set_default_timeout(30000)

    print("1. Feed...")
    page.goto("https://www.linkedin.com/feed/", wait_until="domcontentloaded", timeout=30000)
    time.sleep(4)
    logged = "login" not in page.url and "authwall" not in page.url
    print(f"   Logged: {logged}")

    if not logged:
        print("FAIL: not logged in")
        browser.close()
        exit(1)

    print("2. Profile...")
    page.goto("https://www.linkedin.com/in/jan-boone-499310180/", wait_until="domcontentloaded", timeout=30000)
    time.sleep(6)
    print(f"   Title: {page.title()}")

    connect = page.evaluate("""() => {
        for (const el of document.querySelectorAll('a')) {
            const href = el.getAttribute('href') || '';
            const r = el.getBoundingClientRect();
            if (r.width > 0 && r.height > 0 && r.top > 200 && r.top < 700 && r.left < 500 && href.includes('custom-invite'))
                return href;
        }
        return null;
    }""")
    print(f"   Connect: {connect}")

    if connect:
        print("3. Click Connect...")
        try:
            connect_el = page.locator('a[href*="custom-invite"]').first
            connect_el.click(timeout=5000, force=True)
        except Exception:
            page.evaluate("""() => {
                const link = document.querySelector('a[href*="custom-invite"]');
                if (link) link.click();
            }""")
        try:
            page.wait_for_url("**/preload/custom-invite/**", timeout=10000)
        except Exception:
            pass
        time.sleep(3)
        print(f"   URL: {page.url}")

        if "custom-invite" in page.url:
            print("4. ON INVITATION PAGE")
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
            print(f"   H1: {info['h1']}")
            print(f"   Textareas: {json.dumps(info['textareas'], indent=2, ensure_ascii=False)}")
            print(f"   Buttons: {json.dumps(info['buttons'], indent=2, ensure_ascii=False)}")

            note = "Hoi Jan, leuk om te verbinden! Groet, ClawBuildr"
            textarea = page.locator("textarea").first
            if textarea.is_visible():
                textarea.fill(note)
                print(f"   Typed: {note}")

            send = [b for b in info["buttons"] if any(kw in (b["text"] + b["aria"]).lower() for kw in ["versturen","verzenden","send"])]
            if send:
                print(f"   Send button: {send[0]}")
                print("   DRY RUN - not sending")

            page.screenshot(path="data/invitation_page.png")
            print("   Screenshot saved")
        else:
            print(f"   FAIL: not on invite page")
    else:
        print("   FAIL: no connect link")

    browser.close()
    print("\nDONE - your Firefox session is untouched")
