import sys
sys.path.insert(0, r"C:\Users\marvi\clawbuildr")

from linkedin_engine import _prepare_firefox_profile, _check_if_connected, _detect_auth_wall
from playwright.sync_api import sync_playwright
import time, json

profile_path = _prepare_firefox_profile()
note = "Hi Jan, Ik bouw AI-assistenten voor marketeers. Leuk om te verbinden!"

with sync_playwright() as pw:
    context = pw.firefox.launch_persistent_context(profile_path, headless=False, viewport={"width": 1280, "height": 900})
    page = context.new_page()
    page.set_default_timeout(30000)

    # Go to profile first
    print("=== Navigate to profile ===")
    page.goto("https://www.linkedin.com/in/jan-boone-499310180/", wait_until="domcontentloaded", timeout=90000)
    time.sleep(10)

    status = _check_if_connected(page)
    print(f"Status: {status}")

    # Extract the Connect link href
    invite_url = page.evaluate("""() => {
        const els = Array.from(document.querySelectorAll('a'));
        for (const el of els) {
            const txt = (el.innerText || '').toLowerCase().trim().normalize('NFC');
            const aria = (el.getAttribute('aria-label') || '').toLowerCase().normalize('NFC');
            const r = el.getBoundingClientRect();
            if (r.width === 0 || r.height === 0) continue;
            if (r.top > 600 || r.left > 500) continue;
            if (txt.includes('verbinden') || txt.includes('connect') || txt.includes('kết nối') ||
                (aria.includes('mời') && aria.includes('kết nối'))) {
                let href = el.getAttribute('href') || '';
                if (href.startsWith('/')) href = 'https://www.linkedin.com' + href;
                return href;
            }
        }
        return null;
    }""")
    print(f"Invite URL: {invite_url}")

    if invite_url:
        print(f"\n=== Navigating to invitation page ===")
        page.goto(invite_url, wait_until="domcontentloaded", timeout=30000)
        time.sleep(4)

        print(f"URL: {page.url}")

        # Inspect the invitation page
        page_info = page.evaluate("""() => {
            return {
                title: document.title,
                h1: document.querySelector('h1')?.innerText?.trim() || '',
                h2s: Array.from(document.querySelectorAll('h2')).map(h => h.innerText.trim()).filter(t => t).slice(0,5),
                textareas: Array.from(document.querySelectorAll('textarea')).map(t => ({
                    id: t.id, name: t.name || '',
                    placeholder: t.placeholder || '',
                    aria: t.getAttribute('aria-label') || '',
                    visible: t.getBoundingClientRect().width > 0,
                    rows: t.rows,
                })),
                buttons: Array.from(document.querySelectorAll('button')).filter(b => {
                    const r = b.getBoundingClientRect();
                    return r.width > 0 && r.height > 0;
                }).map(b => ({
                    text: (b.innerText || '').trim().substring(0,50),
                    aria: b.getAttribute('aria-label') || '',
                    type: b.type || '',
                    cls: (b.className || '').substring(0,60),
                })).filter(b => b.text || b.aria),
                body_text: (document.body.innerText || '').substring(0, 500),
            };
        }""")
        print(f"\n=== INVITATION PAGE ===")
        print(f"Title: {page_info['title']}")
        print(f"H1: {page_info['h1']}")
        print(f"H2s: {page_info['h2s']}")
        print(f"\nTextareas: {json.dumps(page_info['textareas'], indent=2, ensure_ascii=False)}")
        print(f"\nButtons: {json.dumps(page_info['buttons'], indent=2, ensure_ascii=False)}")
        print(f"\nBody text: {page_info['body_text']}")

        # Take screenshot
        page.screenshot(path=r"C:\Users\marvi\clawbuildr\data\invite_page.png")

    print("\n=== DONE ===")
    time.sleep(5)
    context.close()
