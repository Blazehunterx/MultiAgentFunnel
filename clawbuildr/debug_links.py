import sys
sys.path.insert(0, r"C:\Users\marvi\clawbuildr")
from linkedin_engine import _prepare_firefox_profile, _check_if_connected
from playwright.sync_api import sync_playwright
import time, json

profile_path = _prepare_firefox_profile()

with sync_playwright() as pw:
    context = pw.firefox.launch_persistent_context(profile_path, headless=False, viewport={"width": 1280, "height": 900})
    page = context.new_page()
    page.set_default_timeout(30000)

    page.goto("https://www.linkedin.com/in/jan-boone-499310180/", wait_until="domcontentloaded", timeout=90000)
    time.sleep(10)

    # Get ALL links in the profile action area with their full details
    links = page.evaluate("""() => {
        const els = Array.from(document.querySelectorAll('a'));
        return els.filter(el => {
            const r = el.getBoundingClientRect();
            return r.width > 0 && r.height > 0 && r.top > 400 && r.top < 600 && r.left < 500;
        }).map(el => {
            const r = el.getBoundingClientRect();
            return {
                text: (el.innerText || '').trim().substring(0,80),
                aria: el.getAttribute('aria-label') || '',
                href: el.getAttribute('href') || '',
                top: Math.round(r.top),
                left: Math.round(r.left),
            };
        });
    }""")
    print(f"=== LINKS IN ACTION AREA ({len(links)}) ===")
    for l in links:
        print(f"  text='{l['text']}' aria='{l['aria']}' href='{l['href']}' top={l['top']} left={l['left']}")

    # Also check status
    status = _check_if_connected(page)
    print(f"\nStatus: {status}")

    # Find by href containing custom-invite
    invite_links = [l for l in links if 'custom-invite' in l['href'] or 'invitation' in l['href']]
    print(f"\nLinks with custom-invite in href: {len(invite_links)}")
    for l in invite_links:
        print(f"  -> {l}")

    print("\n=== DONE ===")
    time.sleep(3)
    context.close()
