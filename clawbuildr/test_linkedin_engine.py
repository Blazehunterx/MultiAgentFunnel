import sys
sys.path.insert(0, r"C:\Users\marvi\clawbuildr")

from linkedin_engine import _prepare_firefox_profile, _search_and_find_profile, _detect_auth_wall, _check_if_connected, _find_profile_more_button, _log
from playwright.sync_api import sync_playwright
import time

profile_path = _prepare_firefox_profile()
print(f"Profile prepared: {profile_path}")

with sync_playwright() as pw:
    context = pw.firefox.launch_persistent_context(
        profile_path, headless=False, viewport={"width": 1280, "height": 900}
    )
    page = context.new_page()
    page.set_default_timeout(30000)

    # Test: search for a real person
    print("\n=== TEST: Search for Dominique de Kleer ===")
    profile_url, error = _search_and_find_profile(page, "Dominique", "de Kleer", "Marketing Manager")
    print(f"Profile URL: {profile_url}")
    print(f"Error: {error}")

    if profile_url:
        print(f"\n=== TEST: Navigate to profile ===")
        page.goto(profile_url, wait_until="domcontentloaded", timeout=90000)
        time.sleep(6)

        auth = _detect_auth_wall(page)
        print(f"Auth wall: {auth}")

        # Get profile name
        name = page.evaluate("() => document.querySelector('h1')?.innerText?.trim() || ''")
        print(f"Profile H1: {name}")

        # Check connection status
        status = _check_if_connected(page)
        print(f"Connection status: {status}")

        # Find the More button
        more = _find_profile_more_button(page)
        print(f"More button position: {more}")

        # Get all visible buttons in the profile action area
        buttons = page.evaluate("""() => {
            return Array.from(document.querySelectorAll('button')).filter(b => {
                const r = b.getBoundingClientRect();
                return r.width > 0 && r.height > 0 && r.top > 100 && r.top < 600;
            }).map(b => ({
                text: (b.innerText||'').trim().substring(0,50),
                aria: b.getAttribute('aria-label')||'',
                top: Math.round(b.getBoundingClientRect().top),
            })).filter(b => b.text || b.aria);
        }""")
        print(f"\nProfile action buttons:")
        for b in buttons:
            print(f"  text='{b['text']}' aria='{b['aria']}' top={b['top']}")

    print("\n=== TEST COMPLETE ===")
    print("Browser will stay open for 10 seconds for visual inspection...")
    time.sleep(10)
    context.close()
