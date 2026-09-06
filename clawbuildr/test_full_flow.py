import sys
sys.path.insert(0, r"C:\Users\marvi\clawbuildr")

from linkedin_engine import _prepare_firefox_profile, _detect_auth_wall, _find_profile_more_button, _check_if_connected
from playwright.sync_api import sync_playwright
import time, json

profile_path = _prepare_firefox_profile()

with sync_playwright() as pw:
    context = pw.firefox.launch_persistent_context(
        profile_path, headless=False, viewport={"width": 1280, "height": 900}
    )
    page = context.new_page()
    page.set_default_timeout(30000)

    # Go to a profile we're NOT connected to
    print("=== Navigating to profile ===")
    page.goto("https://www.linkedin.com/in/ddekleer/", wait_until="domcontentloaded", timeout=90000)
    print("Waiting 10s for SPA load...")
    time.sleep(10)

    # Check status
    status = _check_if_connected(page)
    print(f"Status: {status}")

    # Find More button
    more = _find_profile_more_button(page)
    print(f"More button: {more}")

    if more:
        print(f"\nClicking More button at ({more['x']:.0f}, {more['y']:.0f})...")
        page.mouse.click(more["x"], more["y"])
        time.sleep(3)

        # Get ALL visible items after dropdown opens
        items = page.evaluate("""() => {
            const els = Array.from(document.querySelectorAll('[role="menuitem"], [role="menuitemradio"], button, a, [role="button"]'));
            return els.filter(el => {
                const r = el.getBoundingClientRect();
                return r.width > 0 && r.height > 0 && r.top > 400;
            }).map(el => ({
                tag: el.tagName,
                text: (el.innerText || '').trim().substring(0,60),
                aria: el.getAttribute('aria-label') || '',
                role: el.getAttribute('role') || '',
                top: Math.round(el.getBoundingClientRect().top),
                left: Math.round(el.getBoundingClientRect().left),
            })).filter(el => el.text || el.aria);
        }""")
        print(f"\n=== DROPDOWN ITEMS ({len(items)}) ===")
        for item in items:
            print(f"  tag={item['tag']} text='{item['text']}' aria='{item['aria']}' role='{item['role']}' top={item['top']} left={item['left']}")

        # Check if any item matches Connect keywords
        connect_labels = ["verbinden", "connect", "connectie maken", "kết nối", "se connecter", "conectar"]
        found = [item for item in items if any(
            label in item['text'].lower() or label in item['aria'].lower()
            for label in connect_labels
        )]
        print(f"\n=== CONNECT MATCHES: {len(found)} ===")
        for f in found:
            print(f"  -> {f['text']} ({f['aria']})")

        if not found:
            # Check if there's a "Follow" / "Volgen" / "Theo dõi" item instead
            follow_labels = ["volgen", "follow", "theo dõi", "folgen", "suivre", "seguir"]
            follow_found = [item for item in items if any(
                label in item['text'].lower() or label in item['aria'].lower()
                for label in follow_labels
            )]
            print(f"\n=== FOLLOW MATCHES: {len(follow_found)} ===")
            for f in follow_found:
                print(f"  -> {f['text']} ({f['aria']})")

            # Print ALL items so we can see what's available
            print("\n=== ALL ITEMS (for debugging) ===")
            for item in items:
                print(f"  '{item['text']}' / '{item['aria']}'")

    print("\n=== DONE ===")
    time.sleep(5)
    context.close()
