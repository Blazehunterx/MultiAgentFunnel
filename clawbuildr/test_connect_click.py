import sys
sys.path.insert(0, r"C:\Users\marvi\clawbuildr")

from linkedin_engine import (
    _prepare_firefox_profile, _search_and_find_profile, _detect_auth_wall,
    _check_if_connected, _find_and_click_button
)
from playwright.sync_api import sync_playwright
import time, json

profile_path = _prepare_firefox_profile()

with sync_playwright() as pw:
    context = pw.firefox.launch_persistent_context(
        profile_path, headless=False, viewport={"width": 1280, "height": 900}
    )
    page = context.new_page()
    page.set_default_timeout(30000)

    # Search for someone we're definitely NOT connected to
    print("=== Searching for a random person ===")
    profile_url, error = _search_and_find_profile(page, "Jan", "Boone", "Marketing Manager WPP")
    print(f"Found: {profile_url}, Error: {error}")

    if profile_url:
        page.goto(profile_url, wait_until="domcontentloaded", timeout=90000)
        time.sleep(10)

        status = _check_if_connected(page)
        print(f"Status: {status}")

        # Check for Connect element
        exists = page.evaluate("""() => {
            const els = Array.from(document.querySelectorAll('button, a, [role="button"]'));
            for (const el of els) {
                const txt = (el.innerText || '').toLowerCase().trim();
                const aria = (el.getAttribute('aria-label') || '').toLowerCase();
                const r = el.getBoundingClientRect();
                if (r.width === 0 || r.height === 0) continue;
                if (txt === 'kết nối' || txt === 'verbinden' || txt === 'connect' ||
                    aria.includes('kết nối') || aria.includes('verbinden') || aria.includes('invite') || aria.includes('mời')) {
                    return {tag: el.tagName, text: txt, aria: aria.substring(0,60), top: Math.round(r.top), left: Math.round(r.left)};
                }
            }
            return null;
        }""")
        print(f"Connect element: {exists}")

        if exists and status == "not_connected":
            print("\n=== CLICKING CONNECT (will check modal but NOT send) ===")
            connect_texts = ["verbinden", "connect", "kết nối", "se connecter", "conectar"]
            connect_arias = ["verbinden", "connect", "kết nối", "mời", "invite", "se connecter", "conectar"]
            clicked = _find_and_click_button(page, texts=connect_texts, aria_keywords=connect_arias)
            print(f"Clicked: {clicked}")

            if clicked:
                time.sleep(3)
                modal = page.evaluate("""() => {
                    const modal = document.querySelector('.artdeco-modal, [role="dialog"]');
                    if (!modal) return {found: false};
                    const text = (modal.innerText || '').substring(0, 300);
                    const btns = Array.from(modal.querySelectorAll('button')).map(b => ({
                        text: (b.innerText || '').trim().substring(0, 40),
                    })).filter(b => b.text);
                    const textareas = modal.querySelectorAll('textarea');
                    return {found: true, text: text, buttons: btns, textarea_count: textareas.length};
                }""")
                print(f"Modal: {json.dumps(modal, indent=2, ensure_ascii=False)}")

                # Close without sending
                page.evaluate("""() => {
                    const btn = document.querySelector('.artdeco-modal__dismiss, [aria-label="Dismiss"], [aria-label*="Close"], [aria-label*="Sluiten"], [aria-label*="Đóng"]');
                    if (btn) btn.click();
                }""")
                time.sleep(2)
        else:
            print(f"Cannot test Connect click: status={status}, element={exists}")

    print("\n=== DONE ===")
    time.sleep(3)
    context.close()
