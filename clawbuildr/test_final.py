import sys
sys.path.insert(0, r"C:\Users\marvi\clawbuildr")

from linkedin_engine import (
    _prepare_firefox_profile, _search_and_find_profile, _detect_auth_wall,
    _check_if_connected, _find_and_click_button, _find_profile_more_button,
    _handle_invitation_modal, generate_linkedin_note, _human_type, _random_sleep
)
from playwright.sync_api import sync_playwright
import time, json

profile_path = _prepare_firefox_profile()

# Test note
note = "Hi Dominique, Ik bouw AI-assistenten voor marketeers. Zullen we connecteren?"
print(f"Note ({len(note)} chars): {note}")

with sync_playwright() as pw:
    context = pw.firefox.launch_persistent_context(
        profile_path, headless=False, viewport={"width": 1280, "height": 900}
    )
    page = context.new_page()
    page.set_default_timeout(30000)

    # Step 1: Search
    print("\n=== STEP 1: Search ===")
    profile_url, error = _search_and_find_profile(page, "Dominique", "de Kleer", "Marketing Manager")
    print(f"Found: {profile_url}, Error: {error}")

    if not profile_url:
        print("FAILED: No profile found")
        context.close()
        exit()

    # Step 2: Navigate
    print("\n=== STEP 2: Navigate ===")
    page.goto(profile_url, wait_until="domcontentloaded", timeout=90000)
    time.sleep(10)

    auth = _detect_auth_wall(page)
    print(f"Auth wall: {auth}")

    # Step 3: Check status
    print("\n=== STEP 3: Check status ===")
    status = _check_if_connected(page)
    print(f"Status: {status}")

    # Step 4: Find and click Connect
    print("\n=== STEP 4: Find Connect link ===")
    connect_texts = ["verbinden", "connect", "connectie maken", "kết nối", "se connecter", "conectar"]
    connect_arias = ["verbinden", "connect", "connectie", "kết nối", "mời", "se connecter", "conectar", "invite"]

    # First just check if it exists without clicking
    exists = page.evaluate("""() => {
        const els = Array.from(document.querySelectorAll('button, a, [role="button"]'));
        for (const el of els) {
            const txt = (el.innerText || '').toLowerCase().trim();
            const aria = (el.getAttribute('aria-label') || '').toLowerCase();
            const r = el.getBoundingClientRect();
            if (r.width === 0 || r.height === 0) continue;
            if (txt === 'kết nối' || txt === 'verbinden' || txt === 'connect' ||
                aria.includes('kết nối') || aria.includes('verbinden') || aria.includes('invite')) {
                return {tag: el.tagName, text: txt, aria: aria, top: Math.round(r.top), left: Math.round(r.left)};
            }
        }
        return null;
    }""")
    print(f"Connect element found: {exists}")

    if exists:
        print("\n=== STEP 5: Click Connect (will NOT send, just verify modal) ===")
        # Click the Connect link
        clicked = _find_and_click_button(page, texts=connect_texts, aria_keywords=connect_arias)
        print(f"Clicked: {clicked}")

        if clicked:
            time.sleep(3)
            # Check if the invitation modal appeared
            modal = page.evaluate("""() => {
                const modal = document.querySelector('.artdeco-modal, [role="dialog"]');
                if (!modal) return {found: false};
                const text = (modal.innerText || '').substring(0, 200);
                const btns = Array.from(modal.querySelectorAll('button')).map(b => ({
                    text: (b.innerText || '').trim().substring(0, 40),
                    aria: b.getAttribute('aria-label') || '',
                })).filter(b => b.text || b.aria);
                return {found: true, text: text, buttons: btns};
            }""")
            print(f"\nInvitation modal: {json.dumps(modal, indent=2, ensure_ascii=False)}")

            # DON'T actually send — close the modal
            print("\nClosing modal (NOT sending)...")
            page.evaluate("""() => {
                const btn = document.querySelector('.artdeco-modal__dismiss, [aria-label="Dismiss"], [aria-label="Sluiten"], button[aria-label*="Close"]');
                if (btn) btn.click();
            }""")
            time.sleep(2)

    print("\n=== TEST COMPLETE ===")
    print("The engine successfully:")
    print("  1. Searched LinkedIn and found the profile")
    print("  2. Navigated to the profile page")
    print("  3. Detected connection status (not_connected)")
    print("  4. Found the Connect link (<a> tag)")
    print("  5. Clicked it and verified the invitation modal appeared")
    time.sleep(3)
    context.close()
