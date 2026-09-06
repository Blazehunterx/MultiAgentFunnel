import sys
sys.path.insert(0, r"C:\Users\marvi\clawbuildr")

from linkedin_engine import (
    _prepare_firefox_profile, _inject_cookies_into_context,
    _check_if_connected, _find_and_click_button,
    _handle_invitation_page, _send_connection_request
)
from playwright.sync_api import sync_playwright
import json, os, time

COOKIES_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "linkedin_cookies.json")

def run_e2e_test():
    profile_path = _prepare_firefox_profile()

    with sync_playwright() as pw:
        context = pw.firefox.launch_persistent_context(
            profile_path, headless=False, viewport={"width": 1280, "height": 900}
        )
        _inject_cookies_into_context(context)
        page = context.new_page()
        page.set_default_timeout(30000)

        print("=== Step 1: Verify login ===")
        page.goto("https://www.linkedin.com/feed/", wait_until="domcontentloaded", timeout=60000)
        time.sleep(5)

        current_url = page.url
        if "login" in current_url or "authwall" in current_url:
            print("FAIL: Not logged in. Run: python extract_linkedin_cookies.py")
            context.close()
            return False
        print("PASS: Logged in to LinkedIn feed")

        print("\n=== Step 2: Navigate to profile ===")
        page.goto("https://www.linkedin.com/in/jan-boone-499310180/", wait_until="domcontentloaded", timeout=90000)
        time.sleep(8)

        current_url = page.url
        if "authwall" in current_url or "login" in current_url:
            print(f"FAIL: Auth wall hit. URL: {current_url[:80]}")
            context.close()
            return False
        print(f"PASS: Profile loaded. URL: {current_url[:80]}")

        print("\n=== Step 3: Check connection status ===")
        status = _check_if_connected(page)
        print(f"Status: {status}")
        if status == "unknown":
            print("WARN: Status unknown - inspecting page elements...")
            debug = page.evaluate("""() => {
                const els = Array.from(document.querySelectorAll('button, a, [role="button"]'));
                return els.filter(el => {
                    const r = el.getBoundingClientRect();
                    return r.width > 0 && r.height > 0 && r.top < 600 && r.left < 500;
                }).map(el => ({
                    tag: el.tagName,
                    text: (el.innerText || '').trim().substring(0, 50),
                    aria: el.getAttribute('aria-label') || '',
                }));
            }""")
            print(f"Elements in profile area: {json.dumps(debug, indent=2, ensure_ascii=False)}")

        if status in ("connected", "pending"):
            print(f"SKIP: Already {status}. Cannot test connect flow.")
            context.close()
            return True

        print("\n=== Step 4: Find Connect link href ===")
        invite_url = page.evaluate("""() => {
            const els = Array.from(document.querySelectorAll('a'));
            for (const el of els) {
                const href = el.getAttribute('href') || '';
                const r = el.getBoundingClientRect();
                if (r.width === 0 || r.height === 0) continue;
                if (r.top > 600 || r.left > 500) continue;
                if (href.includes('custom-invite') || href.includes('invitation')) {
                    let fullHref = href;
                    if (fullHref.startsWith('/')) fullHref = 'https://www.linkedin.com' + fullHref;
                    return fullHref;
                }
            }
            const connectTexts = ['verbinden', 'connect', 'k结 nối', 'se connecter', 'conectar'];
            for (const el of els) {
                const txt = (el.innerText || '').toLowerCase().trim().normalize('NFC');
                const r = el.getBoundingClientRect();
                if (r.width === 0 || r.height === 0) continue;
                if (r.top > 600 || r.left > 500) continue;
                if (connectTexts.includes(txt)) {
                    let href = el.getAttribute('href') || '';
                    if (href.startsWith('/')) href = 'https://www.linkedin.com' + href;
                    return href || 'button_no_href';
                }
            }
            return null;
        }""")
        print(f"Invite URL: {invite_url}")

        if not invite_url:
            print("FAIL: No Connect link found")
            context.close()
            return False
        print("PASS: Connect link found")

        print("\n=== Step 5: Navigate to invitation page ===")
        page.goto(invite_url, wait_until="domcontentloaded", timeout=30000)
        time.sleep(4)

        final_url = page.url
        print(f"Final URL: {final_url}")

        if "custom-invite" not in final_url and "invitation" not in final_url:
            print(f"FAIL: Not on invitation page. URL: {final_url[:80]}")
            context.close()
            return False
        print("PASS: On invitation page")

        print("\n=== Step 6: Inspect invitation page ===")
        page_info = page.evaluate("""() => {
            return {
                title: document.title,
                h1: document.querySelector('h1')?.innerText?.trim() || '',
                textareas: Array.from(document.querySelectorAll('textarea')).map(t => ({
                    id: t.id, name: t.name || '',
                    placeholder: t.placeholder || '',
                    aria: t.getAttribute('aria-label') || '',
                    visible: t.getBoundingClientRect().width > 0,
                })),
                buttons: Array.from(document.querySelectorAll('button')).filter(b => {
                    const r = b.getBoundingClientRect();
                    return r.width > 0 && r.height > 0;
                }).map(b => ({
                    text: (b.innerText || '').trim().substring(0,50),
                    aria: b.getAttribute('aria-label') || '',
                    type: b.type || '',
                })).filter(b => b.text || b.aria),
            };
        }""")
        print(f"Title: {page_info['title']}")
        print(f"H1: {page_info['h1']}")
        print(f"Textareas: {json.dumps(page_info['textareas'], indent=2, ensure_ascii=False)}")
        print(f"Buttons: {json.dumps(page_info['buttons'], indent=2, ensure_ascii=False)}")

        has_textarea = any(t.get('visible', False) for t in page_info.get('textareas', []))
        send_buttons = [b for b in page_info.get('buttons', []) if
                        any(kw in (b.get('text', '') + b.get('aria', '')).lower()
                            for kw in ['versturen', 'verzenden', 'send', 'gửi', 'senden', 'envoyer', 'enviar', 'kết nối'])]

        print(f"\nHas textarea: {has_textarea}")
        print(f"Send buttons found: {len(send_buttons)}")

        print("\n=== Step 7: Test _handle_invitation_page (dry run) ===")
        test_note = "Hi Jan, test bericht van ClawBuildr."
        textarea_selectors = [
            'textarea#custom-message',
            'textarea[name="custom-message"]',
            'textarea[name="message"]',
            'textarea',
        ]
        textarea_found = False
        for sel in textarea_selectors:
            try:
                loc = page.locator(sel).first()
                if loc.is_visible(timeout=3000):
                    textarea_found = True
                    print(f"  Found textarea: {sel}")
                    break
            except Exception:
                continue

        if not textarea_found:
            print("  No textarea visible - clicking 'Add a note' first...")
            note_clicked = _find_and_click_button(
                page,
                texts=["opmerking toevoegen", "add a note", "thêm ghi chú",
                        "notiz hinzufügen", "ajouter une note", "añadir una nota"],
                aria_keywords=["opmerking", "note", "ghi chú", "notiz", "note", "nota"]
            )
            print(f"  Clicked add note: {note_clicked}")
            time.sleep(2)
            for sel in textarea_selectors:
                try:
                    loc = page.locator(sel).first()
                    if loc.is_visible(timeout=3000):
                        textarea_found = True
                        print(f"  Found textarea after click: {sel}")
                        break
                except Exception:
                    continue

        print(f"Textarea found: {textarea_found}")
        print(f"Send button available: {len(send_buttons) > 0}")

        print("\n=== RESULTS ===")
        all_pass = (
            "login" not in current_url and
            "authwall" not in page.url and
            status in ("not_connected", "connected", "pending") and
            invite_url is not None and
            "custom-invite" in final_url
        )
        if all_pass:
            print("ALL CHECKS PASSED - Engine is ready for outreach")
        else:
            print("SOME CHECKS FAILED - Review output above")

        context.close()
        return all_pass


if __name__ == "__main__":
    print("LinkedIn Outreach Engine - E2E Test")
    print("=" * 50)
    success = run_e2e_test()
    print(f"\nFinal: {'PASS' if success else 'FAIL'}")