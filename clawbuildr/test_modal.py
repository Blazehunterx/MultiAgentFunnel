import sys
sys.path.insert(0, r"C:\Users\marvi\clawbuildr")

from linkedin_engine import (
    _prepare_firefox_profile, _detect_auth_wall, _check_if_connected,
    _find_and_click_button, _handle_invitation_modal
)
from playwright.sync_api import sync_playwright
import time, json

profile_path = _prepare_firefox_profile()
note = "Hi Jan, Ik bouw AI-assistenten voor marketeers. Zullen we connecteren?"

with sync_playwright() as pw:
    context = pw.firefox.launch_persistent_context(profile_path, headless=False, viewport={"width": 1280, "height": 900})
    page = context.new_page()
    page.set_default_timeout(30000)

    # Go to Jan Boone's profile (we're NOT connected)
    print("=== Navigate to profile ===")
    page.goto("https://www.linkedin.com/in/jan-boone-499310180/", wait_until="domcontentloaded", timeout=90000)
    time.sleep(10)

    status = _check_if_connected(page)
    print(f"Status: {status}")

    if status == "not_connected":
        # Click Connect
        print("\n=== Clicking Connect ===")
        connect_texts = ["verbinden", "connect", "kết nối", "se connecter", "conectar"]
        connect_arias = ["verbinden", "connect", "kết nối", "mời", "invite", "se connecter", "conectar"]
        clicked = _find_and_click_button(page, texts=connect_texts, aria_keywords=connect_arias, profile_area_only=True)
        print(f"Connect clicked: {clicked}")

        if clicked:
            time.sleep(3)
            # Check if invitation modal appeared
            modal = page.evaluate("""() => {
                const modal = document.querySelector('.artdeco-modal, [role="dialog"]');
                if (!modal) return {found: false};
                const text = (modal.innerText || '').substring(0, 300);
                const btns = Array.from(modal.querySelectorAll('button')).map(b => ({
                    text: (b.innerText || '').trim().substring(0, 40),
                })).filter(b => b.text);
                const textareas = Array.from(modal.querySelectorAll('textarea'));
                return {found: true, text: text, buttons: btns, textarea_count: textareas.length};
            }""")
            print(f"\nInvitation modal: {json.dumps(modal, indent=2, ensure_ascii=False)}")

            if modal and modal.get("found"):
                print("\n=== MODAL FOUND! Testing 'Add a note' flow ===")
                # Try clicking "Add a note"
                note_clicked = _find_and_click_button(
                    page,
                    texts=["opmerking toevoegen", "add a note", "thêm ghi chú", "notiz hinzufügen", "ajouter une note"],
                    aria_keywords=["opmerking", "note", "ghi chú", "notiz"]
                )
                print(f"Add a note clicked: {note_clicked}")

                if note_clicked:
                    time.sleep(2)
                    # Check for textarea
                    textarea_info = page.evaluate("""() => {
                        const textareas = Array.from(document.querySelectorAll('textarea'));
                        return textareas.map(t => ({
                            id: t.id, name: t.name || '',
                            placeholder: t.placeholder || '',
                            visible: t.getBoundingClientRect().width > 0,
                        }));
                    }""")
                    print(f"Textareas: {json.dumps(textarea_info, indent=2, ensure_ascii=False)}")

                # DON'T SEND — close the modal
                print("\n=== CLOSING MODAL (NOT SENDING) ===")
                page.evaluate("""() => {
                    const btns = document.querySelectorAll('button');
                    for (const b of btns) {
                        const aria = (b.getAttribute('aria-label') || '').toLowerCase();
                        if (aria.includes('dismiss') || aria.includes('close') || aria.includes('sluiten') || aria.includes('đóng') || aria.includes('hủy')) {
                            b.click();
                            return;
                        }
                    }
                    // Try the X button
                    const x = document.querySelector('.artdeco-modal__dismiss');
                    if (x) x.click();
                }""")
                time.sleep(2)
            else:
                print("No modal appeared after clicking Connect")
        else:
            print("Connect was not clicked")
    else:
        print(f"Status is {status}, not testing Connect click")

    print("\n=== TEST COMPLETE ===")
    time.sleep(3)
    context.close()
