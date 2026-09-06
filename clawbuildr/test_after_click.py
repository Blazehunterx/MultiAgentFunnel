import sys
sys.path.insert(0, r"C:\Users\marvi\clawbuildr")

from linkedin_engine import (
    _prepare_firefox_profile, _check_if_connected,
    _find_and_click_button
)
from playwright.sync_api import sync_playwright
import time, json

profile_path = _prepare_firefox_profile()

with sync_playwright() as pw:
    context = pw.firefox.launch_persistent_context(profile_path, headless=False, viewport={"width": 1280, "height": 900})
    page = context.new_page()
    page.set_default_timeout(30000)

    # Go to a fresh profile we're NOT connected to
    print("=== Navigate to profile ===")
    page.goto("https://www.linkedin.com/in/jan-boone-499310180/", wait_until="domcontentloaded", timeout=90000)
    time.sleep(10)

    status = _check_if_connected(page)
    print(f"Status: {status}")

    # Take screenshot of the action area BEFORE clicking
    page.screenshot(path=r"C:\Users\marvi\clawbuildr\data\before_connect.png")

    # Get all elements in the action area before clicking
    before = page.evaluate("""() => {
        const els = Array.from(document.querySelectorAll('a, button, [role="button"]'));
        return els.filter(el => {
            const r = el.getBoundingClientRect();
            return r.width > 0 && r.height > 0 && r.top > 480 && r.top < 560 && r.left < 500;
        }).map(el => {
            const r = el.getBoundingClientRect();
            return {
                tag: el.tagName,
                text: (el.innerText || '').trim().substring(0,60),
                aria: el.getAttribute('aria-label') || '',
                top: Math.round(r.top), left: Math.round(r.left), w: Math.round(r.width),
                href: el.getAttribute('href') || '',
            };
        });
    }""")
    print(f"\n=== BEFORE CLICK ({len(before)} elements) ===")
    for el in before:
        print(f"  {el['tag']} text='{el['text']}' aria='{el['aria']}' top={el['top']} left={el['left']} href='{el['href']}'")

    # Click Connect
    print("\n=== Clicking Connect ===")
    connect_texts = ["verbinden", "connect", "kết nối", "se connecter", "conectar"]
    connect_arias = ["verbinden", "connect", "kết nối", "mời", "invite", "se connecter", "conectar"]
    clicked = _find_and_click_button(page, texts=connect_texts, aria_keywords=connect_arias, profile_area_only=True)
    print(f"Clicked: {clicked}")

    # Wait and check what happened
    time.sleep(2)
    page.screenshot(path=r"C:\Users\marvi\clawbuildr\data\after_connect.png")

    # Check current URL
    print(f"URL after click: {page.url}")

    # Check for ANY new modal, dialog, or popup
    after = page.evaluate("""() => {
        const results = {};
        // Check for modals
        const modals = document.querySelectorAll('.artdeco-modal, [role="dialog"], [role="alertdialog"]');
        results.modals = Array.from(modals).map(m => ({
            visible: m.getBoundingClientRect().width > 0,
            text: (m.innerText || '').substring(0, 300),
            aria: m.getAttribute('aria-label') || '',
        }));
        // Check for any newly visible fixed-position elements
        results.fixed_els = Array.from(document.querySelectorAll('[style*="position: fixed"], [style*="position:fixed"]')).map(e => ({
            cls: (e.className || '').substring(0, 60),
            text: (e.innerText || '').substring(0, 200),
            visible: e.getBoundingClientRect().width > 0,
        })).filter(e => e.visible);
        // Check all buttons that are now visible
        results.new_btns = Array.from(document.querySelectorAll('button')).filter(b => {
            const r = b.getBoundingClientRect();
            return r.width > 0 && r.height > 0 && r.top > 200 && r.top < 800;
        }).map(b => ({
            text: (b.innerText || '').trim().substring(0,50),
            aria: b.getAttribute('aria-label') || '',
            top: Math.round(b.getBoundingClientRect().top),
        })).filter(b => b.text || b.aria);
        // Check for textareas
        results.textareas = Array.from(document.querySelectorAll('textarea')).map(t => ({
            id: t.id, name: t.name || '', placeholder: t.placeholder || '',
            visible: t.getBoundingClientRect().width > 0,
        }));
        return results;
    }""")
    print(f"\n=== AFTER CLICK ===")
    print(f"Modals: {json.dumps(after['modals'], indent=2, ensure_ascii=False)}")
    print(f"Fixed elements: {json.dumps(after['fixed_els'], indent=2, ensure_ascii=False)}")
    print(f"New buttons: {json.dumps(after['new_btns'], indent=2, ensure_ascii=False)}")
    print(f"Textareas: {json.dumps(after['textareas'], indent=2, ensure_ascii=False)}")

    # Check if the action area changed (e.g., "Pending" appeared)
    after_action = page.evaluate("""() => {
        const els = Array.from(document.querySelectorAll('a, button, [role="button"]'));
        return els.filter(el => {
            const r = el.getBoundingClientRect();
            return r.width > 0 && r.height > 0 && r.top > 480 && r.top < 560 && r.left < 500;
        }).map(el => {
            const r = el.getBoundingClientRect();
            return {
                tag: el.tagName,
                text: (el.innerText || '').trim().substring(0,60),
                aria: el.getAttribute('aria-label') || '',
                top: Math.round(r.top), left: Math.round(r.left),
            };
        });
    }""")
    print(f"\n=== ACTION AREA AFTER CLICK ===")
    for el in after_action:
        print(f"  {el['tag']} text='{el['text']}' aria='{el['aria']}' top={el['top']} left={el['left']}")

    print("\n=== DONE ===")
    time.sleep(5)
    context.close()
