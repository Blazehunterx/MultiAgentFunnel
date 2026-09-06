import sys
sys.path.insert(0, r"C:\Users\marvi\clawbuildr")
from linkedin_engine import _prepare_firefox_profile
from playwright.sync_api import sync_playwright
import time, json

profile_path = _prepare_firefox_profile()

with sync_playwright() as pw:
    context = pw.firefox.launch_persistent_context(profile_path, headless=False, viewport={"width": 1280, "height": 900})
    page = context.new_page()
    page.set_default_timeout(30000)

    page.goto("https://www.linkedin.com/in/jan-boone-499310180/", wait_until="domcontentloaded", timeout=90000)
    time.sleep(10)

    # Get ALL clickable elements in the profile action area (top < 600, left < 500)
    els = page.evaluate("""() => {
        const els = Array.from(document.querySelectorAll('button, a, [role="button"]'));
        const filtered = els.filter(el => {
            const r = el.getBoundingClientRect();
            return r.width > 0 && r.height > 0 && r.top < 600 && r.left < 500;
        });
        return filtered.map(el => {
            const r = el.getBoundingClientRect();
            return {
                tag: el.tagName,
                text: (el.innerText || '').trim().substring(0,60),
                aria: el.getAttribute('aria-label') || '',
                top: Math.round(r.top),
                left: Math.round(r.left),
                w: Math.round(r.width),
            };
        }).filter(el => el.text || el.aria);
    }""")
    print(f"=== PROFILE AREA ELEMENTS ({len(els)}) ===")
    for el in els:
        print(f"  tag={el['tag']} text='{el['text']}' aria='{el['aria']}' top={el['top']} left={el['left']} w={el['w']}")

    # Also check the person's name
    name = page.evaluate("""() => {
        const h2s = Array.from(document.querySelectorAll('h2'));
        for (const h of h2s) {
            const text = (h.innerText || '').trim();
            if (text.length > 3 && text.split(' ').length >= 2 &&
                !text.includes('thông báo') && !text.includes('notification') &&
                !text.toLowerCase().includes('about') && !text.includes('giới thiệu')) {
                return text;
            }
        }
        return '';
    }""")
    print(f"\nName: {name}")

    print("\n=== DONE ===")
    time.sleep(3)
    context.close()
