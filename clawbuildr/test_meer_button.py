import sys
sys.path.insert(0, r"C:\Users\marvi\clawbuildr")

from linkedin_engine import _prepare_firefox_profile, _detect_auth_wall, _find_profile_more_button
from playwright.sync_api import sync_playwright
import time, json

profile_path = _prepare_firefox_profile()

with sync_playwright() as pw:
    context = pw.firefox.launch_persistent_context(
        profile_path, headless=False, viewport={"width": 1280, "height": 900}
    )
    page = context.new_page()
    page.set_default_timeout(30000)

    # Go directly to a profile we're NOT connected to
    print("=== Navigating to profile ===")
    page.goto("https://www.linkedin.com/in/ddekleer/", wait_until="domcontentloaded", timeout=90000)

    # Wait for full SPA hydration
    print("Waiting 10s for SPA to fully load...")
    time.sleep(10)

    # Try multiple ways to get the name
    name_h1 = page.evaluate("() => document.querySelector('h1')?.innerText?.trim() || ''")
    name_h2 = page.evaluate("() => { const h2s = Array.from(document.querySelectorAll('h2')); return h2s.map(h => h.innerText.trim()).filter(t => t.length > 2).slice(0,3); }")
    name_aria = page.evaluate("() => { const el = document.querySelector('[data-testid=\"lazy-column\"]'); return el ? el.getAttribute('aria-label') || '' : ''; }")
    name_section = page.evaluate("""() => {
        const sections = document.querySelectorAll('section[aria-label]');
        for (const s of sections) {
            const aria = s.getAttribute('aria-label');
            if (aria && aria.length > 3 && !aria.includes('content')) return aria;
        }
        return '';
    }""")
    print(f"H1: '{name_h1}'")
    print(f"H2s: {name_h2}")
    print(f"Lazy column aria: '{name_aria}'")
    print(f"Section aria: '{name_section}'")

    # Try to find name from any visible text near top
    name_from_text = page.evaluate("""() => {
        // Look for the profile name in the top card area
        const els = document.querySelectorAll('h1, h2, h3, [class*="name"], [class*="title"]');
        const results = [];
        for (const el of els) {
            const r = el.getBoundingClientRect();
            const text = (el.innerText || '').trim();
            if (text.length > 2 && r.top < 300 && r.top > 50) {
                results.push({tag: el.tagName, text: text.substring(0,60), top: Math.round(r.top)});
            }
        }
        return results;
    }""")
    print(f"Name candidates: {json.dumps(name_from_text, indent=2)}")

    # Click the Meer button and inspect the dropdown
    more = _find_profile_more_button(page)
    print(f"\nMore button: {more}")

    if more:
        print("Clicking Meer button...")
        page.mouse.click(more["x"], more["y"])
        time.sleep(3)

        # Get all visible items in the dropdown
        dropdown = page.evaluate("""() => {
            const items = Array.from(document.querySelectorAll('button, [role="menuitem"], [role="menuitemradio"], a, div'));
            return items.filter(i => {
                const r = i.getBoundingClientRect();
                const text = (i.innerText || '').trim().toLowerCase();
                const aria = (i.getAttribute('aria-label') || '').toLowerCase();
                return r.width > 0 && r.height > 0 && (text || aria) &&
                       (text.includes('verbinden') || text.includes('connect') || text.includes('bericht') ||
                        text.includes('message') || text.includes('volgen') || text.includes('follow') ||
                        text.includes('profiel') || text.includes('profile') || text.includes('deel') ||
                        text.includes('share') || text.includes('rapport') || text.includes('report') ||
                        aria.includes('verbinden') || aria.includes('connect') || aria.includes('bericht'));
            }).map(i => ({
                tag: i.tagName,
                text: (i.innerText||'').trim().substring(0,50),
                aria: i.getAttribute('aria-label')||'',
                role: i.getAttribute('role')||'',
                top: Math.round(i.getBoundingClientRect().top),
            }));
        }""")
        print(f"\nDropdown items:")
        for d in dropdown:
            print(f"  tag={d['tag']} text='{d['text']}' aria='{d['aria']}' role='{d['role']}' top={d['top']}")

        # Get ALL dropdown items (not just filtered)
        all_dropdown = page.evaluate("""() => {
            const items = Array.from(document.querySelectorAll('[role="menuitem"], [role="menuitemradio"], .artdeco-dropdown__item, .dropdown-options__item'));
            return items.filter(i => {
                const r = i.getBoundingClientRect();
                return r.width > 0 && r.height > 0;
            }).map(i => ({
                tag: i.tagName,
                text: (i.innerText||'').trim().substring(0,50),
                aria: i.getAttribute('aria-label')||'',
                role: i.getAttribute('role')||'',
                top: Math.round(i.getBoundingClientRect().top),
            }));
        }""")
        print(f"\nAll role=menuitem items:")
        for d in all_dropdown:
            print(f"  tag={d['tag']} text='{d['text']}' aria='{d['aria']}' role='{d['role']}' top={d['top']}")

    print("\n=== DONE ===")
    time.sleep(5)
    context.close()
