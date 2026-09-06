import sys
sys.path.insert(0, r"C:\Users\marvi\clawbuildr")

from linkedin_engine import _prepare_firefox_profile
from playwright.sync_api import sync_playwright
import time, json

profile_path = _prepare_firefox_profile()

with sync_playwright() as pw:
    context = pw.firefox.launch_persistent_context(
        profile_path, headless=False, viewport={"width": 1280, "height": 900}
    )
    page = context.new_page()
    page.set_default_timeout(30000)

    page.goto("https://www.linkedin.com/in/ddekleer/", wait_until="domcontentloaded", timeout=90000)
    time.sleep(10)

    # 1. Get ALL buttons with full details — including SVG icons
    all_buttons = page.evaluate("""() => {
        return Array.from(document.querySelectorAll('button')).filter(b => {
            const r = b.getBoundingClientRect();
            return r.width > 0 && r.height > 0;
        }).map(b => {
            const r = b.getBoundingClientRect();
            const svgs = b.querySelectorAll('svg');
            const svgIcons = Array.from(svgs).map(s => 
                s.getAttribute('data-test-icon') || s.getAttribute('data-test-id') || 
                s.querySelector('use')?.getAttribute('href') || ''
            ).filter(x => x);
            return {
                text: (b.innerText||'').trim().substring(0,60),
                aria: b.getAttribute('aria-label')||'',
                data_test: b.getAttribute('data-test-id')||b.getAttribute('data-test')||'',
                data_test_icon: b.getAttribute('data-test-icon')||'',
                svg_icons: svgIcons,
                cls: (b.className||'').substring(0,60),
                top: Math.round(r.top),
                left: Math.round(r.left),
                width: Math.round(r.width),
            };
        }).filter(b => b.text || b.aria || b.svg_icons.length > 0);
    }""")
    print("=== ALL BUTTONS WITH ICONS ===")
    for b in all_buttons:
        print(f"  top={b['top']} left={b['left']} w={b['width']} text='{b['text']}' aria='{b['aria']}' svg={b['svg_icons']} data_test={b['data_test']}")

    # 2. Find the overflow/more button by SVG icon type
    overflow_btns = page.evaluate("""() => {
        const btns = Array.from(document.querySelectorAll('button'));
        return btns.filter(b => {
            const svg = b.querySelector('svg');
            if (!svg) return false;
            const icon = svg.getAttribute('data-test-icon') || '';
            const useHref = svg.querySelector('use')?.getAttribute('href') || '';
            return icon.includes('overflow') || icon.includes('horizontal') || icon.includes('ellipsis') ||
                   useHref.includes('overflow') || useHref.includes('horizontal') || useHref.includes('ellipsis');
        }).map(b => {
            const r = b.getBoundingClientRect();
            return {
                aria: b.getAttribute('aria-label')||'',
                icon: b.querySelector('svg')?.getAttribute('data-test-icon')||'',
                useHref: b.querySelector('svg use')?.getAttribute('href')||'',
                top: Math.round(r.top),
                left: Math.round(r.left),
            };
        });
    }""")
    print("\n=== OVERFLOW BUTTONS (by SVG icon) ===")
    for b in overflow_btns:
        print(f"  top={b['top']} left={b['left']} aria='{b['aria']}' icon='{b['icon']}' use='{b['useHref']}'")

    # 3. Click the overflow button that's in the profile area (top > 200)
    profile_overflow = [b for b in overflow_btns if b['top'] > 200]
    if profile_overflow:
        btn = profile_overflow[0]
        print(f"\n=== Clicking overflow button at top={btn['top']} ===")
        # Find the actual button element and click it
        page.evaluate(f"""() => {{
            const btns = Array.from(document.querySelectorAll('button'));
            for (const b of btns) {{
                const svg = b.querySelector('svg');
                if (!svg) continue;
                const icon = svg.getAttribute('data-test-icon') || '';
                if (icon.includes('overflow') || icon.includes('horizontal')) {{
                    const r = b.getBoundingClientRect();
                    if (r.top > 200) {{
                        b.click();
                        return true;
                    }}
                }}
            }}
            return false;
        }}""")
        time.sleep(3)

        # Get ALL dropdown items
        dropdown_items = page.evaluate("""() => {
            const items = Array.from(document.querySelectorAll('[role="menuitem"], [role="menuitemradio"], .artdeco-dropdown__item, [class*="dropdown"]'));
            return items.filter(i => {
                const r = i.getBoundingClientRect();
                return r.width > 0 && r.height > 0;
            }).map(i => ({
                tag: i.tagName,
                text: (i.innerText||'').trim().substring(0,60),
                aria: i.getAttribute('aria-label')||'',
                role: i.getAttribute('role')||'',
                top: Math.round(i.getBoundingClientRect().top),
            }));
        }""")
        print(f"\n=== DROPDOWN ITEMS ({len(dropdown_items)}) ===")
        for d in dropdown_items:
            print(f"  tag={d['tag']} text='{d['text']}' aria='{d['aria']}' role='{d['role']}' top={d['top']}")

        # Also get ALL visible buttons after dropdown opened (the dropdown items might be buttons)
        post_dropdown_btns = page.evaluate("""() => {
            return Array.from(document.querySelectorAll('button, [role="menuitem"], [role="menuitemradio"]')).filter(el => {
                const r = el.getBoundingClientRect();
                return r.width > 0 && r.height > 0 && r.top > 400;
            }).map(el => ({
                tag: el.tagName,
                text: (el.innerText||'').trim().substring(0,60),
                aria: el.getAttribute('aria-label')||'',
                role: el.getAttribute('role')||'',
                top: Math.round(el.getBoundingClientRect().top),
            })).filter(el => el.text || el.aria);
        }""")
        print(f"\n=== POST-DROPDOWN BUTTONS/MENU ITEMS (top>400) ===")
        for b in post_dropdown_btns:
            print(f"  tag={b['tag']} text='{b['text']}' aria='{b['aria']}' role='{b['role']}' top={b['top']}")

    # 4. Get name from H2
    name = page.evaluate("""() => {
        const h2s = Array.from(document.querySelectorAll('h2'));
        for (const h of h2s) {
            const text = (h.innerText || '').trim();
            // Skip UI labels like "0 notifications", "About", etc.
            if (text.length > 3 && !text.includes('thông báo') && !text.includes('notification') &&
                !text.toLowerCase().includes('about') && !text.includes('giới thiệu') &&
                !text.includes('over') && text.split(' ').length >= 2) {
                return text;
            }
        }
        return '';
    }""")
    print(f"\n=== PROFILE NAME: '{name}' ===")

    print("\n=== DONE ===")
    time.sleep(5)
    context.close()
