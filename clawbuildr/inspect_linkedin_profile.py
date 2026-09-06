import asyncio
import json
import os
import shutil

async def inspect_with_firefox_profile():
    """Launch Playwright Firefox using a copy of the user's real Firefox profile.
    This inherits the actual LinkedIn login session."""
    from playwright.async_api import async_playwright

    firefox_profile = r"C:\Users\marvi\AppData\Roaming\Mozilla\Firefox\Profiles\h1vl3oun.default-release"
    temp_profile = r"C:\Users\marvi\clawbuildr\data\firefox_profile_copy"

    print(f"Copying Firefox profile from {firefox_profile}...")
    if os.path.exists(temp_profile):
        shutil.rmtree(temp_profile, ignore_errors=True)

    # Copy only the essential files for cookies/session (don't copy the whole profile - it's huge)
    os.makedirs(temp_profile, exist_ok=True)
    essential_files = ["cookies.sqlite", "sessionstore-backups", "prefs.js", "logins.json", "key4.db", "cert9.db"]
    for f in essential_files:
        src = os.path.join(firefox_profile, f)
        if os.path.exists(src):
            if os.path.isfile(src):
                shutil.copy2(src, os.path.join(temp_profile, f))
                print(f"  Copied: {f}")
            elif os.path.isdir(src):
                shutil.copytree(src, os.path.join(temp_profile, f), dirs_exist_ok=True)
                print(f"  Copied dir: {f}")

    print("\nLaunching Firefox with copied profile...")
    async with async_playwright() as p:
        browser = await p.firefox.launch_persistent_context(
            temp_profile,
            headless=True,
            viewport={"width": 1280, "height": 800},
        )

        page = await browser.new_page()
        results = {}

        # 1. Check login status
        print("\n=== Checking login status ===")
        await page.goto("https://www.linkedin.com/feed/", wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(3000)
        results["feed_url"] = page.url
        is_logged_in = "login" not in page.url and "authwall" not in page.url
        results["logged_in"] = is_logged_in
        print(f"Feed URL: {page.url}")
        print(f"Logged in: {is_logged_in}")

        if not is_logged_in:
            print("NOT logged in! Trying to extract cookies from the browser context...")
            cookies = await browser.cookies("https://www.linkedin.com")
            print(f"Browser has {len(cookies)} linkedin cookies")
            # Save these cookies for future Chromium use
            cookie_list = []
            for c in cookies:
                cookie_list.append({
                    "name": c["name"],
                    "value": c["value"],
                    "domain": c["domain"],
                    "path": c["path"],
                    "secure": c["secure"],
                    "httpOnly": c["httpOnly"],
                    "sameSite": c.get("sameSite", "Lax"),
                    "expires": c.get("expires", -1),
                })
            with open(r"C:\Users\marvi\clawbuildr\data\linkedin_cookies.json", "w") as f:
                json.dump(cookie_list, f, indent=2)
            print(f"Saved {len(cookie_list)} cookies from browser context")
            li_at = [c for c in cookie_list if c["name"] == "li_at"]
            if li_at:
                print(f"li_at value length: {len(li_at[0]['value'])}")
            await browser.close()
            with open(r"C:\Users\marvi\clawbuildr\data\li_logged_in_dom.json", "w", encoding="utf-8") as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            return

        # LOGGED IN — inspect the DOM!
        print("\n=== SUCCESS! Logged in. Inspecting profile DOM ===")

        # Save cookies from the working session
        cookies = await browser.cookies("https://www.linkedin.com")
        cookie_list = []
        for c in cookies:
            cookie_list.append({
                "name": c["name"], "value": c["value"], "domain": c["domain"],
                "path": c["path"], "secure": c["secure"], "httpOnly": c["httpOnly"],
                "sameSite": c.get("sameSite", "Lax"), "expires": c.get("expires", -1),
            })
        with open(r"C:\Users\marvi\clawbuildr\data\linkedin_cookies.json", "w") as f:
            json.dump(cookie_list, f, indent=2)
        print(f"Saved {len(cookie_list)} working cookies")

        # 2. Inspect a profile page
        await page.goto("https://www.linkedin.com/in/williamhgates/", wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(5000)
        results["profile_url"] = page.url
        results["profile_h1"] = await page.evaluate("() => document.querySelector('h1')?.innerText?.trim() || ''")
        print(f"Profile H1: {results['profile_h1']}")

        # Get ALL visible buttons near the top of the profile
        results["profile_buttons"] = await page.evaluate("""() => {
            return Array.from(document.querySelectorAll('button')).filter(b => {
                const r = b.getBoundingClientRect();
                return r.top < 500 && r.width > 0 && r.height > 0;
            }).map(b => ({
                text: (b.innerText||'').trim().substring(0,60),
                aria: b.getAttribute('aria-label') || '',
                cls: (b.className||'').substring(0,120),
                id: b.id || '',
                data_test: b.getAttribute('data-test-id') || b.getAttribute('data-test') || '',
                rect_top: Math.round(b.getBoundingClientRect().top),
            }));
        }""")

        # Check old selectors
        results["old_selectors"] = await page.evaluate("""() => {
            const sels = ['div.ph5','div.pv-top-card--v2-ctas','section.pv-top-card',
                '.pvs-profile-actions','div.pv-top-card','.msg-overlay-list-bubble',
                'textarea#custom-message','.msg-form__contenteditable','.msg-convo-wrapper',
                '.msg-form__send-button','.msg-overlay-conversation-bubble'];
            return sels.map(s => ({sel: s, found: !!document.querySelector(s)}));
        }""")

        # Find action button container
        results["action_containers"] = await page.evaluate("""() => {
            const allDivs = Array.from(document.querySelectorAll('div'));
            return allDivs.filter(d => {
                const btns = d.querySelectorAll('button');
                if (btns.length < 1 || btns.length > 8) return false;
                const r = d.getBoundingClientRect();
                return r.top < 350 && r.width > 200 && r.width < 900;
            }).slice(0, 5).map(d => ({
                cls: (d.className||'').substring(0,150),
                btnCount: d.querySelectorAll('button').length,
                btnTexts: Array.from(d.querySelectorAll('button')).map(b => (b.innerText||'').trim().substring(0,30)).filter(t=>t),
                btnArias: Array.from(d.querySelectorAll('button')).map(b => b.getAttribute('aria-label')||'').filter(a=>a).slice(0,5)
            }));
        }""")

        # Get top section HTML snippet
        results["top_html"] = await page.evaluate("""() => {
            const sections = document.querySelectorAll('section');
            for (const s of sections) {
                const r = s.getBoundingClientRect();
                if (r.top < 200 && r.top >= 0) {
                    return s.outerHTML.substring(0, 4000);
                }
            }
            return 'no section found';
        }""")

        # 3. Inspect search results page
        print("\n=== Inspecting search results page ===")
        await page.goto("https://www.linkedin.com/search/results/people/?keywords=software%20engineer", wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(5000)
        results["search_url"] = page.url

        results["search_result_links"] = await page.evaluate("""() => {
            const links = Array.from(document.querySelectorAll('a[href*="/in/"]'));
            return links.slice(0, 5).map(l => ({
                href: l.getAttribute('href'),
                text: (l.innerText||'').trim().substring(0,60),
            }));
        }""")

        results["search_result_buttons"] = await page.evaluate("""() => {
            return Array.from(document.querySelectorAll('button')).filter(b => {
                const txt = (b.innerText||'').toLowerCase();
                return txt.includes('connect') || txt.includes('message') || txt.includes('verbinden') || txt.includes('bericht');
            }).slice(0, 10).map(b => ({
                text: (b.innerText||'').trim().substring(0,40),
                aria: b.getAttribute('aria-label')||'',
                cls: (b.className||'').substring(0,100),
            }));
        }""")

        results["search_containers"] = await page.evaluate("""() => {
            const sels = ['.search-results-container','.search-result__wrapper',
                '.reusable-search__result-container','li.reusable-search__result-item',
                '.entity-result','.search-result','.artdeco-list__item'];
            return sels.map(s => ({sel: s, count: document.querySelectorAll(s).length}));
        }""")

        # Get search result item HTML snippet
        results["search_item_html"] = await page.evaluate("""() => {
            const item = document.querySelector('.reusable-search__result-container, .entity-result, li.search-result, [data-chameleon-result-card]');
            return item ? item.outerHTML.substring(0, 3000) : 'no item found';
        }""")

        await browser.close()

    out_path = r"C:\Users\marvi\clawbuildr\data\li_logged_in_dom.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\nDONE — DOM inspection saved to {out_path}")


if __name__ == "__main__":
    asyncio.run(inspect_with_firefox_profile())
