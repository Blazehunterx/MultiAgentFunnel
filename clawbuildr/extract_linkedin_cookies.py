import browser_cookie3
import json
import os
import asyncio
from playwright.async_api import async_playwright

COOKIES_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "linkedin_cookies.json")

def extract_firefox_cookies():
    """Extract LinkedIn cookies from Firefox and save in Playwright format."""
    print("Extracting cookies from Firefox...")
    try:
        cj = browser_cookie3.firefox(domain_name="linkedin.com")
    except Exception as e:
        print(f"Firefox extraction failed: {e}")
        try:
            cj = browser_cookie3.firefox(domain_name=".linkedin.com")
        except Exception as e2:
            print(f"Second attempt failed: {e2}")
            return []

    cookies = []
    for c in cj:
        # browser_cookie3 doesn't always expose httpOnly correctly via _rest.
        # For LinkedIn, session cookies (li_at, liap, bscookie, JSESSIONID) are always httpOnly.
        session_cookies = ("li_at", "liap", "bscookie", "JSESSIONID", "li_rm")
        is_session = c.name in session_cookies
        cookie = {
            "name": c.name,
            "value": c.value,
            "domain": c.domain if c.domain.startswith(".") else "." + c.domain.lstrip("."),
            "path": c.path if c.path else "/",
            "secure": bool(c.secure) or is_session,
            "httpOnly": bool(getattr(c, "_rest", {}).get("HttpOnly", False)) or is_session,
            "sameSite": "None" if bool(c.secure) else "Lax",
        }
        if c.expires and isinstance(c.expires, (int, float)) and c.expires > 0:
            expires = int(c.expires)
            if expires > 100000000000:
                expires = expires // 1000
            cookie["expires"] = expires
        else:
            cookie["expires"] = -1
        cookies.append(cookie)

    # Filter to only linkedin.com cookies
    li_cookies = [c for c in cookies if "linkedin" in c.get("domain", "")]
    print(f"Found {len(li_cookies)} LinkedIn cookies from Firefox")

    # Check for critical session cookies
    critical = ["li_at", "liap", "JSESSIONID", "bscookie", "li_rm"]
    found = [c["name"] for c in li_cookies if c["name"] in critical]
    print(f"Critical session cookies found: {found}")

    os.makedirs(os.path.dirname(COOKIES_PATH), exist_ok=True)
    with open(COOKIES_PATH, "w") as f:
        json.dump(li_cookies, f, indent=2)
    print(f"Saved {len(li_cookies)} cookies to {COOKIES_PATH}")

    return li_cookies


async def inspect_logged_in_dom(cookies):
    """Use the extracted cookies to inspect LinkedIn's logged-in DOM structure."""
    if not cookies:
        print("No cookies to inspect with!")
        return

    async with async_playwright() as p:
        browser = await p.firefox.launch(headless=True)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
        )

        await context.add_cookies(cookies)
        page = await browser.new_page()
        results = {}

        # 1. Check if we're logged in by going to feed
        print("\n=== Checking login status ===")
        await page.goto("https://www.linkedin.com/feed/", wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(3000)
        results["feed_url"] = page.url
        print(f"Feed URL: {page.url}")

        is_logged_in = "login" not in page.url and "authwall" not in page.url
        results["logged_in"] = is_logged_in
        print(f"Logged in: {is_logged_in}")

        if not is_logged_in:
            print("NOT logged in! Cookies may be expired.")
            await browser.close()
            with open(os.path.join(os.path.dirname(COOKIES_PATH), "li_logged_in_dom.json"), "w", encoding="utf-8") as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            return

        # 2. Inspect a profile page
        print("\n=== Inspecting profile page DOM ===")
        await page.goto("https://www.linkedin.com/in/williamhgates/", wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(5000)

        results["profile_url"] = page.url
        results["profile_h1"] = await page.evaluate("() => document.querySelector('h1')?.innerText?.trim() || ''")
        print(f"Profile H1: {results['profile_h1']}")

        # Get ALL buttons in the top card / action area
        results["all_buttons"] = await page.evaluate("""() => {
            return Array.from(document.querySelectorAll('button')).filter(b => {
                const r = b.getBoundingClientRect();
                return r.top < 400 && (b.innerText.trim() || b.getAttribute('aria-label'));
            }).map(b => ({
                text: (b.innerText||'').trim().substring(0,50),
                aria: b.getAttribute('aria-label') || '',
                cls: (b.className||'').substring(0,100),
                id: b.id || '',
                data_test: b.getAttribute('data-test-id') || b.getAttribute('data-test') || '',
                rect: {top: Math.round(b.getBoundingClientRect().top), left: Math.round(b.getBoundingClientRect().left), w: Math.round(b.getBoundingClientRect().width)}
            }));
        }""")

        # Check old selectors
        results["old_selectors"] = await page.evaluate("""() => {
            const sels = [
                'div.ph5', 'div.pv-top-card--v2-ctas', 'section.pv-top-card',
                '.pvs-profile-actions', 'div.pv-top-card', '.top-card-layout',
                '.msg-overlay-list-bubble', '.msg-overlay-conversation-bubble',
                'textarea#custom-message', '.msg-form__contenteditable',
                '.msg-convo-wrapper', '.msg-form__send-button'
            ];
            return sels.map(s => ({sel: s, found: !!document.querySelector(s)}));
        }""")

        # Find the action button container (where Message/Connect/Follow buttons live)
        results["action_container_candidates"] = await page.evaluate("""() => {
            // Look for containers that have action buttons
            const allDivs = Array.from(document.querySelectorAll('div'));
            const candidates = allDivs.filter(d => {
                const btns = d.querySelectorAll('button');
                if (btns.length < 1 || btns.length > 6) return false;
                const r = d.getBoundingClientRect();
                if (r.top > 400 || r.width < 200) return false;
                return true;
            }).slice(0, 5);
            return candidates.map(d => ({
                cls: (d.className||'').substring(0,120),
                btnCount: d.querySelectorAll('button').length,
                btnTexts: Array.from(d.querySelectorAll('button')).map(b => (b.innerText||'').trim().substring(0,30)).filter(t=>t)
            }));
        }""")

        # Get the top section structure
        results["top_section_html"] = await page.evaluate("""() => {
            // Get the first 2000 chars of HTML from the main content area
            const main = document.querySelector('main') || document.querySelector('#main-content') || document.body;
            return main ? main.innerHTML.substring(0, 3000) : 'no main found';
        }""")

        # 3. Inspect search results page
        print("\n=== Inspecting search results page ===")
        await page.goto("https://www.linkedin.com/search/results/people/?keywords=software%20engineer", wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(5000)
        results["search_url"] = page.url

        # Get search result structure
        results["search_result_links"] = await page.evaluate("""() => {
            const links = Array.from(document.querySelectorAll('a[href*="/in/"]'));
            return links.slice(0, 5).map(l => ({
                href: l.getAttribute('href'),
                text: (l.innerText||'').trim().substring(0,60),
                cls: (l.className||'').substring(0,80)
            }));
        }""")

        # Get search result action buttons (Connect, Message, etc.)
        results["search_result_buttons"] = await page.evaluate("""() => {
            return Array.from(document.querySelectorAll('button')).filter(b => {
                const txt = (b.innerText||'').toLowerCase();
                return txt.includes('connect') || txt.includes('message') || txt.includes('verbinden') || txt.includes('bericht');
            }).slice(0, 10).map(b => ({
                text: (b.innerText||'').trim().substring(0,40),
                aria: b.getAttribute('aria-label')||'',
                cls: (b.className||'').substring(0,100)
            }));
        }""")

        # Check for "People you may know" or similar result list containers
        results["search_containers"] = await page.evaluate("""() => {
            const sels = [
                '.search-results-container', '.search-result__wrapper',
                '.reusable-search__result-container', 'li.reusable-search__result-item',
                '.entity-result', '.search-result'
            ];
            return sels.map(s => ({sel: s, count: document.querySelectorAll(s).length}));
        }""")

        await browser.close()

    out_path = os.path.join(os.path.dirname(COOKIES_PATH), "li_logged_in_dom.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\nDONE — DOM inspection saved to {out_path}")


if __name__ == "__main__":
    cookies = extract_firefox_cookies()
    asyncio.run(inspect_logged_in_dom(cookies))
