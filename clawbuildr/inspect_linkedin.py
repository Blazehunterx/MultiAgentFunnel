import asyncio
import json
from playwright.async_api import async_playwright

async def inspect():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        results = {}

        # 1. Login page
        await page.goto("https://www.linkedin.com/login", wait_until="domcontentloaded", timeout=20000)
        results["login_url"] = page.url
        results["login_inputs"] = await page.evaluate("""() => Array.from(document.querySelectorAll('input')).map(i => ({
            type: i.type, id: i.id, name: i.name, placeholder: i.placeholder || ''
        }))""")
        results["login_buttons"] = await page.evaluate("""() => Array.from(document.querySelectorAll('button')).map(b => ({
            text: (b.innerText || '').trim().substring(0,50), aria: b.getAttribute('aria-label') || '',
            type: b.type||'', id: b.id||''
        }))""")

        # 2. Search page (logged out — will redirect to login)
        await page.goto("https://www.linkedin.com/search/results/people/?keywords=test", wait_until="domcontentloaded", timeout=20000)
        results["search_url_logged_out"] = page.url

        # 3. Check a profile page (logged out)
        try:
            await page.goto("https://www.linkedin.com/in/williamhgates/", wait_until="domcontentloaded", timeout=20000)
            await page.wait_for_timeout(3000)
            results["profile_url_logged_out"] = page.url
            # Get page title and any h1
            results["profile_title"] = await page.title()
            results["profile_h1s"] = await page.evaluate("""() => {
                try { return Array.from(document.querySelectorAll('h1')).map(h => (h.innerText||'').trim().substring(0,60)); }
                catch(e) { return []; }
            }""")
            # Get all buttons visible
            results["profile_buttons"] = await page.evaluate("""() => {
                try { return Array.from(document.querySelectorAll('button')).filter(b => b.innerText.trim() || b.getAttribute('aria-label')).slice(0,20).map(b => ({
                    text: (b.innerText||'').trim().substring(0,40), aria: b.getAttribute('aria-label')||'',
                    cls: (b.className||'').substring(0,80)
                })); }
                catch(e) { return []; }
            }""")
            # Get top card / profile action area selectors
            results["profile_containers"] = await page.evaluate("""() => {
                try {
                    const sels = ['div.ph5', 'div.pv-top-card--v2-ctas', 'section.pv-top-card', '.pvs-profile-actions',
                                  'div.pv-top-card', '.top-card-layout', '.profile-top-card'];
                    return sels.map(s => ({sel: s, found: !!document.querySelector(s)}));
                } catch(e) { return []; }
            }""")
        except Exception as e:
            results["profile_error"] = str(e)[:100]

        await browser.close()

        with open(r"C:\Users\marvi\clawbuildr\data\li_dom_inspection.json", "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print("DONE — saved to data/li_dom_inspection.json")

asyncio.run(inspect())
