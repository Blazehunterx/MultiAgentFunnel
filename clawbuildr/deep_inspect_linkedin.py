import asyncio
import json
import os
import shutil

async def deep_inspect():
    from playwright.async_api import async_playwright

    firefox_profile = r"C:\Users\marvi\AppData\Roaming\Mozilla\Firefox\Profiles\h1vl3oun.default-release"
    temp_profile = r"C:\Users\marvi\clawbuildr\data\firefox_profile_copy"

    async with async_playwright() as p:
        browser = await p.firefox.launch_persistent_context(
            temp_profile, headless=True, viewport={"width": 1280, "height": 900},
        )
        page = await browser.new_page()
        results = {}

        # Inspect a profile where we're NOT connected (Bill Gates - we can only Follow, not Connect)
        # Let's find a random person to see Connect button
        # First go to search and click into a profile
        print("=== Going to search for a person to find Connect button ===")
        await page.goto("https://www.linkedin.com/search/results/people/?keywords=marketing%20manager%20netherlands", wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(8000)

        # Get ALL buttons on search page
        results["search_all_buttons"] = await page.evaluate("""() => {
            return Array.from(document.querySelectorAll('button')).filter(b => {
                const r = b.getBoundingClientRect();
                return r.width > 0 && r.height > 0;
            }).map(b => ({
                text: (b.innerText||'').trim().substring(0,60),
                aria: b.getAttribute('aria-label')||'',
                data_test: b.getAttribute('data-test-id')||b.getAttribute('data-test')||'',
                rect_top: Math.round(b.getBoundingClientRect().top),
                rect_left: Math.round(b.getBoundingClientRect().left),
            })).filter(b => b.text || b.aria);
        }""")

        # Get all links to profiles
        results["search_profile_links"] = await page.evaluate("""() => {
            return Array.from(document.querySelectorAll('a[href*="/in/"]')).slice(0, 8).map(l => ({
                href: l.getAttribute('href').split('?')[0],
                text: (l.innerText||'').trim().substring(0,80),
                aria: l.getAttribute('aria-label')||'',
            }));
        }""")

        # Now click into the first profile to see what buttons exist there
        first_profile = None
        links = await page.evaluate("""() => {
            const links = Array.from(document.querySelectorAll('a[href*="/in/"]'));
            for (const l of links) {
                const href = l.getAttribute('href');
                if (href && href.includes('/in/')) return href.split('?')[0];
            }
            return null;
        }""")

        if first_profile or links:
            profile_url = first_profile or links
            if not profile_url.startswith("https://"):
                profile_url = "https://www.linkedin.com" + profile_url
            print(f"=== Inspecting profile: {profile_url} ===")
            await page.goto(profile_url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(8000)
            results["profile_url"] = page.url

            # Get ALL buttons on profile page
            results["profile_all_buttons"] = await page.evaluate("""() => {
                return Array.from(document.querySelectorAll('button')).filter(b => {
                    const r = b.getBoundingClientRect();
                    return r.width > 0 && r.height > 0 && r.top < 600;
                }).map(b => ({
                    text: (b.innerText||'').trim().substring(0,80),
                    aria: b.getAttribute('aria-label')||'',
                    cls: (b.className||'').substring(0,150),
                    data_test: b.getAttribute('data-test-id')||b.getAttribute('data-test')||'',
                    data_component: b.getAttribute('data-component-type')||'',
                    rect_top: Math.round(b.getBoundingClientRect().top),
                    rect_left: Math.round(b.getBoundingClientRect().left),
                })).filter(b => b.text || b.aria);
            }""")

            # Get the profile name
            results["profile_name"] = await page.evaluate("""() => {
                // Try h1
                const h1 = document.querySelector('h1');
                if (h1 && h1.innerText.trim()) return h1.innerText.trim();
                // Try aria-label on main section
                const main = document.querySelector('main');
                if (main) {
                    const h = main.querySelector('h1, h2, [class*="name"]');
                    if (h) return h.innerText.trim();
                }
                return '';
            }""")

            # Look for the top card section by aria-label
            results["profile_sections"] = await page.evaluate("""() => {
                return Array.from(document.querySelectorAll('section')).filter(s => {
                    const r = s.getBoundingClientRect();
                    return r.top < 400 && r.top >= 0 && r.width > 200;
                }).slice(0, 5).map(s => ({
                    aria: s.getAttribute('aria-label')||'',
                    cls: (s.className||'').substring(0,120),
                    btnCount: s.querySelectorAll('button').length,
                    btnTexts: Array.from(s.querySelectorAll('button')).map(b => (b.innerText||'').trim().substring(0,30)).filter(t=>t).slice(0,6),
                    btnArias: Array.from(s.querySelectorAll('button')).map(b => b.getAttribute('aria-label')||'').filter(a=>a).slice(0,6),
                }));
            }""")

            # Get data-testid attributes
            results["data_testids"] = await page.evaluate("""() => {
                return Array.from(document.querySelectorAll('[data-testid]')).slice(0, 20).map(e => ({
                    tag: e.tagName, testid: e.getAttribute('data-testid'),
                    text: (e.innerText||'').trim().substring(0,40),
                    cls: (e.className||'').substring(0,60),
                }));
            }""")

            # Check for the "More" / overflow menu button
            results["more_buttons"] = await page.evaluate("""() => {
                return Array.from(document.querySelectorAll('button')).filter(b => {
                    const aria = (b.getAttribute('aria-label')||'').toLowerCase();
                    const text = (b.innerText||'').toLowerCase();
                    return aria.includes('meer') || aria.includes('more') || text.includes('meer') || text.includes('more') ||
                           aria.includes('actie') || aria.includes('action');
                }).map(b => ({
                    text: (b.innerText||'').trim(), aria: b.getAttribute('aria-label')||'',
                    cls: (b.className||'').substring(0,80),
                    rect_top: Math.round(b.getBoundingClientRect().top),
                }));
            }""")

        await browser.close()

    out_path = r"C:\Users\marvi\clawbuildr\data\li_deep_inspection.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\nDONE — saved to {out_path}")

asyncio.run(deep_inspect())
