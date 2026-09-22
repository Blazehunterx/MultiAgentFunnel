"""
Human-like LinkedIn profile reader with scrolling and enrichment.
Opens a profile in Firefox, scrolls through sections, and extracts structured data.
"""
import os
import sys
import json
import time
import random

# Ensure UTF-8 output on Windows console
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLAWBUILDR_DIR = os.path.join(BASE_DIR, "clawbuildr")
if CLAWBUILDR_DIR not in sys.path:
    sys.path.insert(0, CLAWBUILDR_DIR)

from linkedin_engine import get_firefox_driver, _detect_auth_wall


def _safe_evaluate(driver, js, default=None):
    """Execute JS via Selenium directly, avoiding the Playwright-style wrapper issues."""
    try:
        return driver.execute_script(js)
    except Exception as e:
        _log(f"JS eval error: {str(e)[:80]}")
        return default


def _human_scroll_selenium(driver, min_scrolls=2, max_scrolls=6):
    """Scroll down the page like a human, using Selenium directly."""
    scrolls = random.randint(min_scrolls, max_scrolls)
    for _ in range(scrolls):
        distance = random.randint(400, 900)
        driver.execute_script(f"window.scrollBy({{top: {distance}, left: 0, behavior: 'smooth'}});")
        time.sleep(random.uniform(0.8, 2.5))


def _scroll_to_bottom(driver, max_scrolls=15):
    """Scroll to bottom progressively to load all lazy sections."""
    last_height = driver.execute_script("return document.body.scrollHeight")
    for _ in range(max_scrolls):
        driver.execute_script("window.scrollTo({top: document.body.scrollHeight, left: 0, behavior: 'smooth'});")
        time.sleep(random.uniform(1.5, 3.0))
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            # Try one more small scroll to trigger any remaining lazy loaders
            driver.execute_script("window.scrollBy({top: 500, left: 0, behavior: 'smooth'});")
            time.sleep(1.5)
            newer_height = driver.execute_script("return document.body.scrollHeight")
            if newer_height == last_height:
                break
            last_height = newer_height
        else:
            last_height = new_height


def read_linkedin_profile(profile_url: str):
    """Read a LinkedIn profile with human-like scrolling. Returns structured data."""
    result = {
        "profile_url": profile_url,
        "name": "",
        "headline": "",
        "location": "",
        "about": "",
        "current_role": "",
        "current_company": "",
        "tenure": "",
        "experience": [],
        "education": [],
        "skills": [],
        "mutual_connections_count": 0,
        "mutual_connections_text": "",
        "posts": [],
        "connection_status": "unknown",
        "screenshot": "",
        "error": "",
    }

    ctx = None
    try:
        ctx = get_firefox_driver()
        driver, page = ctx.__enter__()

        _log(f"Navigating to {profile_url}")
        page.goto(profile_url, wait_until="domcontentloaded", timeout=90000)

        # Wait for SPA render
        for attempt in range(15):
            ready = _safe_evaluate(driver, r"""
                return {
                    bodyLen: document.body ? document.body.innerText.length : 0,
                    links: document.querySelectorAll('a').length
                };
            """, {"bodyLen": 0, "links": 0})
            if ready.get("bodyLen", 0) > 200:
                _log(f"SPA ready after {(attempt+1)*2}s")
                break
            time.sleep(2)

        auth = _detect_auth_wall(page)
        if auth:
            result["error"] = f"Auth wall: {auth}"
            return result

        # Human pause at top
        time.sleep(random.uniform(2, 4))

        # Extract top-card info BEFORE heavy scrolling
        top_data = _safe_evaluate(driver, r"""
            return (function() {
                const main = document.querySelector('main') || document.body;
                const sections = Array.from(main.querySelectorAll('section'));

                // Find top-card section: title looks like a name, or text contains "connecties"
                let topSection = null;
                for (const s of sections) {
                    const h = s.querySelector('h2, h3');
                    const title = h ? h.innerText.trim() : '';
                    const text = s.innerText.trim();
                    const hasConnections = /\bconnecties\b/i.test(text) || /\bconnections\b/i.test(text);
                    const looksLikeName = title && title.length > 2 && title.length < 60 && !/meldingen|netwerk|vacatures|berichten|advert/i.test(title);
                    if ((looksLikeName && hasConnections) || (title && hasConnections && text.split('\n')[0] === title)) {
                        topSection = s;
                        break;
                    }
                }

                let name = '', headline = '', currentCompany = '', location = '';
                let mutualConnectionsText = '';
                let mutualConnectionsCount = 0;
                if (topSection) {
                    const lines = topSection.innerText.split('\n').map(t => t.trim()).filter(t => t);
                    const h = topSection.querySelector('h2, h3');
                    const sectionTitle = h ? h.innerText.trim() : '';

                    // Name = section title, or first non-degree line
                    let nameIdx = lines.findIndex(l => l === sectionTitle);
                    if (nameIdx < 0) nameIdx = 0;
                    name = lines[nameIdx] || '';

                    const idxConnecties = lines.findIndex(l => /\bconnecties\b|\bconnections\b/i.test(l));
                    const endIdx = idxConnecties >= 0 ? idxConnecties : lines.length;

                    // Headline and company usually follow name
                    for (let i = nameIdx + 1; i < endIdx; i++) {
                        const l = lines[i];
                        // Skip degree badges ("2e", "· 2e", "3de+", "1e")
                        if (/^[^a-zA-Z]*\d+e?\+?[^a-zA-Z]*$|^(1e|2e|3de)\+?\s*$/i.test(l)) continue;
                        if (/contactgegevens|contact info/i.test(l)) continue;
                        if (!headline && l.length > 2) {
                            headline = l;
                        } else if (headline && !currentCompany && l.length > 2) {
                            currentCompany = l;
                        }
                    }

                    // Mutual connections
                    for (const l of lines) {
                        const m = l.match(/(\d+)\s*(?:gemeenschappelijke\s+connecties?|mutual\s+connections?)/i);
                        if (m) {
                            mutualConnectionsCount = parseInt(m[1], 10);
                            mutualConnectionsText = l;
                            break;
                        }
                        if (/is een gemeenschappelijke connectie|is a mutual connection/i.test(l)) {
                            mutualConnectionsCount = 1;
                            mutualConnectionsText = l;
                            break;
                        }
                    }
                }

                // Location: search in main area
                let locationOut = '';
                const allText = Array.from(main.querySelectorAll('span, div, h2, p'))
                    .map(el => el.innerText.trim())
                    .filter(t => t.length > 3 && t.length < 120);
                for (const t of allText) {
                    if (/hogeschool|universiteit|college|school|university/i.test(t)) continue;
                    if (/\b(Amsterdam|Rotterdam|Utrecht|Den Haag|Eindhoven|Groningen|Zwolle|Tilburg|Breda|Nederland|Netherlands|België|Belgium|Duitsland|Germany|Region|Provincie|Hoogvliet|Schiedam|Vlaardingen)\b/i.test(t) && t.length < 80) {
                        locationOut = t;
                        break;
                    }
                }
                if (!locationOut) locationOut = location;

                // Connection status from main-area buttons (prioritize top card)
                let status = 'unknown';
                const buttons = Array.from((topSection || main).querySelectorAll('button, a'));
                const texts = buttons.map(b => (b.innerText || '').toLowerCase().trim());
                if (texts.some(t => /afwachting|pending|annuleren|withdraw|terugtrekken/i.test(t))) status = 'pending';
                else if (texts.some(t => /connectie maken|verbinden|connect/i.test(t))) status = 'not_connected';
                else if (texts.some(t => /bericht|message/i.test(t)) && !texts.some(t => /volgen|follow/i.test(t))) status = 'connected';

                return { name, headline, currentCompany, location: locationOut, connectionStatus: status, mutualConnectionsCount, mutualConnectionsText };
            })();
        """, {})
        if top_data:
            result.update({
                "name": top_data.get("name", ""),
                "headline": top_data.get("headline", ""),
                "current_company": top_data.get("currentCompany", ""),
                "location": top_data.get("location", ""),
                "connection_status": top_data.get("connectionStatus", "unknown"),
                "mutual_connections_count": top_data.get("mutualConnectionsCount", 0),
                "mutual_connections_text": top_data.get("mutualConnectionsText", ""),
            })

        # Scroll through profile to load Experience, About, Activity
        _log("Scrolling through profile like a human...")
        _human_scroll_selenium(driver, min_scrolls=2, max_scrolls=4)
        _scroll_to_bottom(driver, max_scrolls=12)

        # Give lazy content time to settle
        time.sleep(2)

        # Extract sections after scrolling
        sections_data = _safe_evaluate(driver, r"""
            return (function() {
                const main = document.querySelector('main') || document.body;

                // Helper: get section by title
                function getSection(titleRegex) {
                    const sections = Array.from(main.querySelectorAll('section'));
                    return sections.find(s => {
                        const h2 = s.querySelector('h2, h3');
                        return h2 && titleRegex.test(h2.innerText.trim());
                    });
                }

                // About
                let about = '';
                const aboutSection = getSection(/^\s*(Over|About|Over mij|Over Nando)\s*$/i);
                if (aboutSection) {
                    const spans = aboutSection.querySelectorAll('span, div, p');
                    for (const el of spans) {
                        const t = el.innerText.trim();
                        if (t.length > 40 && t.length < 3000 && !/Meer weergeven|Show more|...meer/i.test(t)) {
                            about = t;
                            break;
                        }
                    }
                }

                // Experience
                const experience = [];
                const expSection = getSection(/^\s*(Ervaring|Experience|Werkervaring|Carrière)\s*$/i);
                if (expSection) {
                    const items = expSection.querySelectorAll('li, [data-test-id="profile-experience"], .pv-entity__position-group-pager');
                    items.forEach(item => {
                        const lines = item.innerText.split('\n').map(t => t.trim()).filter(t => t && t.length < 200);
                        if (lines.length >= 2) {
                            experience.push({
                                title: lines[0],
                                company: lines[1],
                                duration: lines.find(t => /\d{4}|present|heden|maanden|months|years|jaar|jr\./i.test(t)) || '',
                                location: lines.find(t => /(Nederland|Netherlands|Amsterdam|Rotterdam|Utrecht|Eindhoven|Remote|Hybrid)/i.test(t)) || ''
                            });
                        }
                    });
                }

                // Education
                const education = [];
                const eduSection = getSection(/^\s*(Opleiding|Education|School|Universiteit)\s*$/i);
                if (eduSection) {
                    const items = eduSection.querySelectorAll('li');
                    items.forEach(item => {
                        const lines = item.innerText.split('\n').map(t => t.trim()).filter(t => t && t.length < 200);
                        if (lines.length >= 1) education.push(lines.slice(0, 4));
                    });
                }

                // Skills
                const skills = [];
                const skillsSection = getSection(/^\s*(Vaardigheden|Skills|Compétences)\s*$/i);
                if (skillsSection) {
                    const items = skillsSection.querySelectorAll('li, span, a');
                    items.forEach(item => {
                        const t = item.innerText.trim();
                        if (t && t.length > 2 && t.length < 60 && !/vaardigheden|skills|compétences|meer weergeven|show more/i.test(t)) {
                            skills.push(t);
                        }
                    });
                    skills.splice(0, 20); // Limit
                }

                // Activity / Posts
                const posts = [];
                const actSection = getSection(/^\s*(Activiteit|Activity|Berichten|Posts)\s*$/i);
                if (actSection) {
                    const items = actSection.querySelectorAll('li, [class*="feed-shared"], [class*="update-components"]');
                    items.forEach(item => {
                        const rawText = item.innerText.trim();
                        // Skip video player UI elements
                        if (/subtitles settings|opens subtitles|caption settings|quality settings|playback speed/i.test(rawText)) return;
                        const lines = rawText.split('\n').map(t => t.trim()).filter(t => t && t.length > 30 && t.length < 600);
                        if (lines.length > 0) {
                            posts.push(lines.slice(0, 3).join(' | '));
                        }
                    });
                }

                // Tenure = duration of most recent experience
                let tenure = '';
                if (experience.length > 0 && experience[0].duration) {
                    tenure = experience[0].duration;
                }

                return { about, experience, education, skills, posts, tenure };
            })();
        """, {})
        if sections_data:
            # Preserve education found in top-card if section extraction returned none
            existing_edu = result.get("education", [])
            result.update(sections_data)
            if not result.get("education") and existing_edu:
                result["education"] = existing_edu
            if sections_data.get("tenure"):
                result["tenure"] = sections_data["tenure"]

        # Derive current role/company from headline if headline includes separator
        if result["headline"]:
            import re
            parts = re.split(r'\s+(at|bij|@)\s+', result["headline"], flags=re.IGNORECASE)
            if len(parts) >= 3:
                result["current_role"] = parts[0].strip()
                result["current_company"] = parts[2].strip()
            elif not result["current_role"]:
                # Headline itself is the role
                result["current_role"] = result["headline"].strip()

        # Screenshot: scroll back to top for consistent face-shot, then capture
        driver.execute_script("window.scrollTo({top: 0, left: 0, behavior: 'smooth'});")
        time.sleep(1)
        proof_dir = os.path.join(CLAWBUILDR_DIR, "data", "proof")
        os.makedirs(proof_dir, exist_ok=True)
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        screenshot_path = os.path.join(proof_dir, f"read_profile_{timestamp}.png")
        driver.save_screenshot(screenshot_path)
        result["screenshot"] = screenshot_path

    except Exception as e:
        result["error"] = str(e)
    finally:
        if ctx:
            try:
                ctx.__exit__(None, None, None)
            except Exception:
                pass

    return result


def _log(msg):
    print(f"[LI-READ] {msg}")


if __name__ == "__main__":
    url = sys.argv[1] if len(sys.argv) > 1 else "https://www.linkedin.com/in/nando-odems-698202177/"
    data = read_linkedin_profile(url)
    print(json.dumps(data, indent=2, ensure_ascii=False))
