"""
Human-like LinkedIn company page reader with firmographics and hiring signals.
Opens a company page in Firefox, scrolls through sections, and extracts structured data.
"""
import os
import sys
import json
import time
import random
import re

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLAWBUILDR_DIR = os.path.join(BASE_DIR, "clawbuildr")
if CLAWBUILDR_DIR not in sys.path:
    sys.path.insert(0, CLAWBUILDR_DIR)

from linkedin_engine import get_firefox_driver, _detect_auth_wall


def _log(msg):
    print(f"[LI-COMPANY] {msg}")


def _safe_evaluate(driver, js, default=None):
    """Execute JS via Selenium directly."""
    try:
        return driver.execute_script(js)
    except Exception as e:
        _log(f"JS eval error: {str(e)[:80]}")
        return default


def _human_scroll_selenium(driver, min_scrolls=2, max_scrolls=5):
    """Scroll down the page like a human, using Selenium directly."""
    scrolls = random.randint(min_scrolls, max_scrolls)
    for _ in range(scrolls):
        distance = random.randint(400, 900)
        driver.execute_script(f"window.scrollBy({{top: {distance}, left: 0, behavior: 'smooth'}});")
        time.sleep(random.uniform(0.8, 2.5))


def _scroll_to_bottom(driver, max_scrolls=12):
    """Scroll to bottom progressively to load all lazy sections."""
    last_height = driver.execute_script("return document.body.scrollHeight")
    for _ in range(max_scrolls):
        driver.execute_script("window.scrollTo({top: document.body.scrollHeight, left: 0, behavior: 'smooth'});")
        time.sleep(random.uniform(1.5, 2.5))
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            driver.execute_script("window.scrollBy({top: 500, left: 0, behavior: 'smooth'});")
            time.sleep(1.5)
            newer_height = driver.execute_script("return document.body.scrollHeight")
            if newer_height == last_height:
                break
            last_height = newer_height
        else:
            last_height = new_height


def read_linkedin_company(company_url: str):
    """Read a LinkedIn company page with human-like scrolling."""
    result = {
        "url": company_url,
        "name": "",
        "industry": "",
        "size": "",
        "headquarters": "",
        "founded": "",
        "specialties": "",
        "website": "",
        "followers": "",
        "employee_count_text": "",
        "about_text": "",
        "hiring_count": 0,
        "hiring_jobs": [],
        "posts": [],
        "screenshot": "",
        "error": "",
    }

    ctx = None
    try:
        ctx = get_firefox_driver()
        driver, page = ctx.__enter__()

        if not company_url.endswith('/'):
            company_url += '/'

        _log(f"Navigating to {company_url}")
        page.goto(company_url, wait_until="domcontentloaded", timeout=60000)

        # Wait for SPA render
        for attempt in range(12):
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

        time.sleep(random.uniform(2, 4))

        # Extract top-card firmographics BEFORE heavy scrolling
        top_data = _safe_evaluate(driver, r"""
            return (function() {
                const main = document.querySelector('main') || document.body;
                const sections = Array.from(main.querySelectorAll('section'));

                // Find overview/top-card section
                let topSection = null;
                for (const s of sections) {
                    const text = s.innerText.trim();
                    if (/\bvolgers\b|\bfollowers\b|\bmedewerkers\b|\bemployees\b/i.test(text) && text.length > 100) {
                        topSection = s;
                        break;
                    }
                }
                if (!topSection) return {};

                const lines = topSection.innerText.split('\n').map(t => t.trim()).filter(t => t);

                // Company name: first substantial line
                let name = '';
                for (const l of lines) {
                    if (l.length > 1 && !/^(Home|Info|Bijdragen|Vacatures|Personen|Salesinzichten)$/i.test(l)) {
                        name = l;
                        break;
                    }
                }

                // Industry: line before/after known patterns
                let industry = '';
                let headquarters = '';
                let size = '';
                let followers = '';

                for (let i = 0; i < lines.length; i++) {
                    const l = lines[i];
                    const lower = l.toLowerCase();

                    // Followers / employees count
                    if (/\b(\d+[\d.,]*\s*(mln\.?|m|k)?)\s*(volgers|followers)\b/i.test(l)) {
                        followers = l;
                    }
                    // Company size: prefer top-card patterns, ignore "X medewerkers werken hier"
                    if (/werken\s+(in|bij|hier)|people\s+work\s+here/i.test(l)) continue;
                    // Size line should be short (top-card metadata, not a sentence)
                    if (l.length > 60) continue;
                    if (/\bmeer dan\s*[\d.,]+\s*(medewerkers|employees|werknemers)\b/i.test(l) ||
                        /\b[\d.,]+\s*-\s*[\d.,]+\s*(medewerkers|employees|werknemers)\b/i.test(l) ||
                        /\b[\d.,]+\+?\s*(medewerkers|employees|werknemers)\b/i.test(l)) {
                        size = l;
                    }

                    // Headquarters: location pattern
                    if (/^(Diemen|Amsterdam|Rotterdam|Utrecht|Den Haag|Eindhoven|Groningen|Zwolle|Tilburg|Breda|Antwerpen|Brussel|Bruxelles|Berlijn|München|London|New York|Paris|Barcelona|Singapore|Hong Kong)[,\s]/i.test(l) && l.length < 80) {
                        headquarters = l;
                    }
                }

                // Industry often appears as a short line near top with "·" separators
                for (let i = 0; i < Math.min(lines.length, 10); i++) {
                    const l = lines[i];
                    if (/^(Services|Software|IT|Technologie|Consultancy|Advies|Financiële|Financieel|Marketing|Productie|Logistiek|Transport|Groothandel|Horeca|Zorg|Onderwijs|Bouw|Vastgoed|Telecommunicatie|Energie|Landbouw|Pharma|Recht|Accountancy|HR|Ingenieursbureau)/i.test(l) && l.length < 60) {
                        industry = l;
                        break;
                    }
                }

                // Website from top card
                let website = '';
                const links = Array.from(topSection.querySelectorAll('a[href]'));
                for (const a of links) {
                    const href = a.getAttribute('href') || '';
                    if (/^https?:\/\/(?!www\.linkedin\.com)/i.test(href) && !/google|facebook|twitter|instagram/i.test(href)) {
                        website = href;
                        break;
                    }
                }

                return { name, industry, size, headquarters, followers, website };
            })();
        """, {})
        if top_data:
            result.update(top_data)

        # Try to open About tab for full description and specialties
        try:
            about_link = driver.find_elements("css selector", 'a[href*="/about/"]')
            if about_link:
                _log("Opening About tab")
                driver.execute_script("arguments[0].click();", about_link[0])
                time.sleep(random.uniform(3, 5))
                _scroll_to_bottom(driver, max_scrolls=6)

                about_data = _safe_evaluate(driver, r"""
                    return (function() {
                        const main = document.querySelector('main') || document.body;
                        const sections = Array.from(main.querySelectorAll('section'));

                        function getSection(titleRegex) {
                            return sections.find(s => {
                                const h2 = s.querySelector('h2, h3');
                                return h2 && titleRegex.test(h2.innerText.trim());
                            });
                        }

                        // About text: extract the main company description paragraph
                        let aboutText = '';
                        const aboutSection = getSection(/^\s*(Over|About|Over ons|Overzicht|Overview)\s*$/i);
                        if (aboutSection) {
                            const text = aboutSection.innerText.trim();
                            aboutText = text.replace(/^Over\s*|^About\s*|^Over ons\s*|^Overzicht\s*/i, '')
                                .replace(/…\s*meer weergeven|…\s*meer\s*$/i, '')
                                .replace(/Alles weergeven\s*$/i, '')
                                .trim();
                        }
                        // Fallback: find the paragraph that looks like a company description
                        if (!aboutText) {
                            const paras = Array.from(main.querySelectorAll('p, span, div'))
                                .map(el => el.innerText.trim())
                                .filter(t => t.length > 150 && t.length < 1500);
                            // Prefer one that starts with company name or "X is/was founded"
                            const descriptionLike = paras.find(t => /^[A-Z][a-zA-Z\s&.,'-]{2,40}\s+(is|was|provides|helps|offers|delivers|supports|enables|creates)/.test(t) ||
                                /\bfounded in \d{4}\b/.test(t));
                            if (descriptionLike) {
                                aboutText = descriptionLike;
                            } else if (paras.length) {
                                paras.sort((a, b) => b.length - a.length);
                                aboutText = paras[0];
                            }
                        }
                        // Clean LinkedIn About tab boilerplate
                        if (aboutText) {
                            // Remove "Overzicht" / "Overview" header
                            aboutText = aboutText.replace(/^Overzicht\s*\n\n/i, '').replace(/^Overview\s*\n\n/i, '');
                            // Keep text before metadata blocks
                            const cleanMatch = aboutText.match(/([\s\S]{50,1500}?)(?:\n\nWebsite\n\n|\n\nGeverifieerde pagina\n\n|\n\nBranche\n\n)/);
                            if (cleanMatch) {
                                aboutText = cleanMatch[1].trim();
                            }
                        }

                        // Extract label/value pairs (e.g. "Website\nwww.randstad.com")
                        const allText = main.innerText;
                        const lines = allText.split('\n').map(t => t.trim()).filter(t => t);
                        let founded = '', specialties = '', website = '';
                        for (let i = 0; i < lines.length; i++) {
                            const l = lines[i];
                            const next = lines[i+1] || '';
                            if (/^Founded|Opgericht|Fondée en|Opgericht in$/i.test(l) && /^\d{4}$/.test(next)) {
                                founded = next;
                            }
                            if (/^Specialties|Specialisaties|Spécialités|Specialismen$/i.test(l) && next && next.length > 3) {
                                specialties = next;
                            }
                            if (/^Website|Webseite|Site web$/i.test(l) && next) {
                                website = next;
                            }
                        }

                        // Fallback regex for specialties if label/value not found
                        if (!specialties) {
                            const mSpecialties = allText.match(/(?:Specialties|Specialisaties|Spécialités|Specialismen)\s*[:\n]\s*([\s\S]{3,500}?)(?:\n\n|\n[A-Z]|$)/i);
                            if (mSpecialties) specialties = mSpecialties[1].replace(/\n/g, ', ').trim();
                        }

                        return { aboutText, founded, specialties, website };
                    })();
                """, {})
                if about_data:
                    result["about_text"] = about_data.get("aboutText", "")
                    if about_data.get("founded"):
                        result["founded"] = about_data.get("founded")
                    if about_data.get("specialties"):
                        result["specialties"] = about_data.get("specialties")
                    if about_data.get("website"):
                        result["website"] = about_data.get("website")

                # Go back to main page for hiring signals
                page.goto(company_url, wait_until="domcontentloaded", timeout=60000)
                time.sleep(random.uniform(2, 4))
        except Exception as e:
            _log(f"About tab failed: {str(e)[:80]}")

        # Scroll main page to load posts
        _log("Scrolling company page like a human...")
        _human_scroll_selenium(driver, min_scrolls=2, max_scrolls=4)
        _scroll_to_bottom(driver, max_scrolls=10)
        time.sleep(2)

        # Extract posts from main page
        posts_data = _safe_evaluate(driver, r"""
            return (function() {
                const main = document.querySelector('main') || document.body;
                const sections = Array.from(main.querySelectorAll('section'));
                function getSection(titleRegex, bodyRegex) {
                    return sections.find(s => {
                        const h2 = s.querySelector('h2, h3');
                        const title = h2 ? h2.innerText.trim() : '';
                        const text = s.innerText.trim();
                        return (titleRegex && titleRegex.test(title)) || (bodyRegex && bodyRegex.test(text));
                    });
                }
                const posts = [];
                const postsSection = getSection(/^\s*(Paginabijdragen|Posts|Bijdragen|Activiteit|Activity)\s*$/i, /\b(Paginabijdragen|Posts|Bijdragen)\b/i);
                if (postsSection) {
                    const items = postsSection.querySelectorAll('li, [class*="feed-shared"], [class*="update-components"]');
                    items.forEach(item => {
                        const rawText = item.innerText.trim();
                        if (/subtitles settings|opens subtitles|caption settings|quality settings|playback speed/i.test(rawText)) return;
                        const lines = rawText.split('\n').map(t => t.trim()).filter(t => t && t.length > 30 && t.length < 600);
                        if (lines.length > 0) {
                            posts.push(lines.slice(0, 3).join(' | '));
                        }
                    });
                }
                return { posts };
            })();
        """, {})
        if posts_data:
            result["posts"] = posts_data.get("posts", [])

        # Navigate to dedicated jobs page for reliable hiring signals
        try:
            jobs_url = company_url + "jobs/"
            _log(f"Checking dedicated jobs page: {jobs_url}")
            page.goto(jobs_url, wait_until="domcontentloaded", timeout=60000)
            time.sleep(random.uniform(3, 5))
            _scroll_to_bottom(driver, max_scrolls=8)

            jobs_data = _safe_evaluate(driver, r"""
                return (function() {
                    const main = document.querySelector('main') || document.body;
                    const bodyText = main.innerText;
                    const jobs = [];
                    const seen = new Set();

                    // Strategy 1: structured cards
                    const skipTitlePatterns = /^(Onlangs geplaatste vacatures|Recent jobs|Vacatures|Jobs|Gerelateerde pagina|Gerelateerde pagina's|Mensen volgen ook|Randstad|Services\s+personeelszaken|\d+\s*mln\.?\s*volgers|Meer dan\s*[\d.,]+\s*medewerkers|Technologie, informatie en media)/i;
                    const skipLocationPatterns = /^(Technologie, informatie en media|Gerelateerde pagina|Gerelateerde pagina's|Mensen volgen ook)$/i;
                    const cards = main.querySelectorAll('li, [class*="job-card"], [class*="jobs-search-results__list-item"], article, div');
                    cards.forEach(card => {
                        const text = card.innerText.trim();
                        if (!text || /subtitles|descriptions|default, selected|quality|playback/i.test(text)) return;
                        const lines = text.split('\n').map(t => t.trim()).filter(t => t && t.length < 200);
                        if (lines.length < 1) return;
                        const title = lines[0].replace(/\(Geverifieerde vacature\)/i, '').trim();
                        if (title.length < 4 || skipTitlePatterns.test(title) || seen.has(title)) return;
                        const location = lines.find(l => /(Nederland|Netherlands|België|Belgium|Remote|Hybrid|Op locatie|Op afstand|[A-Za-z]+,\s*[A-Za-z]+)/i.test(l)) || '';
                        if (skipLocationPatterns.test(location)) return;
                        const hasRoleWords = /(manager|directeur|engineer|consultant|specialist|adviseur|advisor|developer|analist|analyst|coördinator|coordinator|project|sales|marketing|hr|finance|accountant|recruiter|operator|medewerker|assistant|ondersteuner|verpleegkundige|arts|technicus|monteur|inkoper|inkoop|logistiek|transport|warehouse|magazijn|administratief|secretaresse|receptionist|teamlead|team lead|lead|chief|head|vp|president|officer|director)/i.test(title);
                        if ((hasRoleWords || location) && !seen.has(title)) {
                            seen.add(title);
                            jobs.push({ title, location });
                        }
                    });

                    // Strategy 2: parse body text after "Onlangs geplaatste vacatures" / "Recent jobs"
                    if (jobs.length < 3) {
                        const sectionMatch = bodyText.match(/(?:Onlangs geplaatste vacatures|Recent jobs|Recente vacatures|Vacatures|Jobs)[\s\S]{0,15000}/i);
                        if (sectionMatch) {
                            const section = sectionMatch[0];
                            // Split by job separators
                            const blocks = section.split(/(?:Nu geplaatst|Nu|Actief|Geverifieerde vacature|Eenvoudig solliciteren|Be\. ?verdiend|\d+\s*dagen? geleden|\d+\s*uur geleden)/i);
                            const skipTitlePatterns = /^(Onlangs geplaatste vacatures|Recent jobs|Vacatures|Jobs|Gerelateerde pagina|Gerelateerde pagina's|Mensen volgen ook|Randstad|Services\s+personeelszaken|\d+\s*mln\.?\s*volgers|Meer dan\s*[\d.,]+\s*medewerkers|Technologie, informatie en media)/i;
                            const skipLocationPatterns = /^(Technologie, informatie en media|Gerelateerde pagina|Gerelateerde pagina's|Mensen volgen ook)$/i;
                            blocks.forEach(block => {
                                const lines = block.split('\n').map(t => t.trim()).filter(t => t && t.length < 200 && t.length > 3);
                                if (lines.length < 2) return;
                                let title = lines[0].replace(/\(Geverifieerde vacature\)/i, '').trim();
                                if (title.length < 4 || seen.has(title)) return;
                                if (skipTitlePatterns.test(title)) return;
                                const location = lines.find(l => /(Nederland|Netherlands|België|Belgium|Remote|Hybrid|Op locatie|Op afstand|[A-Za-z]+,\s*[A-Za-z]+)/i.test(l)) || '';
                                if (skipLocationPatterns.test(location)) return;
                                const hasRoleWords = /(manager|directeur|engineer|consultant|specialist|adviseur|advisor|developer|analist|analyst|coördinator|coordinator|project|sales|marketing|hr|finance|accountant|recruiter|operator|medewerker|assistant|ondersteuner|verpleegkundige|arts|technicus|monteur|inkoper|inkoop|logistiek|transport|warehouse|magazijn|administratief|secretaresse|receptionist|teamlead|team lead|lead|chief|head|vp|president|officer|director)/i.test(title);
                                if (hasRoleWords || location) {
                                    seen.add(title);
                                    jobs.push({ title, location });
                                }
                            });
                        }
                    }

                    // Total count from page text
                    const totalMatch = bodyText.match(/(\d+[\d.,]*)\s*(?:resultaten|results|vacatures|jobs)/i);
                    return { jobs, totalText: totalMatch ? totalMatch[1] : '' };
                })();
            """, {})
            if jobs_data:
                result["hiring_jobs"] = jobs_data.get("jobs", [])
                result["hiring_count"] = len(jobs_data.get("jobs", []))
                if jobs_data.get("totalText"):
                    result["hiring_total_text"] = jobs_data["totalText"]
        except Exception as e:
            _log(f"Jobs page failed: {str(e)[:80]}")

        # Employee count from size or visible text
        if not result["employee_count_text"] and result["size"]:
            m = re.search(r'([\d.,]+\s*(?:-\s*[\d.,]+)?\+?)', result["size"])
            if m:
                result["employee_count_text"] = m.group(1).strip()
        if not result["employee_count_text"]:
            emp_match = re.search(r'([\d,.]+)\s*(?:employees|medewerkers|werknemers)', result.get("raw_text", ""), re.IGNORECASE)
            if emp_match:
                result["employee_count_text"] = emp_match.group(1)

        # Fallback about text from main page if About tab didn't populate it
        if not result["about_text"]:
            raw = result.get("raw_text", "")
            m = re.search(r'(?:Over\s+|About\s+|Over ons\s+)([\s\S]{50,1500}?)(?:…\s*meer weergeven|…\s*meer\s*$|Alles weergeven\s*$)', raw, re.IGNORECASE)
            if m:
                result["about_text"] = m.group(1).strip()[:1500]

        # Extract founded year from about text
        if not result["founded"] and result["about_text"]:
            founded_match = re.search(r'(?:founded in|opgericht in|opgericht)\s+(\d{4})', result["about_text"], re.IGNORECASE)
            if founded_match:
                result["founded"] = founded_match.group(1)

        # Screenshot
        driver.execute_script("window.scrollTo({top: 0, left: 0, behavior: 'smooth'});")
        time.sleep(1)
        proof_dir = os.path.join(CLAWBUILDR_DIR, "data", "proof")
        os.makedirs(proof_dir, exist_ok=True)
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        screenshot_path = os.path.join(proof_dir, f"read_company_{timestamp}.png")
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


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    url = sys.argv[1] if len(sys.argv) > 1 else "https://www.linkedin.com/company/randstad/"
    data = read_linkedin_company(url)
    print(json.dumps(data, indent=2, ensure_ascii=False))
