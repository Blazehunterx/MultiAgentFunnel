"""
Search engine rotator with rate limiting, query diversification, and engine-specific parsers.
Used by the dashboard to avoid Brave 429s and keep the pipeline flowing.
"""
import asyncio
import json
import random
import re
import time
from urllib.parse import urlparse, unquote
import httpx

# Global rate limiting
_LAST_WEB_SEARCH_TIME = 0
_MIN_SEARCH_INTERVAL = 12  # seconds between any two search requests

_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0"


def _normalize_domain(url: str) -> str:
    """Extract clean domain from URL."""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower().replace("www.", "")
        return domain
    except Exception:
        return ""


def _skip_domain(domain: str) -> bool:
    """Return True if domain is not a business website."""
    skip_domains = {
        "wikipedia.org", "linkedin.com", "twitter.com", "x.com", "instagram.com",
        "github.com", "reddit.com", "amazon.com", "bol.com", "marktplaats.nl",
        "kvk.nl", "zoominfo.com", "crunchbase.com", "trustpilot.com",
        "google.com", "bing.com", "brave.com", "duckduckgo.com", "mojeek.com",
        "youtube.com", "facebook.com", "apple.com", "microsoft.com",
        "nl.linkedin.com", "linkedin.nl",
    }
    return any(domain.endswith(d) for d in skip_domains) or not domain


def _extract_mojeek_results(html: str) -> list:
    """Parse Mojeek result HTML."""
    results = []
    seen = set()
    # Mojeek results: <a class="title" href="URL">Title</a>
    for match in re.finditer(r'<a[^>]*class="title"[^>]*href="([^"]+)"[^>]*>(.*?)</a>', html, re.S):
        href = match.group(1)
        title = re.sub(r'<[^>]+>', ' ', match.group(2)).strip()
        if href.startswith("/"):
            href = "https://www.mojeek.com" + href
        domain = _normalize_domain(href)
        if _skip_domain(domain) or domain in seen:
            continue
        seen.add(domain)
        results.append({"title": title, "url": href, "domain": domain})
    return results


def _extract_ddg_results(html: str) -> list:
    """Parse DuckDuckGo Lite result HTML."""
    results = []
    seen = set()
    # DDG lite results are in table rows with .result-link
    for match in re.finditer(r'<a[^>]*class="result-link"[^>]*href="([^"]+)"[^>]*>(.*?)</a>', html, re.S):
        href = match.group(1)
        title = re.sub(r'<[^>]+>', ' ', match.group(2)).strip()
        domain = _normalize_domain(href)
        if _skip_domain(domain) or domain in seen:
            continue
        seen.add(domain)
        results.append({"title": title, "url": href, "domain": domain})
    return results


def _extract_brave_results(html: str) -> list:
    """Parse Brave Search result HTML."""
    results = []
    seen = set()
    # Brave: <a href="URL" class="..."><h2>...</h2></a> or result title links
    for match in re.finditer(r'<a[^>]*href="([^"]+)"[^>]*>\s*<h2[^>]*>(.*?)</h2>\s*</a>', html, re.S):
        href = match.group(1)
        title = re.sub(r'<[^>]+>', ' ', match.group(2)).strip()
        domain = _normalize_domain(href)
        if _skip_domain(domain) or domain in seen:
            continue
        seen.add(domain)
        results.append({"title": title, "url": href, "domain": domain})
    return results


def _extract_bing_results(html: str) -> list:
    """Parse Bing result HTML."""
    results = []
    seen = set()
    # Split by b_algo blocks and find the real result link (class "tilk") + title (h2)
    blocks = re.split(r'<li[^>]*class="b_algo"[^>]*>', html)
    for block in blocks[1:]:
        # Title
        title_match = re.search(r'<h2[^>]*>(.*?)</h2>', block, re.S)
        if not title_match:
            continue
        title = re.sub(r'<[^>]+>', ' ', title_match.group(1)).strip()
        # Find the visible result URL (class tilk)
        href_match = re.search(r'<a[^>]*class="tilk"[^>]*href="([^"]+)"', block, re.S)
        if not href_match:
            # fallback: any external href in block
            href_match = re.search(r'href="(https?://[^"]+)"', block, re.S)
        if not href_match:
            continue
        href = href_match.group(1)
        href = unquote(href)
        # Decode Bing redirect if needed (u= parameter is base64 with possible a1/a2 prefix)
        if "bing.com/ck/a?!" in href:
            m = re.search(r'u=([a-zA-Z0-9_-]+)', href)
            if m:
                try:
                    import base64
                    val = m.group(1)
                    if val.startswith(('a1', 'a2', 'a3')):
                        val = val[2:]
                    decoded = base64.urlsafe_b64decode(val + '==').decode('utf-8')
                    href = decoded
                except Exception:
                    pass
        domain = _normalize_domain(href)
        if _skip_domain(domain) or domain in seen or not domain:
            continue
        seen.add(domain)
        results.append({"title": title, "url": href, "domain": domain})
    return results


def _extract_google_results(html: str) -> list:
    """Parse Google result HTML."""
    results = []
    seen = set()
    # Google organic results: <div class="g"><a href="/url?q=..."...><h3>...</h3></a></div>
    for match in re.finditer(r'<div[^>]*class="[^"]*(?:g|xpd)[^"]*"[^>]*>.*?<a[^>]*href="(?:/url\?q=)?([^"&]+)[^"]*"[^>]*>.*?<h3[^>]*>(.*?)</h3>.*?</div>', html, re.S):
        href = match.group(1)
        title = re.sub(r'<[^>]+>', ' ', match.group(2)).strip()
        href = unquote(href)
        if href.startswith("/url?q="):
            href = href.split("/url?q=", 1)[1].split("&", 1)[0]
        domain = _normalize_domain(href)
        if _skip_domain(domain) or domain in seen:
            continue
        seen.add(domain)
        results.append({"title": title, "url": href, "domain": domain})
    return results


def _extract_generic_results(html: str) -> list:
    """Generic fallback parser: grab clean external links with titles."""
    results = []
    seen = set()
    for match in re.finditer(r'<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>', html, re.S):
        href = match.group(1)
        title = re.sub(r'<[^>]+>', ' ', match.group(2)).strip()

        if not href or href.startswith(("#", "/", "javascript:", "data:")):
            continue
        if any(x in href.lower() for x in ["google.", "bing.com", "brave.com", "duckduckgo.com", "mojeek.com", "youtube.com", "facebook.com"]):
            continue
        if any(x in title.lower() for x in ["advertentie", "ad ", "sponsored", "privacy", "cookies", "login", "sign in"]):
            continue

        if href.startswith("//"):
            href = "https:" + href
        elif href.startswith("/url?q="):
            q = href.split("/url?q=", 1)[1].split("&", 1)[0]
            href = unquote(q)

        domain = _normalize_domain(href)
        if _skip_domain(domain) or domain in seen:
            continue
        if not title or len(title) < 3:
            continue

        seen.add(domain)
        results.append({"title": title, "url": href, "domain": domain})

    return results[:10]


def _extract_results(html: str, source: str) -> list:
    """Extract results using engine-specific parser if available."""
    parsers = {
        "mojeek": _extract_mojeek_results,
        "duckduckgo_lite": _extract_ddg_results,
        "brave": _extract_brave_results,
        "bing": _extract_bing_results,
        "google": _extract_google_results,
        "yelp": lambda h: [],  # Yelp handled separately
    }
    parser = parsers.get(source, _extract_generic_results)
    return parser(html)


def _query_variations(query: str) -> list:
    """Generate query variations to improve yield."""
    variations = [query]
    # Strip country qualifiers
    base = re.sub(r'\b(Nederland|Netherlands|NL|Belgium|België|BE|Germany|Duitsland|DE)\b', '', query, flags=re.IGNORECASE).strip()
    if base and base != query:
        variations.append(base)
    # Add site exclusions to avoid directory sites
    variations.append(query + " -linkedin.com -wikipedia.org")
    return variations


async def search_web(query: str, logger=None) -> list:
    """Search the web using rotating engines with rate limiting and query diversification.
    Returns list of {title, url, domain} dicts.
    """
    global _LAST_WEB_SEARCH_TIME

    # Enforce global minimum interval
    elapsed = time.time() - _LAST_WEB_SEARCH_TIME
    if elapsed < _MIN_SEARCH_INTERVAL:
        wait = _MIN_SEARCH_INTERVAL - elapsed + random.uniform(0, 2)
        if logger:
            logger.info(f"[Search Rotator] Rate limit: sleeping {wait:.1f}s before next query")
        await asyncio.sleep(wait)
    _LAST_WEB_SEARCH_TIME = time.time()

    # Engine configs: (name, url, params, parser)
    engines = [
        {
            "name": "google",
            "url": "https://www.google.com/search",
            "params": {"q": "{query}", "hl": "nl", "num": "10"},
        },
        {
            "name": "mojeek",
            "url": "https://www.mojeek.com/search",
            "params": {"q": "{query}"},
        },
        {
            "name": "duckduckgo_lite",
            "url": "https://lite.duckduckgo.com/lite/",
            "params": {"q": "{query}", "kl": "nl-nl"},
        },
        {
            "name": "brave",
            "url": "https://search.brave.com/search",
            "params": {"q": "{query}"},
        },
        {
            "name": "bing",
            "url": "https://www.bing.com/search",
            "params": {"q": "{query}", "setlang": "nl-nl", "count": "10"},
        },
    ]

    all_results = []
    seen_domains = set()

    # Try each engine with each query variation until we have enough results
    random.shuffle(engines)
    variations = _query_variations(query)

    for engine in engines:
        for variation in variations:
            try:
                params = {k: v.format(query=variation) for k, v in engine["params"].items()}
                async with httpx.AsyncClient(timeout=20, follow_redirects=True, verify=False,
                                             headers={"User-Agent": _USER_AGENT}) as client:
                    r = await client.get(engine["url"], params=params)
                    if r.status_code == 429:
                        if logger:
                            logger.info(f"[Search Rotator] {engine['name']} returned 429")
                        break  # skip to next engine
                    if r.status_code != 200:
                        if logger:
                            logger.info(f"[Search Rotator] {engine['name']} returned {r.status_code}")
                        continue

                    html = r.text
                    results = _extract_results(html, engine["name"])
                    new_results = []
                    for res in results:
                        if res["domain"] not in seen_domains:
                            seen_domains.add(res["domain"])
                            new_results.append(res)

                    if new_results:
                        if logger:
                            logger.info(f"[Search Rotator] {engine['name']} returned {len(new_results)} new results for: {variation[:50]}")
                        all_results.extend(new_results)

                    if len(all_results) >= 8:
                        return all_results[:10]

            except Exception as e:
                if logger:
                    logger.warning(f"[Search Rotator] {engine['name']} failed: {str(e)[:80]}")
                continue

    # Final fallback: Yelp.nl
    if len(all_results) < 3:
        try:
            yelp_query = query.replace("Nederland", "").replace("Netherlands", "").strip()
            async with httpx.AsyncClient(timeout=15, follow_redirects=True, verify=False,
                                         headers={"User-Agent": _USER_AGENT}) as client:
                r = await client.get("https://www.yelp.nl/search", params={"find_desc": yelp_query, "find_loc": "Nederland"})
                if r.status_code == 200:
                    results = _extract_results(r.text, "yelp")
                    for res in results:
                        if res["domain"] not in seen_domains:
                            seen_domains.add(res["domain"])
                            all_results.append(res)
                    if results and logger:
                        logger.info(f"[Search Rotator] Yelp fallback returned {len(results)} results")
        except Exception as e:
            if logger:
                logger.warning(f"[Search Rotator] Yelp fallback failed: {str(e)[:80]}")

    # Browser fallback: if HTTP engines gave poor/irrelevant results, use Firefox via Bing
    if len(all_results) < 3:
        try:
            browser_results = await _browser_search_fallback(query)
            for res in browser_results:
                if res["domain"] not in seen_domains:
                    seen_domains.add(res["domain"])
                    all_results.append(res)
            if browser_results and logger:
                logger.info(f"[Search Rotator] Browser fallback returned {len(browser_results)} results")
        except Exception as e:
            if logger:
                logger.warning(f"[Search Rotator] Browser fallback failed: {str(e)[:80]}")

    return all_results[:10]


async def _browser_search_fallback(query: str) -> list:
    """Fallback search using Firefox + Bing when HTTP engines fail or return spam."""
    import sys
    import os
    import time
    base = os.path.dirname(os.path.abspath(__file__))
    if base not in sys.path:
        sys.path.insert(0, base)
    from linkedin_engine import get_firefox_driver

    results = []
    ctx = None
    try:
        ctx = get_firefox_driver()
        driver, page = ctx.__enter__()

        # Go to Bing
        page.goto("https://www.bing.com", wait_until="domcontentloaded", timeout=30000)
        time.sleep(2)

        # Find search box and type query
        search_selectors = ['input[name="q"]', 'textarea[name="q"]', '#sb_form_q', '.b_searchbox']
        box = None
        for sel in search_selectors:
            try:
                elems = driver.find_elements("css selector", sel)
                if elems:
                    box = elems[0]
                    break
            except Exception:
                continue
        if not box:
            return results

        box.clear()
        # Human-like typing
        for char in query:
            box.send_keys(char)
            time.sleep(0.05 + 0.1 * (ord(char) % 3) / 3)
        box.submit()

        # Wait for results
        for _ in range(15):
            time.sleep(1)
            try:
                if driver.find_elements("css selector", 'li.b_algo'):
                    break
            except Exception:
                pass

        # Extract results using same Bing parser
        html = driver.page_source
        return _extract_bing_results(html)

    except Exception:
        return results
    finally:
        if ctx:
            try:
                ctx.__exit__(None, None, None)
            except Exception:
                pass


if __name__ == "__main__":
    import sys
    q = sys.argv[1] if len(sys.argv) > 1 else "logistiek bedrijf Nederland"
    res = asyncio.run(search_web(q))
    print(json.dumps(res, indent=2, ensure_ascii=False))
