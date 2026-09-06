import sys
sys.path.insert(0, r"C:\Users\marvi\clawbuildr")

from linkedin_engine import _prepare_firefox_profile, _check_if_connected
from playwright.sync_api import sync_playwright
import json
import os

def test_cookies():
    """Test using extracted cookies to bypass LinkedIn login"""
    cookies_path = os.path.join(os.path.dirname(__file__), "data", "linkedin_cookies.json")
    
    if not os.path.exists(cookies_path):
        print("❌ No cookies found. Please run extract_linkedin_cookies.py first.")
        return False
    
    # Load cookies
    with open(cookies_path, 'r') as f:
        cookies = json.load(f)
    
    print(f"Loaded {len(cookies)} cookies from {cookies_path}")
    
    # Check for critical session cookies
    critical = ["li_at", "liap", "bscookie"]
    found_critical = [c["name"] for c in cookies if c["name"] in critical]
    print(f"Found {len(found_critical)} critical session cookies")
    print(f"Critical session cookies: {found_critical}")
    
    # Prepare Firefox profile
    profile_path = _prepare_firefox_profile()
    
    with sync_playwright() as pw:
        context = pw.firefox.launch_persistent_context(
            profile_path, headless=False, viewport={"width": 1280, "height": 900}
        )
        
        # Add the extracted cookies
        context.add_cookies(cookies)
        page = context.new_page()
        page.set_default_timeout(30000)

        # Test basic LinkedIn access with cookies
        print("\n=== Testing LinkedIn access with cookies ===")
        page.goto("https://www.linkedin.com/feed/", wait_until="domcontentloaded", timeout=60000)
        time.sleep(5)
        
        # Check if we're logged in
        title = page.title()
        current_url = page.url
        print(f"Page title: {title}")
        print(f"Current URL: {current_url}")
        
        # Check for auth walls
        if "login" in current_url or "authwall" in current_url:
            print("Still getting auth wall - cookies may be expired")
            return False
        else:
            print("Successfully bypassed login with cookies!")
        
        # Try to navigate to the test profile
        print("\n=== Navigating to test profile ===")
        page.goto("https://www.linkedin.com/in/jan-boone-499310180/", wait_until="domcontentloaded", timeout=90000)
        time.sleep(10)
        
        # Check status
        status = _check_if_connected(page)
        print(f"Connection status: {status}")
        
        # Debug: Check what elements are visible
        elements = page.evaluate("""() => {
            const els = Array.from(document.querySelectorAll('button, a, [role="button"]'));
            const visible = els.filter(el => {
                const r = el.getBoundingClientRect();
                return r.width > 0 && r.height > 0 && r.top < 1000;
            }).slice(0, 15);
            return visible.map(el => ({
                tag: el.tagName,
                text: (el.innerText || '').trim().substring(0, 50),
                aria: el.getAttribute('aria-label') || '',
                top: Math.round(el.getBoundingClientRect().top),
                left: Math.round(el.getBoundingClientRect().left),
            }));
        }""")
        print(f"Visible elements: {elements}")
        
        # Look for Connect specifically
        connect_info = page.evaluate("""() => {
            const els = Array.from(document.querySelectorAll('a'));
            const connect_links = els.filter(el => {
                const href = el.getAttribute('href') || '';
                const txt = (el.innerText || '').toLowerCase().trim();
                const r = el.getBoundingClientRect();
                return r.width > 0 && r.height > 0 && r.top < 600 && 
                       (href.includes('custom-invite') || txt.includes('connect') || txt.includes('verbinden'));
            }).map(el => ({
                href: el.getAttribute('href'),
                text: el.innerText,
                aria: el.getAttribute('aria-label'),
                top: Math.round(el.getBoundingClientRect().top),
                left: Math.round(el.getBoundingClientRect().left)
            }));
            return connect_links.slice(0, 5);
        }""")
        print(f"Connect links found: {connect_info}")
        
        print("\n=== DONE ===")
        time.sleep(10)
        context.close()
        return True

if __name__ == "__main__":
    import time
    test_cookies()