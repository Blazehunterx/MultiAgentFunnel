import sys
sys.path.insert(0, r"C:\Users\marvi\clawbuildr")

from linkedin_engine import _prepare_firefox_profile, _check_if_connected
from playwright.sync_api import sync_playwright
import time

profile_path = _prepare_firefox_profile()

with sync_playwright() as pw:
    context = pw.firefox.launch_persistent_context(
        profile_path, headless=False, viewport={"width": 1280, "height": 900}
    )
    page = context.new_page()
    page.set_default_timeout(30000)

    # Test basic LinkedIn access
    print("=== Testing basic LinkedIn access ===")
    page.goto("https://www.linkedin.com", wait_until="domcontentloaded", timeout=60000)
    time.sleep(5)
    
    # Check if we're logged in
    title = page.title()
    print(f"Page title: {title}")
    
    # Check for any auth walls
    auth = page.url
    print(f"Current URL: {auth}")
    
    # Try to navigate to the test profile
    print("\n=== Navigating to test profile ===")
    page.goto("https://www.linkedin.com/in/jan-boone-499310180/", wait_until="domcontentloaded", timeout=90000)
    time.sleep(10)
    
    # Check status
    status = _check_if_connected(page)
    print(f"Status: {status}")
    
    # Debug: Check what elements are visible
    elements = page.evaluate("""() => {
        const els = Array.from(document.querySelectorAll('button, a, [role="button"]'));
        const visible = els.filter(el => {
            const r = el.getBoundingClientRect();
            return r.width > 0 && r.height > 0 && r.top < 1000;
        }).slice(0, 10);
        return visible.map(el => ({
            tag: el.tagName,
            text: (el.innerText || '').trim().substring(0, 50),
            aria: el.getAttribute('aria-label') || '',
            top: Math.round(el.getBoundingClientRect().top),
            left: Math.round(el.getBoundingClientRect().left),
        }));
    }""")
    print(f"Visible elements: {elements}")
    
    print("\n=== DONE ===")
    time.sleep(5)
    context.close()