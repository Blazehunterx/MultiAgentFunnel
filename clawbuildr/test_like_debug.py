import sys
sys.path.insert(0, r'C:\Users\marvi\clawbuildr')
from linkedin_engine import get_firefox_driver
import time

with get_firefox_driver() as ctx:
    driver, page = ctx
    driver.set_page_load_timeout(20)
    page.goto('https://www.linkedin.com/feed/', wait_until='domcontentloaded', timeout=20000)
    time.sleep(8)
    
    # Check if we're logged in
    url = driver.current_url
    title = driver.title
    print(f"URL: {url}")
    print(f"Title: {title}")
    
    # Check for auth wall
    has_auth = driver.execute_script("""
        const authWall = document.querySelector('[data-testid="auth-wall"]');
        const loginForm = document.querySelector('input[name="session_key"]');
        return !!(authWall || loginForm);
    """)
    print(f"Auth wall: {has_auth}")
    
    # Count all buttons
    btn_count = driver.execute_script("return document.querySelectorAll('button').length")
    print(f"Total buttons: {btn_count}")
    
    # Find like buttons with full debug
    like_buttons = driver.execute_script("""
        const results = [];
        const allButtons = document.querySelectorAll('button');
        for (const btn of allButtons) {
            const ariaLabel = btn.getAttribute('aria-label') || '';
            const text = (btn.innerText || '').trim();
            const rect = btn.getBoundingClientRect();
            
            // Check all buttons for reaction-related content
            const al = ariaLabel.toLowerCase();
            const txt = text.toLowerCase();
            
            if (al.includes('reactieknop') || al.includes('like') || 
                al.includes('geen reactie') || txt === 'interessant' || txt === 'like') {
                results.push({
                    ariaLabel: ariaLabel.substring(0, 60),
                    text: text.substring(0, 30),
                    width: rect.width,
                    height: rect.height,
                    x: rect.x,
                    y: rect.y,
                    visible: rect.y > 0 && rect.y < window.innerHeight
                });
            }
        }
        return results;
    """)
    
    print(f"\nLike buttons found: {len(like_buttons)}")
    for b in like_buttons:
        print(f'  "{b["ariaLabel"]}" text="{b["text"]}" rect=({b["x"]:.0f},{b["y"]:.0f},{b["width"]:.0f},{b["height"]:.0f}) visible={b["visible"]}')
