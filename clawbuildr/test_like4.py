import sys
sys.path.insert(0, r'C:\Users\marvi\clawbuildr')
from linkedin_engine import get_firefox_driver
import time

with get_firefox_driver() as ctx:
    driver, page = ctx
    driver.set_page_load_timeout(20)
    page.goto('https://www.linkedin.com/feed/', wait_until='domcontentloaded', timeout=20000)
    time.sleep(5)
    
    # Get ALL buttons with full details
    all_buttons = driver.execute_script("""
        const results = [];
        const buttons = document.querySelectorAll('button');
        for (const btn of buttons) {
            const ariaLabel = btn.getAttribute('aria-label') || '';
            const text = (btn.innerText || '').trim().substring(0, 40);
            const rect = btn.getBoundingClientRect();
            results.push({
                ariaLabel: ariaLabel, 
                text: text, 
                width: rect.width, 
                height: rect.height, 
                x: rect.x, 
                y: rect.y,
                visible: rect.width > 10 && rect.height > 10 && rect.y > 0 && rect.y < window.innerHeight
            });
        }
        return results;
    """)
    
    # Filter for reaction buttons
    print("Reaction buttons with visibility check:")
    for b in all_buttons:
        al = b['ariaLabel'].lower()
        txt = b['text'].lower()
        if 'reactieknop' in al or 'interessant' in txt:
            print(f'  aria="{b["ariaLabel"]}" text="{b["text"]}" rect=({b["x"]:.0f},{b["y"]:.0f},{b["width"]:.0f},{b["height"]:.0f}) visible={b["visible"]}')
