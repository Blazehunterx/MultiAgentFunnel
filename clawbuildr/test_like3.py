import sys
sys.path.insert(0, r'C:\Users\marvi\clawbuildr')
from linkedin_engine import get_firefox_driver
import time

with get_firefox_driver() as ctx:
    driver, page = ctx
    driver.set_page_load_timeout(20)
    page.goto('https://www.linkedin.com/feed/', wait_until='domcontentloaded', timeout=20000)
    time.sleep(5)
    
    # Get ALL button aria-labels
    all_buttons = driver.execute_script("""
        const results = [];
        const buttons = document.querySelectorAll('button');
        for (const btn of buttons) {
            const ariaLabel = btn.getAttribute('aria-label') || '';
            const text = (btn.innerText || '').trim().substring(0, 40);
            const rect = btn.getBoundingClientRect();
            if (rect.width > 0 && rect.height > 0) {
                results.push({ariaLabel: ariaLabel, text: text});
            }
        }
        return results;
    """)
    
    # Filter for anything reaction-related
    print(f"Total buttons: {len(all_buttons)}")
    print("\nReaction-related buttons:")
    for b in all_buttons:
        al = b['ariaLabel'].lower()
        txt = b['text'].lower()
        if any(x in al or x in txt for x in ['react', 'like', 'interessant', 'leuk', 'comment', 'reageer']):
            print(f'  aria="{b["ariaLabel"]}" text="{b["text"]}"')
