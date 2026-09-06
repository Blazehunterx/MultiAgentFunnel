import sys
sys.path.insert(0, r'C:\Users\marvi\clawbuildr')
from linkedin_engine import get_firefox_driver
import time

with get_firefox_driver() as ctx:
    driver, page = ctx
    driver.set_page_load_timeout(20)
    page.goto('https://www.linkedin.com/feed/', wait_until='domcontentloaded', timeout=20000)
    time.sleep(5)
    
    # Test the exact selector logic from auto_like_feed_posts
    like_buttons = driver.execute_script("""
        const results = [];
        const buttons = document.querySelectorAll('button');
        for (const btn of buttons) {
            const ariaLabel = btn.getAttribute('aria-label') || '';
            const text = (btn.innerText || '').trim();
            const isLikeBtn = ariaLabel.includes('reactieknop') || 
                             ariaLabel.includes('Like') || 
                             ariaLabel.includes('geen reactie') ||
                             text === 'Interessant' || 
                             text === 'Like';
            if (isLikeBtn) {
                const rect = btn.getBoundingClientRect();
                if (rect.width > 10 && rect.height > 10 && rect.y > 0 && rect.y < window.innerHeight) {
                    const isLiked = btn.classList.contains('artdeco-button--muted') || 
                                   btn.getAttribute('aria-pressed') === 'true' ||
                                   ariaLabel.includes('Actieve') || ariaLabel.includes('Active');
                    results.push({
                        ariaLabel: ariaLabel, 
                        text: text, 
                        x: rect.x + rect.width/2, 
                        y: rect.y + rect.height/2,
                        isLiked: isLiked,
                        classes: btn.className.substring(0, 50)
                    });
                }
            }
        }
        return results;
    """)
    
    print(f"Found {len(like_buttons)} like buttons:")
    for b in like_buttons:
        print(f'  aria="{b["ariaLabel"][:50]}" text="{b["text"]}" liked={b["isLiked"]} classes="{b["classes"]}"')
