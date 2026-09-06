import sys
sys.path.insert(0, r'C:\Users\marvi\clawbuildr')
from linkedin_engine import get_firefox_driver
import time

with get_firefox_driver() as ctx:
    driver, page = ctx
    driver.set_page_load_timeout(20)
    page.goto('https://www.linkedin.com/feed/', wait_until='domcontentloaded', timeout=20000)
    time.sleep(5)
    
    # Find all buttons with their aria-labels
    buttons = driver.execute_script("""
        const results = [];
        const buttons = document.querySelectorAll("button");
        for (const btn of buttons) {
            const ariaLabel = btn.getAttribute("aria-label") || "";
            const text = (btn.innerText || "").trim().substring(0, 30);
            const rect = btn.getBoundingClientRect();
            if (rect.width > 10 && rect.height > 10 && rect.y > 0) {
                results.push({ariaLabel: ariaLabel, text: text, x: rect.x, y: rect.y});
            }
        }
        return results.slice(0, 30);
    """)
    
    print(f"Found {len(buttons)} buttons:")
    for b in buttons:
        print(f'  aria-label="{b["ariaLabel"]}" text="{b["text"]}" at ({b["x"]:.0f}, {b["y"]:.0f})')
