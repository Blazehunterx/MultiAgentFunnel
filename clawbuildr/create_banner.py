import sys
sys.path.insert(0, r'C:\Users\marvi\clawbuildr')
from linkedin_engine import get_firefox_driver
import time

with get_firefox_driver() as ctx:
    driver, page = ctx
    driver.set_page_load_timeout(10)
    
    # Set window to exact banner size
    driver.set_window_size(1584, 396)
    
    # Open the HTML banner template
    page.goto('file:///C:/Users/marvi/clawbuildr/data/banner_template.html', wait_until='domcontentloaded', timeout=10000)
    time.sleep(1)
    
    # Screenshot the banner
    driver.save_screenshot(r'C:\Users\marvi\clawbuildr\data\banner_new.png')
    print('Banner saved to banner_new.png')
