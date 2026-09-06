import sys
sys.path.insert(0, r"C:\Users\marvi\clawbuildr")

from linkedin_engine import _prepare_firefox_profile, linkedin_login
from playwright.sync_api import sync_playwright
import json
import os
import time

def quick_login_test():
    """Quick test to check if we can bypass login with current cookies"""
    cookies_path = os.path.join(os.path.dirname(__file__), "data", "linkedin_cookies.json")
    
    if not os.path.exists(cookies_path):
        print("❌ No cookies found. Please run extract_linkedin_cookies.py first.")
        return False
    
    with open(cookies_path, 'r') as f:
        cookies = json.load(f)
    
    profile_path = _prepare_firefox_profile()
    
    with sync_playwright() as pw:
        context = pw.firefox.launch_persistent_context(
            profile_path, headless=False, viewport={"width": 1280, "height": 900}
        )
        
        context.add_cookies(cookies)
        page = context.new_page()
        page.set_default_timeout(30000)

        print("=== Quick Login Test ===")
        print("Testing if current cookies work for profile access...")
        
        # Test profile access
        page.goto("https://www.linkedin.com/in/jan-boone-499310180/", wait_until="domcontentloaded", timeout=30000)
        time.sleep(3)
        
        title = page.title()
        url = page.url
        
        print(f"Page title: {title}")
        print(f"Current URL: {url}")
        
        if "authwall" in title.lower() or "login" in title.lower():
            print("\n❌ Cookies expired - hitting auth wall")
            print("\n=== Manual Login Required ===")
            print("Please follow these steps:")
            print("1. Close this browser window")
            print("2. Open Firefox and log in to LinkedIn manually")
            print("3. After login, run: python extract_linkedin_cookies.py")
            print("4. Then run this test again")
            return False
            else:
                print("\nCookies work! Testing full flow...")
            
            # Test connection status
            from linkedin_engine import _check_if_connected
            status = _check_if_connected(page)
            print(f"Connection status: {status}")
            
            # Test connect button
            from linkedin_engine import _find_and_click_button
            clicked = _find_and_click_button(
                page, 
                texts=["verbinden", "connect", "kết nối"], 
                aria_keywords=["verbinden", "connect", "kết nối", "mời", "invite"]
            )
            print(f"Clicked connect: {clicked}")
            
            if clicked:
                time.sleep(2)
                final_url = page.url
                print(f"Final URL: {final_url}")
                
                if "custom-invite" in final_url:
                    print("✅ Successfully reached invitation page!")
                    print("\nLinkedIn outreach engine is working correctly!")
                    return True
                else:
                    print("❌ Did not reach invitation page")
            else:
                print("❌ Could not click connect button")
        
        context.close()
        return False

if __name__ == "__main__":
    print("LinkedIn Outreach Engine - Quick Test")
    print("=" * 40)
    
    success = quick_login_test()
    
    if success:
        print("\n✅ All tests passed!")
    else:
        print("\n❌ Test failed - manual login required")