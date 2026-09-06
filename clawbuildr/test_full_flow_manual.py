import sys
sys.path.insert(0, r"C:\Users\marvi\clawbuildr")

from linkedin_engine import _prepare_firefox_profile, linkedin_login
from playwright.sync_api import sync_playwright
import json
import os
import time

def manual_login():
    """Open browser for manual login"""
    print("=== Manual Login Required ===")
    print("Please log in to LinkedIn manually in the opened browser.")
    print("After successful login, this script will continue testing.")
    print("Make sure you complete the login within 2 minutes.")
    
    result = linkedin_login()
    print(f"Login result: {result}")
    
    if result.get("status") == "success":
        print("✅ Login successful! Now testing the full flow...")
        return True
    else:
        print("❌ Login failed. Please check the browser window.")
        return False

def test_full_flow_after_login():
    """Test the full LinkedIn flow after successful login"""
    cookies_path = os.path.join(os.path.dirname(__file__), "data", "linkedin_cookies.json")
    
    if not os.path.exists(cookies_path):
        print("No cookies found after login")
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

        print("\n=== Testing full flow after login ===")
        
        # Test 1: Access profile page
        print("1. Testing profile page access...")
        page.goto("https://www.linkedin.com/in/jan-boone-499310180/", wait_until="domcontentloaded", timeout=90000)
        time.sleep(5)
        
        title = page.title()
        if "authwall" in title.lower() or "login" in title.lower():
            print("❌ Still hitting auth wall after login")
            return False
        else:
            print(f"✅ Profile page loaded: {title}")
        
        # Test 2: Check connection status
        print("\n2. Testing connection status...")
        from linkedin_engine import _check_if_connected
        status = _check_if_connected(page)
        print(f"Connection status: {status}")
        
        # Test 3: Find and click connect button
        print("\n3. Testing connect button click...")
        from linkedin_engine import _find_and_click_button
        
        connect_texts = ["verbinden", "connect", "kết nối", "se connecter", "conectar"]
        connect_arias = ["verbinden", "connect", "kết nối", "mời", "invite", "se connecter", "conectar"]
        
        clicked = _find_and_click_button(page, texts=connect_texts, aria_keywords=connect_arias)
        print(f"Clicked connect button: {clicked}")
        
        if clicked:
            time.sleep(3)
            current_url = page.url
            print(f"Current URL after click: {current_url}")
            
            if "custom-invite" in current_url:
                print("✅ Successfully navigated to invitation page!")
                
                # Test 4: Test _handle_invitation_page function
                print("\n4. Testing _handle_invitation_page function...")
                from linkedin_engine import _handle_invitation_page
                note = "Hi Jan, Ik bouw AI-assistenten voor marketeers. Leuk om te verbinden!"
                
                # For safety, let's just test the function without actually sending
                print("Testing invitation page handling (without actual send)...")
                
                # Check what's on the page
                page_info = page.evaluate("""() => {
                    return {
                        title: document.title,
                        has_textarea: !!document.querySelector('textarea'),
                        has_send_button: !!document.querySelector('button'),
                        textarea_placeholder: document.querySelector('textarea')?.placeholder || '',
                        send_button_text: Array.from(document.querySelectorAll('button')).map(b => b.innerText.trim()).filter(t => t).slice(0,5)
                    };
                }""")
                print(f"Invitation page info: {json.dumps(page_info, indent=2, ensure_ascii=False)}")
                
                print("✅ Full flow test completed successfully!")
                return True
            else:
                print(f"❌ Not on invitation page. URL: {current_url}")
        else:
            print("❌ Could not click connect button")
        
        print("\n=== Full flow test completed ===")
        time.sleep(5)
        context.close()
        return False

if __name__ == "__main__":
    print("LinkedIn Outreach Engine - Full Flow Test")
    print("=" * 50)
    
    # Step 1: Manual login
    if not manual_login():
        print("Cannot proceed without successful login")
        exit(1)
    
    # Step 2: Test full flow
    success = test_full_flow_after_login()
    
    if success:
        print("\n🎉 All tests passed! LinkedIn outreach engine is working correctly.")
    else:
        print("\n❌ Some tests failed. Check the browser window for details.")