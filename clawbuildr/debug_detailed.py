import sys
sys.path.insert(0, r"C:\Users\marvi\clawbuildr")

from linkedin_engine import _prepare_firefox_profile, _check_if_connected, _find_and_click_button
from playwright.sync_api import sync_playwright
import json
import os
import time

def debug_profile_page():
    """Debug the profile page to see what's happening"""
    cookies_path = os.path.join(os.path.dirname(__file__), "data", "linkedin_cookies.json")
    
    if not os.path.exists(cookies_path):
        print("No cookies found")
        return
    
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

        print("=== Navigating to profile ===")
        page.goto("https://www.linkedin.com/in/jan-boone-499310180/", wait_until="domcontentloaded", timeout=90000)
        time.sleep(10)
        
        # Get detailed page info
        page_info = {
            "title": page.title(),
            "url": page.url,
            "h1": page.evaluate("() => document.querySelector('h1')?.innerText?.trim() || ''"),
            "h2s": page.evaluate("() => Array.from(document.querySelectorAll('h2')).map(h => h.innerText.trim()).filter(t => t).slice(0,5)"),
        }
        print(f"Page Info: {json.dumps(page_info, indent=2, ensure_ascii=False)}")
        
        # Check connection status step by step
        print("\n=== Debug _check_if_connected ===")
        status_result = page.evaluate("""() => {
            const els = Array.from(document.querySelectorAll('button, a, [role="button"]'));
            const profileEls = els.filter(el => {
                const r = el.getBoundingClientRect();
                return r.width > 0 && r.height > 0 && r.top < 600 && r.left < 500;
            });
            
            const texts = profileEls.map(el => (el.innerText || '').toLowerCase().trim().normalize('NFC')).filter(t => t);
            const arias = profileEls.map(el => (el.getAttribute('aria-label') || '').toLowerCase().normalize('NFC')).filter(t => t);
            const all = [...texts, ...arias];
            
            console.log('All texts:', texts);
            console.log('All aria:', arias);
            
            // Pending
            const pending = all.some(t => t.includes('afwachting') || t.includes('pending') || t.includes('chờ') ||
                t.includes('ausstehend') || t.includes('attente') || t.includes('pendiente'));
            if (pending) return {status: 'pending', reason: 'found pending text'};
            
            // CHECK CONNECT FIRST
            const hasConnect = all.some(t =>
                t.includes('verbinden') || t.includes('connect') || t.includes('kết nối') ||
                t.includes('se connecter') || t.includes('conectar')
            );
            console.log('Has connect:', hasConnect);
            
            if (hasConnect) return {status: 'not_connected', reason: 'found connect button'};
            
            // If no Connect link but Message exists → already connected
            const hasMessage = all.some(t =>
                t.includes('bericht') || t.includes('message') || t.includes('nhắn tin') || t.includes('tin nhắn') ||
                t.includes('nachricht') || t.includes('mensaje')
            );
            console.log('Has message:', hasMessage);
            
            if (hasMessage) return {status: 'connected', reason: 'found message button but no connect'};
            
            // Follow (not connected, can only follow)
            const hasFollow = all.some(t => t.includes('volgen') || t.includes('follow') || t.includes('theo dõi') ||
                t.includes('folgen') || t.includes('suivre') || t.includes('seguir'));
            if (hasFollow) return {status: 'not_connected', reason: 'found follow button'};
            
            return {status: 'unknown', reason: 'no matching buttons found', all: all.slice(0, 20)};
        }""")
        print(f"Status debug result: {json.dumps(status_result, indent=2, ensure_ascii=False)}")
        
        # Find Connect links manually
        print("\n=== Manual Connect Link Search ===")
        connect_links = page.evaluate("""() => {
            const els = Array.from(document.querySelectorAll('a'));
            return els.filter(el => {
                const href = el.getAttribute('href') || '';
                const txt = (el.innerText || '').toLowerCase().trim();
                const aria = (el.getAttribute('aria-label') || '').toLowerCase();
                const r = el.getBoundingClientRect();
                if (r.width === 0 || r.height === 0) return false;
                if (r.top > 600 || r.left > 500) return false;
                if (href.includes('custom-invite') || txt.includes('verbinden') || txt.includes('connect') || 
                    aria.includes('mời') || aria.includes('connect')) {
                    return {href, txt, aria, top: r.top, left: r.left};
                }
            });
        }""")
        print(f"Connect links: {json.dumps(connect_links, indent=2, ensure_ascii=False)}")
        
        # Test _find_and_click_button directly
        print("\n=== Testing _find_and_click_button ===")
        clicked = _find_and_click_button(page, texts=["verbinden", "connect", "kết nối"], 
                                        aria_keywords=["verbinden", "connect", "kết nối", "mời", "invite"])
        print(f"Clicked connect button: {clicked}")
        
        if clicked:
            time.sleep(3)
            # Check if we're on invitation page
            current_url = page.url
            print(f"Current URL after click: {current_url}")
            
            if "custom-invite" in current_url:
                print("✅ Successfully navigated to invitation page!")
                # Test _handle_invitation_page
                note = "Hi Jan, Ik bouw AI-assistenten voor marketeers. Leuk om te verbinden!"
                # We would call _handle_invitation_page(page, note) here
            else:
                print(f"❌ Not on invitation page. URL: {current_url}")
        
        print("\n=== DONE ===")
        time.sleep(5)
        context.close()

if __name__ == "__main__":
    debug_profile_page()