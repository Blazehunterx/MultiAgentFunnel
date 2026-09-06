import json
with open("data/linkedin_cookies.json") as f:
    cookies = json.load(f)
for c in cookies:
    if c["name"] in ["li_at", "liap", "bscookie", "JSESSIONID"]:
        print(f"{c['name']}: domain={c['domain']} secure={c['secure']} httpOnly={c['httpOnly']}")
        print(f"  value={c['value'][:40]}...")
