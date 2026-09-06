import json
with open("data/linkedin_cookies.json") as f:
    cookies = json.load(f)
dedup = {}
for c in cookies:
    key = (c["name"], c["domain"])
    if key not in dedup or c.get("expires", -1) > dedup[key].get("expires", -1):
        dedup[key] = c
result = list(dedup.values())
print(f"Before: {len(cookies)}, After: {len(result)}")
session = [c for c in result if c["name"] in ["li_at", "liap", "bscookie", "JSESSIONID"]]
for c in session:
    print(f"  {c['name']}: domain={c['domain']} httpOnly={c['httpOnly']} secure={c['secure']}")
with open("data/linkedin_cookies.json", "w") as f:
    json.dump(result, f, indent=2)
print(f"Saved {len(result)} deduped cookies")
