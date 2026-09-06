import sqlite3
db = sqlite3.connect(r"C:\Users\marvi\AppData\Roaming\Mozilla\Firefox\Profiles\h1vl3oun.default-release\cookies.sqlite")
rows = db.execute("""
    SELECT name, value, host, path, expiry, isSecure, isHttpOnly
    FROM moz_cookies
    WHERE host LIKE '%linkedin.com%'
""").fetchall()
print(f"Total rows: {len(rows)}")
linkedin_names = set()
for r in rows:
    linkedin_names.add(r[0])
    if r[0] in ("li_at", "liap"):
        print(f"  FOUND: name={r[0]} host={r[2]} value={r[1][:30]}... secure={r[5]} httpOnly={r[6]}")
print(f"\nAll names: {sorted(linkedin_names)}")
db.close()
