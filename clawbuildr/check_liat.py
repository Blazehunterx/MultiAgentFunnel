import sqlite3
db = sqlite3.connect(r"C:\Users\marvi\AppData\Roaming\Mozilla\Firefox\Profiles\h1vl3oun.default-release\cookies.sqlite")
rows = db.execute("SELECT name, host, value FROM moz_cookies WHERE name IN ('li_at','liap')").fetchall()
for r in rows:
    print(f"name={r[0]} host={r[1]} value={r[2][:30]}...")
if not rows:
    print("NOT FOUND")
db.close()
