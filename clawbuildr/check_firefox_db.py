import sqlite3
db = sqlite3.connect(r"C:\Users\marvi\AppData\Roaming\Mozilla\Firefox\Profiles\h1vl3oun.default-release\cookies.sqlite")
rows = db.execute(
    "SELECT name, substr(value,1,40), host FROM moz_cookies WHERE host LIKE '%linkedin%' AND name IN ('li_at','liap','bscookie','JSESSIONID')"
).fetchall()
if rows:
    for r in rows:
        print(f"{r[0]}: {r[1]}... ({r[2]})")
else:
    print("No session cookies found!")
    all_names = db.execute("SELECT DISTINCT name FROM moz_cookies WHERE host LIKE '%linkedin%'").fetchall()
    print(f"All linkedin cookie names: {[n[0] for n in all_names]}")
db.close()
