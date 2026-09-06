"""
Directly read LinkedIn cookies from Firefox's cookies.sqlite database.
This bypasses browser_cookie3 and works even while Firefox is running.
"""
import sqlite3
import os
import json
import shutil

FIREFOX_PROFILE_SRC = r"C:\Users\marvi\AppData\Roaming\Mozilla\Firefox\Profiles\h1vl3oun.default-release"
COOKIES_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "linkedin_cookies.json")
FIREFOX_COOKIES_DB = os.path.join(FIREFOX_PROFILE_SRC, "cookies.sqlite")


def extract_cookies_direct():
    """Read LinkedIn cookies directly from cookies.sqlite using WAL mode."""
    if not os.path.exists(FIREFOX_COOKIES_DB):
        print(f"ERROR: cookies.sqlite not found at {FIREFOX_COOKIES_DB}")
        return []

    # Copy to temp so we don't lock Firefox's database
    tmp_db = os.path.join(os.path.dirname(COOKIES_PATH), "cookies_copy.sqlite")
    shutil.copy2(FIREFOX_COOKIES_DB, tmp_db)

    try:
        db = sqlite3.connect(f"file:{tmp_db}?mode=ro", uri=True)
        db.row_factory = sqlite3.Row

        rows = db.execute("""
            SELECT name, value, host, path, expiry, isSecure, isHttpOnly
            FROM moz_cookies
            WHERE host LIKE '%linkedin.com%'
        """).fetchall()
        db.close()

        cookies = []
        for row in rows:
            domain = row["host"]
            if not domain.startswith("."):
                domain = "." + domain.lstrip(".")

            cookie = {
                "name": row["name"],
                "value": row["value"],
                "domain": domain,
                "path": row["path"] or "/",
                "secure": bool(row["isSecure"]),
                "httpOnly": bool(row["isHttpOnly"]),
                "sameSite": "None" if bool(row["isSecure"]) else "Lax",
            }

            expires = row["expiry"]
            if expires and expires > 0:
                # Normalize: if > 10 trillion, it's in milliseconds
                if expires > 10000000000:
                    expires = expires // 1000
                cookie["expires"] = int(expires)
            else:
                cookie["expires"] = -1

            cookies.append(cookie)

        print(f"Extracted {len(cookies)} LinkedIn cookies from cookies.sqlite")

        # Check for critical session cookies
        critical = ["li_at", "liap", "bscookie", "JSESSIONID"]
        found = [c["name"] for c in cookies if c["name"] in critical]
        print(f"Session cookies found: {found}")

        # Filter and deduplicate
        li_cookies = [c for c in cookies if "linkedin" in c.get("domain", "")]
        dedup = {}
        for c in li_cookies:
            key = (c["name"], c["domain"])
            if key not in dedup or c.get("expires", -1) > dedup[key].get("expires", -1):
                dedup[key] = c
        li_cookies = list(dedup.values())

        has_session = any(c["name"] in ("li_at", "liap", "bscookie") for c in li_cookies)
        print(f"Session cookies: {[c['name'] for c in li_cookies if c['name'] in ['li_at','liap','bscookie','JSESSIONID']]}")

        os.makedirs(os.path.dirname(COOKIES_PATH), exist_ok=True)
        with open(COOKIES_PATH, "w") as f:
            json.dump(li_cookies, f, indent=2)
        print(f"Saved {len(li_cookies)} deduped cookies to {COOKIES_PATH}")

        return li_cookies

    except Exception as e:
        print(f"ERROR reading cookies.sqlite: {e}")
        return []
    finally:
        try:
            os.remove(tmp_db)
        except Exception:
            pass


if __name__ == "__main__":
    extract_cookies_direct()
