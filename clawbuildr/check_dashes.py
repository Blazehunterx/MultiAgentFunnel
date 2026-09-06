import sqlite3
conn = sqlite3.connect(r'C:\Users\marvi\odysseus\data\clawbuildr.db')
cur = conn.cursor()
rows = cur.execute('SELECT id, content FROM linkedin_posts WHERE id BETWEEN 26 AND 36').fetchall()
for r in rows:
    post_id, content = r
    has_dash = ' - ' in content or chr(8212) in content
    print(f"ID {post_id}: {'HAS DASH' if has_dash else 'OK'}")
conn.close()
