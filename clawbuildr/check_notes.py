import sqlite3

conn = sqlite3.connect(r'C:\Users\marvi\odysseus\data\clawbuildr.db')
cur = conn.cursor()

# Get recent notes
print("=== RECENT CONNECTION NOTES (last 20) ===\n")
rows = cur.execute("""
    SELECT id, first_name, last_name, company, note, outcome, connection_status
    FROM linkedin_outreach
    WHERE note IS NOT NULL AND length(note) > 10
    ORDER BY id DESC
    LIMIT 20
""").fetchall()

for r in rows:
    print(f"#{r[0]}: {r[1]} {r[2]} | outcome={r[5]} | status={r[6]}")
    print(f"  Company: {r[3][:60]}")
    print(f"  Note: {r[4]}")
    print()

# Note length stats
print("\n=== NOTE LENGTH STATS ===")
lengths = cur.execute("SELECT length(note) FROM linkedin_outreach WHERE note IS NOT NULL AND length(note) > 10").fetchall()
lengths = [l[0] for l in lengths]
if lengths:
    print(f"  Count: {len(lengths)}")
    print(f"  Min: {min(lengths)} chars")
    print(f"  Max: {max(lengths)} chars")
    print(f"  Avg: {sum(lengths)//len(lengths)} chars")
    # Distribution
    under_100 = len([l for l in lengths if l < 100])
    between_100_150 = len([l for l in lengths if 100 <= l < 150])
    between_150_180 = len([l for l in lengths if 150 <= l < 180])
    over_180 = len([l for l in lengths if l >= 180])
    print(f"  Under 100: {under_100}")
    print(f"  100-149: {between_100_150}")
    print(f"  150-179: {between_150_180}")
    print(f"  180+: {over_180}")

# Check which notes mention ClawBuildr vs FounderFlow
print("\n=== NOTE CONTENT ANALYSIS ===")
clawbuildr_notes = cur.execute("SELECT COUNT(*) FROM linkedin_outreach WHERE note LIKE '%ClawBuildr%'").fetchone()[0]
founderflow_notes = cur.execute("SELECT COUNT(*) FROM linkedin_outreach WHERE note LIKE '%FounderFlow%'").fetchone()[0]
print(f"  Notes mentioning ClawBuildr: {clawbuildr_notes}")
print(f"  Notes mentioning FounderFlow: {founderflow_notes}")

# Check accepted vs rejected notes
print("\n=== ACCEPTED (7 people) - their notes ===")
accepted = cur.execute("""
    SELECT id, first_name, last_name, note
    FROM linkedin_outreach
    WHERE connection_status IN ('accepted', 'connected')
""").fetchall()
for r in accepted:
    print(f"#{r[0]}: {r[1]} {r[2]}")
    print(f"  Note: {r[3]}")
    print()

conn.close()
