import sys
sys.path.insert(0, r'C:\Users\marvi\clawbuildr')

from linkedin_engine import generate_linkedin_note, _clean_company_name

print("=== Company name cleaning ===")
tests = [
    'Placetel  it',
    'Apotheek apotheek.be Apotheek.be',
    'Happy Horizon happyhorizon.com',
    'Shopify shopify.com',
    'ANP Connect anp.nl',
    'Bouwbedrijf Gelens',
    'WYS adviseurs wysadviseurs.nl',
    'ROXTAR roxtar.nl',
    'Kliniekzoeker kliniekzoeker.nl',
    'M+R mplusr.nl',
    'Steuerberaten steuerberaten.de',
    'Pharmabox pharmabox.be',
]
for t in tests:
    cleaned = _clean_company_name(t)
    print(f"  {t[:50]} -> '{cleaned}'")

print("\n=== Note generation ===")
notes = [
    ('Jan', 'TechCorp'),
    ('Marie', 'HealthPlus'),
    ('Peter', ''),  # No company
    ('Lisa', 'GreenEnergy'),
    ('Menno', 'Apotheek apotheek.be'),  # Garbage company
    ('Antonio', 'Placetel  it'),  # Garbage company
]
for first, company in notes:
    note = generate_linkedin_note("", first, company)
    print(f"  {first} ({company[:25]}): {note}")

print("\n=== Check if notes are general and curious ===")
note = generate_linkedin_note("", "Sophie", "GreenTech")
print(f"  Sample note: {note}")
if note:
    has_salesy = any(w in note.lower() for w in ['clawbuildr', 'founderflow', 'ai-automatisering', 'gratis', 'call', 'demo'])
    print(f"  Contains salesy words: {has_salesy}")
    print(f"  Length: {len(note)} chars")
