import sys
sys.path.insert(0, r'C:\Users\marvi\clawbuildr')
from linkedin_engine import generate_linkedin_note

tests = [
    ('Jan', 'TechCorp'),
    ('Marie', 'HealthPlus'),
    ('Peter', ''),
    ('Lisa', 'GreenEnergy'),
]
for first, company in tests:
    note = generate_linkedin_note("", first, company)
    has_dash = ' - ' in note
    print(f"{first}: {note} [dash: {has_dash}]")
