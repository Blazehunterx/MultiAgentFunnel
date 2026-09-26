# -*- coding: utf-8 -*-
"""Generate a readable PDF overview of enrichment data used for outbound emails."""
import os
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, KeepTogether, HRFlowable, ListFlowable, ListItem,
)

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                   "docs", "email_enrichment_data_overview.pdf")

NAVY = colors.HexColor("#0f172a")
BLUE = colors.HexColor("#2563eb")
LIGHT_BLUE = colors.HexColor("#eff6ff")
SLATE = colors.HexColor("#334155")
MUTED = colors.HexColor("#64748b")
GREEN = colors.HexColor("#059669")
AMBER = colors.HexColor("#d97706")
RED = colors.HexColor("#dc2626")
ROW_ALT = colors.HexColor("#f8fafc")
BORDER = colors.HexColor("#e2e8f0")
WHITE = colors.white


def build_styles():
    ss = getSampleStyleSheet()
    styles = {
        "title": ParagraphStyle(
            "TitleX", parent=ss["Title"], fontName="Helvetica-Bold",
            fontSize=22, leading=28, textColor=NAVY, spaceAfter=4, alignment=TA_LEFT,
        ),
        "subtitle": ParagraphStyle(
            "SubX", parent=ss["Normal"], fontName="Helvetica",
            fontSize=11, leading=15, textColor=MUTED, spaceAfter=14,
        ),
        "h1": ParagraphStyle(
            "H1X", parent=ss["Heading1"], fontName="Helvetica-Bold",
            fontSize=15, leading=20, textColor=BLUE, spaceBefore=16, spaceAfter=8,
        ),
        "h2": ParagraphStyle(
            "H2X", parent=ss["Heading2"], fontName="Helvetica-Bold",
            fontSize=12, leading=16, textColor=NAVY, spaceBefore=10, spaceAfter=6,
        ),
        "body": ParagraphStyle(
            "BodyX", parent=ss["Normal"], fontName="Helvetica",
            fontSize=10, leading=14, textColor=SLATE, spaceAfter=6,
        ),
        "bullet": ParagraphStyle(
            "BulX", parent=ss["Normal"], fontName="Helvetica",
            fontSize=10, leading=14, textColor=SLATE, leftIndent=12, spaceAfter=3,
        ),
        "cell": ParagraphStyle(
            "CellX", parent=ss["Normal"], fontName="Helvetica",
            fontSize=9, leading=12, textColor=SLATE,
        ),
        "cell_b": ParagraphStyle(
            "CellBX", parent=ss["Normal"], fontName="Helvetica-Bold",
            fontSize=9, leading=12, textColor=NAVY,
        ),
        "cell_hdr": ParagraphStyle(
            "CellHX", parent=ss["Normal"], fontName="Helvetica-Bold",
            fontSize=9, leading=12, textColor=WHITE,
        ),
        "callout": ParagraphStyle(
            "CallX", parent=ss["Normal"], fontName="Helvetica",
            fontSize=10, leading=14, textColor=NAVY, leftIndent=6, rightIndent=6,
        ),
        "footer": ParagraphStyle(
            "FootX", parent=ss["Normal"], fontName="Helvetica",
            fontSize=8, leading=10, textColor=MUTED, alignment=TA_CENTER,
        ),
        "ok": ParagraphStyle(
            "OkX", parent=ss["Normal"], fontName="Helvetica-Bold",
            fontSize=10, leading=13, textColor=GREEN,
        ),
        "warn": ParagraphStyle(
            "WarnX", parent=ss["Normal"], fontName="Helvetica-Bold",
            fontSize=10, leading=13, textColor=AMBER,
        ),
        "bad": ParagraphStyle(
            "BadX", parent=ss["Normal"], fontName="Helvetica-Bold",
            fontSize=10, leading=13, textColor=RED,
        ),
    }
    return styles


def P(text, style):
    return Paragraph(text, style)


def bullets(items, style):
    return ListFlowable(
        [ListItem(P(x, style), leftIndent=14, value="circle") for x in items],
        bulletType="bullet", start="circle", leftIndent=14, bulletFontSize=6,
        spaceBefore=2, spaceAfter=6,
    )


def make_table(headers, rows, col_widths, styles):
    hdr = [P(h, styles["cell_hdr"]) for h in headers]
    data = [hdr]
    for r in rows:
        data.append([P(str(c), styles["cell"]) if not isinstance(c, tuple)
                     else P(str(c[0]), styles[c[1]]) for c in r])
    t = Table(data, colWidths=col_widths, repeatRows=1)
    cmds = [
        ("BACKGROUND", (0, 0), (-1, 0), NAVY),
        ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("ALIGN", (0, 0), (-1, 0), "LEFT"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("GRID", (0, 0), (-1, -1), 0.4, BORDER),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]
    for i in range(1, len(data)):
        if i % 2 == 0:
            cmds.append(("BACKGROUND", (0, i), (-1, i), ROW_ALT))
    t.setStyle(TableStyle(cmds))
    return t


def callout(text, styles, bg=LIGHT_BLUE, border=BLUE):
    inner = Table([[P(text, styles["callout"])]], colWidths=[170 * mm])
    inner.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), bg),
        ("BOX", (0, 0), (-1, -1), 1, border),
        ("LEFTPADDING", (0, 0), (-1, -1), 10),
        ("RIGHTPADDING", (0, 0), (-1, -1), 10),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
    ]))
    return KeepTogether([inner, Spacer(1, 8)])


def footer(canvas, doc):
    canvas.saveState()
    canvas.setStrokeColor(BORDER)
    canvas.setLineWidth(0.5)
    canvas.line(18 * mm, 14 * mm, A4[0] - 18 * mm, 14 * mm)
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(MUTED)
    canvas.drawString(18 * mm, 9 * mm, "ClawBuildr — Email Enrichment Data Overview")
    canvas.drawRightString(A4[0] - 18 * mm, 9 * mm, f"Page {doc.page}")
    canvas.restoreState()


def build():
    styles = build_styles()
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    doc = SimpleDocTemplate(
        OUT, pagesize=A4,
        leftMargin=18 * mm, rightMargin=18 * mm,
        topMargin=16 * mm, bottomMargin=20 * mm,
        title="Email Enrichment Data Overview",
        author="ClawBuildr",
    )
    story = []

    # ── Cover ──
    story.append(P("Email Enrichment Data Overview", styles["title"]))
    story.append(P(
        "Welke data we nu gebruiken voor gepersonaliseerde outbound e-mails, "
        "wat we van LinkedIn kunnen scrapen, wat we van bedrijven nodig hebben, "
        "en waar de huidige gaps zitten.",
        styles["subtitle"],
    ))
    story.append(HRFlowable(width="100%", thickness=1.5, color=BLUE, spaceAfter=12))

    story.append(callout(
        "<b>Doel van dit document:</b> overzicht voor het team — wat gaat er de e-mail in, "
        "welke firmographics/pain-points zijn al beschikbaar, welke LinkedIn-velden kunnen we "
        "ophalen, en welke stappen zetten we door naar de leads-pipeline.",
        styles,
    ))

    # ── 1. What goes into the email ──
    story.append(P("1. Wat gaat er nu de e-mail in?", styles["h1"]))
    story.append(P(
        "De AI-emailgenerator (Gemini) bouwt subject + body uit onderstaande context. "
        "Regel: open met 1 concreet feit, koppel aan de pijn van de ontvanger, "
        "CTA = lage drempel (“10 minuten bellen”).",
        styles["body"],
    ))

    story.append(make_table(
        ["Veld", "Bron", "Voorbeeld / rol in de mail"],
        [
            ["Company + domain", "companies", "“Deloitte” · deloitte.nl — verplicht in subject/body"],
            ["Contact naam + rol", "contacts", "Frits · CEO — aanhef en authority-signal"],
            ["Research findings (max 1500 chars)", "research_result", "summary, services, signals — opening hook"],
            ["ICP context", "tenant_config", "industrie, size, roles — targeting-filter"],
            ["Opportunity pains / hook", "opportunity_mapping", "pain_points, strongest_hook — probleemkoppeling"],
            ["LinkedIn personalization hooks", "research (LI)", "post, vacatures, mutual connections, headline, tenure"],
            ["BANT / SPIN", "qualification_assessment", "score + prioriteit — NIET in de mailtekst"],
            ["A/B subject variants", "outreach_draft", "subject_line_A/B/C + body step 1"],
        ],
        [48 * mm, 38 * mm, 84 * mm],
        styles,
    ))
    story.append(Spacer(1, 8))

    story.append(P("Onderwerpregels in de AI-prompt", styles["h2"]))
    story.append(bullets([
        "Max 120 woorden, informeel-professioneel (je/jullie).",
        "NOOIT placeholders ([Naam], [Bedrijf]) of intern jargon (BANT, score).",
        "Open met een SPECIFIEK feit uit LinkedIn/research — geen generieke observatie.",
        "Koppel feit → constraint (meer volume = meer handwerk) → onze operating layer.",
        "Banned words: vervangen, automatiseren, personeel snijden, FTE-reductie, efficiëntie, monitoren…",
        "Geen “exclusive page” / 80% korting / suggestieve toon (Instagram/LinkedIn-vlags).",
    ], styles["bullet"]))

    # ── 2. LinkedIn scrape ──
    story.append(P("2. Wat we van LinkedIn kunnen scrapen", styles["h1"]))
    story.append(P(
        "De scrape-code bestaat al (Firefox + human-like scrolling). "
        "Velden hieronder zijn beschikbaar in de modules — zie sectie 5 voor de dekkingsgaps in de DB.",
        styles["body"],
    ))

    story.append(P("2.1 Persoonsprofiel (read_linkedin_profile.py)", styles["h2"]))
    story.append(make_table(
        ["Veld", "Gebruik in outreach"],
        [
            ["name, headline, location", "aanhef, seniority, geo-personalisatie"],
            ["about (Over sectie)", "context over expertise / rol"],
            ["current_role, current_company, tenure", "role seniority + “sinds wanneer in functie”"],
            ["experience[], education[], skills[], posts[]", "openings-hook uit carrière of content"],
            ["mutual_connections_count/text", "warm intro (“we kennen X”)"],
            ["connection_status", "of we al 1e graad zijn voor DM"],
        ],
        [70 * mm, 100 * mm],
        styles,
    ))

    story.append(P("2.2 Bedrijfspagina (read_linkedin_company.py)", styles["h2"]))
    story.append(make_table(
        ["Veld", "Gebruik in outreach / ICP"],
        [
            ["name, industry", "branchematch, subject-line context"],
            ["size / employee_count_text", "bedrijfsgrootte vs ICP (bijv. 10–50)"],
            ["headquarters", "locatie / regio-filter (NL)"],
            ["founded", "maturity / groeifase"],
            ["specialties", "diensten → pijn-koppeling"],
            ["about_text", "bedrijfssamenvatting voor research"],
            ["website, followers", "kanalen + bereik-signal"],
            ["hiring_count + hiring_jobs[]", "groei-signal → “jullie werven, processen haperen”"],
            ["posts[]", "recente bedrijfspost als hook"],
            ["People-tab: employees[] (name, title, url)", "beslisser-selectie (tot 50)"],
        ],
        [70 * mm, 100 * mm],
        styles,
    ))

    story.append(P("2.3 Combined helper (enrich_lead.py)", styles["h2"]))
    story.append(bullets([
        "ICP signals: company_size_category (startup/sme/midmarket/large/enterprise), industry, hiring_signal (high/medium/low/none), role_seniority, tenure, mutual_connections.",
        "Personalization snippets: recent_post, company_recent_post, company_context, warmth (mutual connections).",
        "Opslag: tools.save_linkedin_enrichment() schrijft naar companies.* + contacts.linkedin_* — maar in de huidige DB staan die firmographics grotendeels leeg (zie §5).",
    ], styles["bullet"]))

    story.append(PageBreak())

    # ── 3. Company data needed ──
    story.append(P("3. Wat we van bedrijven nodig hebben (ICP)", styles["h1"]))
    story.append(P(
        "Uit tenant_config + onboarding. Dit bepaalt targeting én welke pain-presets "
        "standaard in de sequence-prompt zitten.",
        styles["body"],
    ))

    story.append(make_table(
        ["Categorie", "Huidige waarden (tenant clawbuildr / injexion)"],
        [
            ["Industries", "IT, Software, SaaS, Consultancy, Managed Services, Cloud, Cybersecurity "
                           "(injexion: Groothandel, Logistiek & Transport, B2B SaaS, IT Dienstverlening)"],
            ["Company size", "10–50 (clawbuildr) of 10–200; onboarding presets 1-10 / 10-50 / 50-200 / 200+"],
            ["Roles / decision makers", "CEO, Directeur, Eigenaar, Oprichter"],
            ["Regions", "nl (Nederland)"],
            ["Pain presets (onboarding chips)", "Te weinig tijd administratie · Handmatig prospecten · "
                                                 "Leads reageren niet · Online zichtbaarheid · "
                                                 "Hoge kosten per lead · Concurrentie sneller"],
        ],
        [48 * mm, 122 * mm],
        styles,
    ))
    story.append(Spacer(1, 8))

    story.append(P("3.1 Firmographics die we per lead willen hebben", styles["h2"]))
    story.append(make_table(
        ["Veld", "Waarom", "Bron"],
        [
            ["company_size / employee count", "ICP-filter + “groei zonder extra handen”", "LinkedIn company page"],
            ["location / HQ", "geo-personalisatie, NL-filter", "LinkedIn HQ + website footer"],
            ["industry / SBI", "branchematch, subject context", "LinkedIn + KVK + website classify"],
            ["founded year", "maturity, groeifase", "LinkedIn About / KVK"],
            ["specialties / services", "pijnkoppeling (dienst X → pijn Y)", "LinkedIn + website /diensten"],
            ["open vacancies (hiring)", "groeisignal = sterkste hook", "LinkedIn Jobs"],
            ["recent posts (persoon + bedrijf)", "opening hook, netheid", "LinkedIn feed"],
            ["pain points (evidence-based)", "kern van de body-tekst", "research + opportunity mapping"],
            ["tech stack / tools (optioneel)", "vergelijking, gap-analyse", "website / BuiltWith / jobs"],
            ["contact email + deliverability", "verzending + bounce-risico", "Hunter / website scrape / MX"],
        ],
        [48 * mm, 62 * mm, 60 * mm],
        styles,
    ))

    # ── 4. Research pipeline ──
    story.append(P("4. Research-pipeline (hoe data samenkomen)", styles["h1"]))
    story.append(P(
        "research_company() in pipeline.py combineert meerdere bronnen parallel "
        "en schrijft het resultaat naar contacts.research_result.",
        styles["body"],
    ))
    story.append(bullets([
        "<b>Website deep scrape</b> — homepage + prioriteitspagina’s (/about, /team, /contact, /producten) → tekst, e-mails, socials, telefoons.",
        "<b>Google signals</b> — queries op vacatures/hiring, nieuws, klanten/cases → snippets in signals[].",
        "<b>LinkedIn company</b> — size, HQ, about, specialties, hiring, posts.",
        "<b>LinkedIn profile</b> — als linkedin_url bekend is: headline, tenure, posts, mutuals.",
        "<b>KVK lookup</b> — NL handelsregister (timeout 10s).",
        "<b>→ ResearchData</b> → <b>map_opportunity</b> (pain_points + ROI) → <b>qualify</b> (BANT/SPIN) → <b>write_email</b>.",
    ], styles["bullet"]))

    story.append(callout(
        "<b>Belangrijk:</b> opportunity_mapping en qualification_assessment worden nog maar "
        "voor een klein deel van de leads gevuld (zie §5). Zonder pains in research/opportunity "
        "valt de opening terug op generieke signalen — wat de personalisatie merkbaar verzwakt.",
        styles,
        bg=colors.HexColor("#fff7ed"), border=AMBER,
    ))

    # ── 5. Coverage / gaps ──
    story.append(P("5. Huidige dekkingscijfers (DB)", styles["h1"]))
    story.append(P(
        "Stand peiling op MultiAgentFunnel/data/clawbuildr.db — dit is de kern van het probleem: "
        "de LinkedIn-scrape-code bestaat, maar firmographics worden nauwelijkt persistent opgeslagen.",
        styles["body"],
    ))

    story.append(make_table(
        ["Data", "Gevuld", "Status"],
        [
            ["companies.industry", "1352 / 1352", ("OK", "ok")],
            ["companies.estimated_size", "0 / 1352", ("LEEG", "bad")],
            ["companies.headquarters", "0 / 1352", ("LEEG", "bad")],
            ["companies.founded", "0 / 1352", ("LEEG", "bad")],
            ["companies.specialties", "0 / 1352", ("LEEG", "bad")],
            ["companies.employee_count_text", "0 / 1352", ("LEEG", "bad")],
            ["companies.hiring_count", "0 / 1352", ("LEEG", "bad")],
            ["companies.company_posts_json", "0 / 1352", ("LEEG", "bad")],
            ["contacts.linkedin_url", "122 / 1343 (~9%)", ("LAAG", "warn")],
            ["contacts.research_result", "845", ("DEKT", "ok")],
            ["contacts.outreach_draft", "829", ("DEKT", "ok")],
            ["contacts.opportunity_mapping", "68", ("LAAG", "warn")],
            ["contacts.qualification_assessment", "68", ("LAAG", "warn")],
        ],
        [70 * mm, 55 * mm, 45 * mm],
        styles,
    ))
    story.append(Spacer(1, 8))

    story.append(P("Wat dit betekent voor de e-mail", styles["h2"]))
    story.append(bullets([
        "Research leunt nu vooral op website-tekst + generieke signals → vage openings zoals “IT is actief in …”.",
        "LinkedIn size / HQ / specialties staan wél als sleutels in research_result, maar zijn leeg omdat de company-scrape niet betrouwbaar meedraait bij insert.",
        "Zonder linkedin_url (~91% van contacts) geen profile-hooks (post, tenure, mutuals).",
        "Opportunity pains (opportunity_mapping) bereiken maar ~5% van de leads → body valt terug op prefab/LLM-generiek.",
    ], styles["bullet"]))

    # ── 6. Next steps ──
    story.append(P("6. Aanbevolen stappen → doorzetten naar leads", styles["h1"]))
    story.append(make_table(
        ["#", "Actie", "Effect"],
        [
            ["1", "Forceer LinkedIn firmographics bij insert/update "
                  "(estimated_size, headquarters, founded, specialties, hiring_count, posts) op companies",
             "Size/location/problems standaard beschikbaar voor elke nieuwe lead"],
            ["2", "Verplicht / verrijkt linkedin_url op contacts (nu ~9%)",
             "Profile-hooks: post, tenure, mutual connections in de opening"],
            ["3", "Stop of pause lege research — min. data_quality_score / summary-lengte vóór send",
             "Geen generieke “actief in …”-opener meer"],
            ["4", "Draai opportunity_mapping breder (nu 68 vs 845 research)",
             "Concrete pain_points + ROI in de body-tekst"],
            ["5", "Toon size / location / hiring op lead card + drawer (Industry-veld bestaat al)",
             "Snellere menselijke review in de CRM-UI"],
            ["6", "Optioneel: tech stack + cases meenemen in research-prompt",
             "Scherpere pijn-koppeling voor SaaS/IT-ICP"],
        ],
        [10 * mm, 95 * mm, 65 * mm],
        styles,
    ))
    story.append(Spacer(1, 10))

    story.append(callout(
        "<b>Samenvatting voor het team:</b> de pipeline en de scrape-modules zijn er al. "
        "De quick win is <b>persistentie + dekking</b>: vul firmographics bij elke lead, "
        "haal linkedin_url op, en stuur niet door naar outreach zolang research/pains te dun zijn. "
        "Daarna wordt personalisatie meetbaar sterker zonder nieuwe scrapelogica te schrijven.",
        styles,
    ))

    story.append(Spacer(1, 14))
    story.append(HRFlowable(width="100%", thickness=0.8, color=BORDER, spaceAfter=6))
    story.append(P(
        "Bronnen: clawbuildr/pipeline.py · models.py · enrich_lead.py · read_linkedin_profile.py · "
        "read_linkedin_company.py · clawbuildr_ai_email.py · tools.py · tenant_config / onboarding_state · "
        "fill-rate query op clawbuildr.db (1352 companies, 1343 contacts).",
        styles["footer"],
    ))

    doc.build(story, onFirstPage=footer, onLaterPages=footer)
    print("Wrote", OUT)
    print("Size", os.path.getsize(OUT), "bytes")


if __name__ == "__main__":
    build()
