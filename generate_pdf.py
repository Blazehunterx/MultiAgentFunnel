# -*- coding: utf-8 -*-
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors

def generate_pdf():
    doc = SimpleDocTemplate("Odysseus_Multi_Agent_System.pdf", pagesize=letter)
    styles = getSampleStyleSheet()
    
    # Custom styles
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=22,
        spaceAfter=15,
        textColor=colors.HexColor("#1e3a8a")
    )
    
    heading_style = ParagraphStyle(
        'CustomHeading',
        parent=styles['Heading2'],
        fontSize=16,
        spaceBefore=12,
        spaceAfter=6,
        textColor=colors.HexColor("#2563eb")
    )
    
    subheading_style = ParagraphStyle(
        'CustomSubHeading',
        parent=styles['Heading3'],
        fontSize=12,
        spaceBefore=10,
        spaceAfter=4,
        textColor=colors.HexColor("#1e40af")
    )
    
    normal_style = ParagraphStyle(
        'CustomNormal',
        parent=styles['Normal'],
        fontSize=11,
        spaceAfter=8,
        leading=14
    )
    
    bullet_style = ParagraphStyle(
        'CustomBullet',
        parent=styles['Normal'],
        fontSize=11,
        leftIndent=20,
        spaceAfter=4,
        leading=14,
        bulletIndent=10
    )

    story = []

    # Title
    story.append(Paragraph("Odysseus: Multi-Agent Funnel Overview", title_style))
    story.append(Paragraph("A complete breakdown of the 7-Agent autonomous B2B lead generation system.", normal_style))
    story.append(Spacer(1, 12))

    # Section 1
    story.append(Paragraph("The 7 Autonomous Agents", heading_style))

    agents = [
        ("1. Agent Zero (Sourcing & Ingestion)", 
         "Acts as the top of the funnel. It reads bulk lists of raw data (like Apollo or KVK exports), removes duplicates against your CRM, and applies hard filters (e.g., dropping B2C, ensuring correct company size).<br/><b>Output:</b> A clean, qualified list of pure B2B targets injected into the pipeline."),
        
        ("2. The Research Agent (Web & Context)", 
         "Acts as your SDR. It visits the qualified company's website and scrapes their pages to understand exactly what they do, who their clients are, and what operational bottlenecks they might face.<br/><b>Output:</b> A structured dossier on the company's business model."),
        
        ("3. The Deliverability Agent (The Bouncer)", 
         "Protects your domain reputation. Before any email is drafted, it checks the prospect's MX records, validates the email, and checks for bounce risks.<br/><b>Output:</b> A 'Pass/Fail' flag. If risky, it redirects the system to target the prospect via LinkedIn instead."),
        
        ("4. The Opportunity Mapping Agent (The Strategist)", 
         "Takes the dossier from the Research Agent and maps the prospect's specific business pain points directly to the solutions your company offers.<br/><b>Output:</b> A strategic angle for outreach."),
        
        ("5. The BANT Scoring Agent (The Qualifier)", 
         "Evaluates the prospect using the BANT framework (Budget, Authority, Need, Timing). It looks at the contact's job title and company size to assign a Lead Score from 1-100.<br/><b>Output:</b> Prioritizes leads into 'Send Now', 'Nurture', or 'Blocked'."),
        
        ("6. The Copywriting Agent (Dutch Email Outreach)", 
         "Uses the strategic angle to write a hyper-personalized cold email in fluent B2B Dutch. It avoids generic templates and speaks directly to the prospect's current situation.<br/><b>Output:</b> A ready-to-send draft email loaded into the dashboard."),
        
        ("7. The LinkedIn Networking Agent", 
         "Operates browser automation in the background to log into LinkedIn, visit the prospect's profile, send a connection request with a custom note, and autonomously monitor for follow-ups.<br/><b>Output:</b> A parallel social selling pipeline.")
    ]

    for title, desc in agents:
        story.append(Paragraph(title, subheading_style))
        story.append(Paragraph(desc, normal_style))
    
    story.append(Spacer(1, 12))

    # Section 2
    story.append(Paragraph("How They Are Set Up", heading_style))
    setup_steps = [
        "<b>Environment:</b> Add your API keys to the system (e.g., OpenAI or local LLMs).",
        "<b>Database:</b> The system automatically builds a local SQLite database to store leads securely on your own hardware.",
        "<b>LinkedIn Login:</b> Start the dashboard, go to the LinkedIn tab, and log in once. The system runs silently in the background.",
        "<b>Ingestion:</b> Feed CSV exports into Agent Zero and watch the dashboard populate."
    ]
    for step in setup_steps:
        story.append(Paragraph(f"• {step}", bullet_style))
        
    story.append(Spacer(1, 12))

    # Section 3
    story.append(Paragraph("Required Client Information", heading_style))
    story.append(Paragraph("To make the AI accurate for a specific business, three core things are required:", normal_style))
    
    reqs = [
        ("1. The Value Doctrine (Solutions)", "A document explaining exactly what the company sells, unique value propositions, case studies, and pricing. Without this, the AI won't know what to pitch."),
        ("2. The Ideal Customer Profile (ICP)", "Strict parameters for Agent Zero: Target industries, company size, and decision-maker titles."),
        ("3. Brand Voice & Outreach Rules", "How the company speaks (formal vs. modern Dutch), email length limits, and the primary call-to-action (e.g., calendar booking).")
    ]
    
    for title, desc in reqs:
        story.append(Paragraph(title, subheading_style))
        story.append(Paragraph(desc, normal_style))

    doc.build(story)

if __name__ == '__main__':
    generate_pdf()
