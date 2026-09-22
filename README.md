# ClawBuildr — Autonomous B2B Outreach Platform

An autonomous, multi-agent outbound intelligence and B2B lead generation system for the Dutch B2B market.

Uses AI agents to autonomously discover leads, qualify them, perform deep web research, and execute personalized multi-channel outreach (Email + LinkedIn).

**Live Demo:** https://deutsche-match-billing-strategy.trycloudflare.com

## Key Features

- **Simple Onboarding**: Skylead-style setup wizard — company, ICP, message, search terms, launch
- **Simple Dashboard**: Clean, calm UI focused on campaigns, leads, and results (no noise)
- **Lead Generation**: Scrapes Dutch business directories, team pages, and hunter.io for decision-maker emails
- **AI Email Generation**: Gemini-powered hyper-personalized cold emails with tone/length targets
- **LinkedIn Automation**: Browser-based connection requests, follow-ups, and InMail sequences
- **Email Tracking**: Open/reply/bounce detection via Gmail IMAP
- **Follow-up Engine**: 48h/96h/168h automated follow-up sequences
- **Classic Dashboard**: Full Mission Control still available at `/classic`

## Quick Start (3 steps)

### 1. Clone & Install

```bash
git clone https://github.com/Blazehunterx/MultiAgentFunnel.git
cd MultiAgentFunnel
python -m venv .venv
.venv\Scripts\activate     # Windows
# source .venv/bin/activate  # macOS/Linux
pip install -r clawbuildr/requirements.txt
playwright install chromium
```

### 2. Configure Credentials

```bash
cp clawbuildr/.env.example clawbuildr/.env
```

Edit `clawbuildr/.env` with your own keys:

| Variable | Description | Get it from |
|----------|-------------|-------------|
| `GMAIL_USER` | Your Gmail address | Gmail |
| `GMAIL_PASSWORD` | Gmail App Password (not your login password) | Gmail → Security → App passwords |
| `GEMINI_API_KEY1` | Google Gemini API key | https://aistudio.google.com/apikey |
| `HUNTER_API_KEY` | Hunter.io API key for lead discovery | https://hunter.io/api-keys |

### 3. Run

```bash
# Terminal 1: Dashboard
cd data
python -m uvicorn clawbuildr_dashboard:app --host 0.0.0.0 --port 8000

# Terminal 2: Pipeline (24/7 outreach loop)
python clawbuildr/pipeline_runner.py
```

Open **http://localhost:8000** in your browser. First time you land on the onboarding wizard; after that on the simple dashboard.

The classic dashboard is still available at `/classic`.

## What the Pipeline Does

Runs continuously in 30-45 minute cycles:

1. **Lead Discovery** — Scrapes Dutch business directories + team pages for decision-maker emails
2. **Email Verification** — MX lookup + SMTP RCPT TO validation
3. **AI Email Generation** — Gemini drafts personalized emails per contact
4. **Email Sending** — Gmail SMTP with 15-30s delays between sends
5. **Follow-ups** — 48h / 96h / 168h automated sequences
6. **Reply Detection** — Gmail IMAP polling, auto-stops follow-ups on reply
7. **LinkedIn** — Connection requests + follow-up messages

## Rate Limits (Free Tier)

- Gemini: ~50 requests/day (6 API keys with rotation)
- Hunter.io: ~50 lookups/day (2 keys with rotation)
- Gmail SMTP: ~500 emails/day
- LinkedIn: ~20-30 connection requests/day

## Architecture

```
clawbuildr/
├── pipeline_runner.py          # Main 24/7 loop
├── clawbuildr_dashboard.py     # FastAPI dashboard (port 8000)
├── clawbuildr_ai_email.py      # Gemini email generation
├── clawbuildr_lead_generator.py# Lead scraping + verification
├── clawbuildr_reply_detector.py# IMAP reply/bounce detection
├── clawbuildr_scheduler.py     # Follow-up scheduling
├── linkedin_engine.py          # LinkedIn browser automation
├── tools.py                    # Shared utilities (Gemini, DNS, SMTP)
├── models.py                   # SQLite ORM
├── .env                        # Your credentials (gitignored)
└── .env.example                # Template for new installs
```

**Stack:** Python 3.11+, FastAPI, SQLite (WAL mode), Playwright, Gemini 3.1 Flash Lite, Gmail SMTP/IMAP

## Troubleshooting

**Dashboard not loading:** Make sure you're in the `data/` directory when starting uvicorn.

**No emails sending:** Check `GMAIL_PASSWORD` is an App Password, not your login password.

**LinkedIn login fails:** Delete `clawbuildr/data/ig_session_cookies_*.json` and re-login via the dashboard.

**Pipeline stops:** Check `data/logs/pipeline_runner.log` for errors. Most common: Gemini rate limit (wait 1 min) or DNS timeout (retry).
