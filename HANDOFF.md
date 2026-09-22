# ClawBuildr Platform — Context Handoff

## Project Overview
Multi-tenant outbound GTM engine (Clay-competitive): DM sender, AI auto-responder, lead harvester, automated follow-up sequences, intent-based pipeline management, CRM.

**Status**: 68 contacts in pipeline, 24/7 runner built, dashboard running on port 8000.

## Key Architecture
- **Single entry point**: `python clawbuildr/pipeline_runner.py` (24/7 background loop)
- **Dashboard**: `python -m uvicorn clawbuildr_dashboard:app --host 0.0.0.0 --port 8000` (from `data/`)
- **Cloudflare tunnel**: `https://antibody-importantly-sponsor-kodak.trycloudflare.com` (temporary)
- **API prefix**: All new routes mounted at `/api/v2` in dashboard
- **DB**: `MultiAgentFunnel/data/clawbuildr.db` — SQLite, 34 tables, 68 contacts

## Environment Setup
```bash
cd MultiAgentFunnel
python -m venv .venv
.venv\Scripts\activate  # Windows
pip install -r requirements.txt
playwright install chromium
```

## Credentials
- **Gmail SMTP**: `marvin@clawbuildr.com` / `oglh pngm riit jijx`
- **Gmail IMAP**: `imap.gmail.com`
- **Hunter.io**: `b0bd926d33dc4801b189c4f09184c443eb750e5b,aadc24fdd3090a1e94465346d19dad48bf4de0f2`
- **Gemini**: `AIzaSyDf6_ZVbiV_GuHK8Udj5Kh2yZ8t7dIjvfs,AIzaSyC4gy1x59ARkJdJe0Gkusu-ZNp57ERPGpk` + 4 more
- **Groq**: `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`
- **Vercel DNS**: SPF + DMARC already configured

## DNS Status
- ✅ DKIM: configured
- ✅ MX: configured
- ✅ SPF: `v=spf1 include:_spf.google.com ~all` (just added)
- ✅ DMARC: `v=DMARC1; p=quarantine; rua=mailto:dmarc@clawbuildr.com` (just added)

## ICP (Ideal Customer Profile)
- Service businesses, B2B, manual processes, founder-led
- NL/BE/DE focus
- Industries: Accounting, Legal, IT, Marketing, Construction, Real Estate

## 24/7 Pipeline Runner (`clawbuildr/pipeline_runner.py`)
Runs continuously with 30-45 min cycles:
1. **Lead gen** — 3 Hunter.io domain searches per cycle (from `KNOWN_BUSINESS_DOMAINS` list)
2. **Enrollment** — Moves INGESTED → ACTIVE_OUTREACH
3. **AI email generation** — Gemini 2.5 Flash drafts
4. **Email sending** — Gmail SMTP with 15-30s delays
5. **Follow-ups** — 48h/96h/168h sequence
6. **Reply detection** — Gmail IMAP polling

**Rate limits**: ~48 domain searches/day, ~8-12 emails/day
**Goal**: 300+ emails in 7 days

Run in background: `python clawbuildr/pipeline_runner.py`

## Critical Files

### Dashboard
- `data/clawbuildr_dashboard.py` — Main FastAPI app (~10K lines), all endpoints, background loops. **FROZEN per AGENTS.md** — only import + mount allowed.

### Core Modules (all in `clawbuildr/`)
| File | Purpose |
|------|---------|
| `clawbuildr_api.py` | 44 API endpoints mounted at `/api/v2` |
| `clawbuildr_ai_email.py` | Gemini email generation + follow-ups |
| `clawbuildr_reply_detector.py` | Gmail IMAP reply detection + sentiment |
| `clawbuildr_scheduler.py` | Follow-up sequence queue (48h/96h/168h) |
| `clawbuildr_auth.py` | JWT auth + API keys + users table |
| `clawbuildr_workspace.py` | Multi-tenant workspaces + members |
| `clawbuildr_onboarding.py` | Client onboarding checklist |
| `clawbuildr_integration.py` | Pipeline orchestration |
| `clawbuildr_learning.py` | Self-learning: performance, A/B tests |
| `clawbuildr_sequences.py` | Campaign sequences + enrollments |
| `clawbuildr_deliverability.py` | SPF/DKIM/DMARC/MX checks |
| `clawbuildr_sentiment.py` | Reply sentiment + meeting detection |
| `clawbuildr_lead_scoring.py` | Lead scoring + tiering |
| `clawbuildr_hunter.py` | Hunter.io domain search + verify |
| `clawbuildr_watchdog.py` | Daily/hourly limits + bounce/reply health |
| `clawbuildr_client_dashboard.py` | Client-facing overview views |
| `clawbuildr_linkedin_integration.py` | LinkedIn campaign management |
| `clawbuildr_lead_generator.py` | Multi-source lead discovery (Hunter.io, website scraping) |
| `pipeline_runner.py` | 24/7 background loop (NEW) |

### Supporting Files
| File | Purpose |
|------|---------|
| `search_rotator.py` | Multi-engine search with rate limiting |
| `email_finder.py` | Email discovery with Hunter.io + patterns |
| `enrich_lead.py` | LinkedIn lead enrichment |
| `pipeline.py` | Email generation via LLM chain |
| `tools.py` | LLM tools, Gmail send, web scraping |
| `.env` | All API keys |

### Data
| File | Purpose |
|------|---------|
| `data/clawbuildr.db` | SQLite database |
| `data/start_background_runner.py` | Continuous reply/follow-up processing |
| `requirements.txt` | Python dependencies |

## Database State (Current)
- **68 contacts**: 43 INGESTED, 12 ACTIVE_OUTREACH, 5 NURTURE, 6 BOUNCED, 1 OPPORTUNITY_MAPPED, 1 PENDING_APPROVAL
- **75 companies**
- **0 emails sent** (ready to start)
- **0 LinkedIn connections** (warming up)

## API Endpoints (key ones)
```
POST /api/v2/leads/generate      — Generate leads (JSON body)
POST /api/v2/leads/generate/sync — Generate leads (query string)
GET  /api/v2/pipeline/overview   — Pipeline stats
GET  /api/v2/pipeline/health     — Health check
GET  /api/v2/followup/stats      — Follow-up queue stats
GET  /api/v2/linkedin/stats      — LinkedIn campaign stats
GET  /api/v2/workspaces          — List workspaces
```

## Known Issues
1. **Web search engines** — DuckDuckGo blocked (DNS), Bing geo-locked (Indonesia), Google returns consent pages. Hunter.io API works as primary source.
2. **Google Calendar API** — Not enabled (403), meeting booking doesn't work
3. **Selenium not installed** — LinkedIn employee search in funnel fails
4. **Pipeline overview endpoint** — Slow due to heavy queries (works but times out under load)

## User Preferences
- Free solutions only (no budget)
- No coordinates for button detection (they change) — must read page like a human
- Dashboard is FROZEN — minimal changes only
- `.txt` files, NOT `.md` for content

## Next Steps
1. **Start pipeline runner**: `python clawbuildr/pipeline_runner.py` (24/7)
2. **Monitor**: Check `data/logs/pipeline_runner.log` for progress
3. **LinkedIn**: Verify connections after warming period
4. **New features**: Whatever the user wants next

## File Structure
```
MultiAgentFunnel/
├── clawbuildr/           # All Python modules
│   ├── .env              # API keys
│   ├── clawbuildr_*.py   # Feature modules
│   ├── pipeline_runner.py # 24/7 loop (NEW)
│   ├── search_rotator.py
│   ├── email_finder.py
│   └── ...
├── data/
│   ├── clawbuildr.db     # SQLite database
│   ├── clawbuildr_dashboard.py # Main app (FROZEN)
│   ├── logs/             # Pipeline runner logs
│   └── start_background_runner.py
├── requirements.txt
└── .venv/                # Virtual environment
```
