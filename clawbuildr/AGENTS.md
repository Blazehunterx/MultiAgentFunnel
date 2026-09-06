# ClawBuildr — Antigravity Audit: Status & Gaps

> Audit date: 2026-06-10 | Auditor: antigravity

---

## Project Overview

ClawBuildr is a multi-agent outbound intelligence pipeline for Dutch/German/Belgian B2B lead generation. It is composed of two separate codebases:

| Component | Location | Size | Status |
|-----------|----------|------|--------|
| **Pipeline** | `C:\Users\marvi\clawbuildr\` | ~60 KB Python | Active, modifiable |
| **Dashboard** | `C:\Users\marvi\odysseus\data\clawbuildr_dashboard.py` | 172 KB / 3058 lines | 🔒 FROZEN — never modify |
| **Odysseus support modules** | `C:\Users\marvi\odysseus\data\` | 6 Python files, ~80 KB | Active |
| **Cloud control plane** | `C:\Users\marvi\.gemini\antigravity\playground\glacial-apogee\antigravity-cloud\` | Next.js 16 / Supabase | Separate product (FounderFlow) |

---

## What Has Been Built

### Pipeline (`C:\Users\marvi\clawbuildr\`)

| File | Lines | Purpose | Status |
|------|-------|---------|--------|
| `pipeline.py` | 846 | 7-stage orchestration: Research → Trust → Opportunity → BANT → Email → CRM + Learning | ✅ Working |
| `tools.py` | 494 | Web scraping, email discovery, MX verification, KVK lookup, Gmail SMTP, CRM CRUD | ✅ Working (recently improved) |
| `main.py` | 53 | FastAPI entry point, 6 REST endpoints, serves `templates/index.html` | ✅ Working |
| `models.py` | 62 | Pydantic schemas for all pipeline data types | ✅ Working |
| `templates/index.html` | 295 | Dark-theme single-page dashboard with pipeline viz, CRM table, learnings | ✅ Working |
| `.env` | 4 lines | Gmail app password for marvin@clawbuildr.com | ✅ Configured |
| `data/crm.db` | 20 KB | SQLite CRM with prospects + crm_records tables | ✅ Has data |
| `data/learning.db` | 12 KB | SQLite learning/memory store | ✅ Active |

### Dashboard (`C:\Users\marvi\odysseus\data\clawbuildr_dashboard.py`) — FROZEN

| Feature | Status | Notes |
|---------|--------|-------|
| Lead sourcing agent (DDG lite, 55+ queries) | ✅ Working | Runs every 15 min, 24/7 |
| 6-agent pipeline with SSE real-time updates | ✅ Working | Research → Deliverability → Opportunity → Qualification → Outreach → Inbox |
| Human approval gate (PENDING_APPROVAL) | ✅ Working | Stages stops for human review |
| CRM / lead table with stage badges | ✅ Working | 109 contacts, 109 companies |
| Agent status cards | ✅ Working | Shows RUNNING/IDLE/FAILED |
| Approval modal with editable draft | ✅ Working | Review & Send, edit subject/body |
| Delete lead from pipeline | ✅ Working | Trash icon + confirmation |
| Drawer with tabs (Research, Deliverability, Opportunity, Qualification, Outreach) | ✅ Working | Full lead detail view |
| Pipeline handoff visualizer (SVG) | ✅ Working | Animated agent handoffs |

### Supporting Modules (`C:\Users\marvi\odysseus\data\`)

| File | Lines | Purpose | Status |
|------|-------|---------|--------|
| `clawbuildr_multi_agent.py` | 739 | Reference multi-agent framework with mock LLM | 🟡 Reference only (not in active pipeline) |
| `clawbuildr_db.py` | 320 | Unified PostgreSQL + SQLite adapter, 8 tables, GDPR opt-out | ✅ Working |
| `clawbuildr_scheduler.py` | 249 | Humanized email scheduling (NL business hours, randomized delays) | ✅ Working |
| `clawbuildr_gmail.py` | 168 | Gmail OAuth API connector | ✅ Working |
| `clawbuildr_calendar.py` | 149 | Google Calendar + Meet link creation | ✅ Working |
| `clawbuildr_compliance.py` | 111 | GDPR LIA generation, opt-out checks | ✅ Working |
| `clawbuildr_discovery.py` | 113 | Email pattern generation + MX verification | 🟡 Duplicates tools.py verify_email — consolidate |
| `clawbuildr_dashboard_schema.sql` | 52 | Dashboard DB schema | ✅ Applied |
| `clawbuildr_schema.sql` | 112 | Full PostgreSQL schema | ✅ Available |

### Operational Database (`clawbuildr.db`)

| Table | Rows | Purpose |
|-------|------|---------|
| `contacts` | 109 | Leads at various pipeline stages |
| `companies` | 109 | Sourced companies |
| `activity_log` | 769 | Full audit trail |
| `agent_actions` | 1094 | Inter-agent messages |
| `emails` | 3 | Sent/received email logs |
| `agent_status` | 6 | Live agent state |

---

## Identified Gaps & Upgrade Opportunities

### 🔴 Critical Gaps

| Gap | Details | Impact | Suggested Fix |
|-----|---------|--------|---------------|
| **No dependency management** | No requirements.txt, pyproject.toml, or setup.py. Relies on shared venv at `C:\Users\marvi\odysseus\venv\` | Portable setup impossible; new machines must find the venv | Add `requirements.txt` with pinned versions for `fastapi`, `uvicorn`, `httpx`, `pydantic`, `dnspython` |
| **No version control** | Neither `clawbuildr\` nor `odysseus\data\` is a git repo | No history, no rollback, no collaboration | Initialize git repos or at least add to existing odysseus repo |
| **No tests** | Zero test files across both codebases | Changes can't be validated; regressions go undetected | Add pytest suite for: email extraction, pipeline stages, MX verification, API endpoints |
| **Email quality still unreliable** | Some leads get info@ generic emails; %20 and Cloudflare-obfuscated emails still slip through recently | Bounces hurt sender reputation | Add email scoring threshold: require confidence >= 70 before inserting lead, else flag for human review |

### 🟡 Medium Priority

| Gap | Details | Suggested Fix |
|-----|---------|---------------|
| **Pipeline is purely deterministic** | No LLM calls — all scoring is regex-based. No AI personalization beyond keyword matching | Integrate Gemini API (key exists in pipeline.py?) for: smarter pain detection, better email personalization, dynamic follow-up generation |
| **Duplicate lead handling** | Companies with same domain but different queries create duplicate contacts | Add domain uniqueness enforcement across sourcing cycles |
| **No email send analytics** | No open/click tracking, no bounce-back parsing | Add webhook endpoint for receiving bounce/read receipts; integrate with dashboard |
| **Sourcing queries could be smarter** | 55 static queries shuffled randomly; no learning from past successes | Track which queries produce high-quality leads; bias toward productive queries |
| **No cold email warmup** | Sending from a single domain with no gradual ramp | Add volume limits, gradual ramp-up, rotation between sending addresses |
| **`clawbuildr_discovery.py` duplicates `verify_email` in tools.py** | Two separate email verification implementations | Consolidate into a single module |
| **Dashboard has no email history view** | Can't see what was previously sent to a lead | Add sent email log to lead drawer |
| **No export/mass operations** | Can't export leads to CSV, can't approve in batch | Add CSV export and batch approve endpoint |

### 🟢 Low Priority / Nice-to-Have

| Gap | Details |
|-----|---------|
| **No logging to file** | Dashboard logs go to uvicorn stdout only | Add file logging for production monitoring |
| **No health check endpoint** | Dashboard has no /health route for monitoring | Add simple health check |
| **No rate limiting** | API endpoints have no throttling | Add rate limiting middleware |
| **No HTTPS** | Dashboard serves plain HTTP on 0.0.0.0:8000 | Add nginx reverse proxy with Let's Encrypt for production |
| **No backup strategy** | SQLite databases have no scheduled backup | Add cron task to copy .db files daily |
| **Dashboard not mobile-optimized** | Tailwind layout is desktop-first | Add responsive breakpoints for phone access |
| **Unused files** | `clawbuildr_multi_agent.py` (739 lines) is reference-only but maintained | Archive or remove to reduce confusion |

---

## Architecture Relationships

```
┌─────────────────────────────────────────────────────┐
│                   Browser (port 8000)                │
├─────────────────────────────────────────────────────┤
│                                                     │
│  clawbuildr_dashboard.py (FROZEN)                   │
│  ┌─────────────────┐   ┌─────────────────────────┐  │
│  │ Lead Sourcing    │   │ Agent Pipeline           │  │
│  │ Agent (DDG lite) │──▶│ Research → Trust → Opp  │  │
│  │ 55 queries, 15m  │   │ → BANT → Email → CRM    │  │
│  └─────────────────┘   └──────────┬──────────────┘  │
│                                   │                  │
│  ┌────────────────────────────────▼──────────────┐  │
│  │  C:\Users\marvi\clawbuildr\                   │  │
│  │  ┌──────────┐  ┌──────────┐  ┌─────────────┐  │  │
│  │  │pipeline  │  │  tools   │  │   models     │  │  │
│  │  │.py       │  │  .py     │  │   .py        │  │  │
│  │  │846 lines │  │494 lines │  │  62 lines    │  │  │
│  │  └──────────┘  └──────────┘  └─────────────┘  │  │
│  │  ┌──────────────────┐  ┌────────────────────┐  │  │
│  │  │main.py (FastAPI) │  │templates/index.html│  │  │
│  │  │53 lines, 6 routes│  │295 lines, dark UI  │  │  │
│  │  └──────────────────┘  └────────────────────┘  │  │
│  └────────────────────────────────────────────────┘  │
│                                                     │
│  Supporting modules (odysseus/data/)                 │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────┐ │
│  │  gmail   │ │ calendar │ │scheduler │ │comply  │ │
│  │  .py     │ │ .py      │ │ .py      │ │ .py    │ │
│  └──────────┘ └──────────┘ └──────────┘ └────────┘ │
│  ┌──────────┐ ┌────────────┐ ┌──────────────┐      │
│  │ discovery│ │multi_agent │ │     db       │      │
│  │ .py      │ │ .py (ref)  │ │    .py       │      │
│  └──────────┘ └────────────┘ └──────────────┘      │
└─────────────────────────────────────────────────────┘
```

### Key Data Flow

1. **Sourcing**: DuckDuckGo lite search → web scrape → email extraction → MX verification → insert as INGESTED
2. **Pipeline**: INGESTED → RESEARCHED (scrape) → DELIVERABILITY_VERIFIED (trust gate) → OPPORTUNITY_MAPPED (pain matching) → PRE_QUALIFIED (BANT scoring) → OUTREACH_DRAFTED (email gen) → PENDING_APPROVAL (human gate)
3. **Approval**: Human reviews → edits email → clicks Send → Gmail SMTP → ACTIVE_OUTREACH
4. **Inbox**: Reply detected → classified (interested/objection/meeting) → pipeline update or Calendly link sent

---

## Immediate Recommendations (Priority Order)

1. **Add `requirements.txt`** to both `clawbuildr\` and `odysseus\data\` for reproducible installs
2. **Initialize git in `clawbuildr\`** and make an initial commit of working state
3. **Add email confidence threshold** to sourcing — skip leads where `verify_email()` returns confidence < 70
4. **Set up daily SQLite backups** via Windows Task Scheduler or startup script
5. **Add file logging** to dashboard so crashes are debuggable
6. **Consolidate `clawbuildr_discovery.py`** into `tools.py` to avoid duplicate email verification logic
7. **Add CSV export** for leads at PENDING_APPROVAL for manual review
