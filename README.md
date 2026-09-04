# Multi-Agent Funnel (Odysseus)

An autonomous, multi-agent outbound intelligence and B2B lead generation system designed for the Dutch B2B market. 

This system uses a fleet of AI agents to autonomously discover leads, qualify them based on strict criteria, perform deep web research, and execute highly personalized, multi-channel outreach (Email & LinkedIn).

## Key Features

- **Agent Zero (Ingestion Layer)**: Reads lead exports (Apollo/Clay), normalizes domains, deduplicates against the SQLite CRM, and hands qualified leads directly to the pipeline.
- **Research Agent**: Scrapes target websites to identify decision-makers, core services, and pain points.
- **Deliverability Agent**: Checks MX records, email validity, and bounces before sending.
- **LinkedIn Engine**: Fully automated LinkedIn outreach using browser automation. Logs in, connects, and sends customized connection notes and follow-ups.
- **Inbox Management Agent**: Reads replies and intelligently auto-replies or schedules meetings.
- **Interactive Dashboard**: A real-time Mission Control dashboard to view agent activities, review leads, monitor LinkedIn stats, and track overall B2B pipeline health.

## Setup & Installation

1. Install dependencies:
   ``bash
   pip install -r requirements.txt
   ``
2. Copy .env.example to .env and fill in your API keys (OpenAI, Groq, local LLM configurations).
3. Ensure Firefox is installed (for the LinkedIn automation geckodriver).

## Running the Dashboard

Start the Mission Control dashboard by running:
``bash
python data/clawbuildr_dashboard.py
``
Then open your browser to http://127.0.0.1:8000.

## Architecture

- **Backend**: FastAPI (Python)
- **Database**: SQLite (clawbuildr.db with WAL mode for concurrency)
- **LLM Engine**: LangChain + Local/Remote LLMs
- **Browser Automation**: Selenium (for LinkedIn)
- **Frontend**: Raw HTML/Tailwind served via FastAPI
