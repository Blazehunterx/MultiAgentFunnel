-- Migration: add LinkedIn enrichment fields to contacts and companies
-- Apply with: sqlite3 data/clawbuildr.db < clawbuildr/migrations/add_linkedin_enrichment_fields.sql

-- Profile enrichment
ALTER TABLE contacts ADD COLUMN tenure TEXT;
ALTER TABLE contacts ADD COLUMN skills_json TEXT;
ALTER TABLE contacts ADD COLUMN mutual_connections_count INTEGER DEFAULT 0;
ALTER TABLE contacts ADD COLUMN mutual_connections_text TEXT;
ALTER TABLE contacts ADD COLUMN linkedin_last_enriched_at TEXT;

-- Company enrichment
-- NOTE: writers use estimated_size (pre-existing column), not this legacy `size` alias.
ALTER TABLE companies ADD COLUMN size TEXT;
ALTER TABLE companies ADD COLUMN headquarters TEXT;
ALTER TABLE companies ADD COLUMN founded TEXT;
ALTER TABLE companies ADD COLUMN specialties TEXT;
ALTER TABLE companies ADD COLUMN website TEXT;
ALTER TABLE companies ADD COLUMN followers TEXT;
ALTER TABLE companies ADD COLUMN employee_count_text TEXT;
ALTER TABLE companies ADD COLUMN hiring_count INTEGER DEFAULT 0;
ALTER TABLE companies ADD COLUMN hiring_jobs_json TEXT;
ALTER TABLE companies ADD COLUMN company_posts_json TEXT;
ALTER TABLE companies ADD COLUMN linkedin_last_enriched_at TEXT;
