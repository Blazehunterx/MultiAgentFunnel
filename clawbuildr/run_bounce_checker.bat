@echo off
REM Auto bounce checker for ClawBuildr
REM Runs every 30 minutes via Windows Task Scheduler

cd /d "C:\Users\marvi\.gemini\antigravity\playground\glacial-apogee\antigravity-cloud\MultiAgentFunnel"
python "clawbuildr\imap_bounce_checker.py" >> "data\bounce_checker.log" 2>&1
