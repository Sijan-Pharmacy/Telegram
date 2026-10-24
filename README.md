# SkyGen Telegram Bot — Modular Edition

This edition separates Telegram commands from the process lifecycle while preserving the existing GitHub Actions + private GitHub SQLite handoff design.

## Files

- `bot.py` — Telegram handlers only: `/start`, `/ping`, `/id`, and the handoff notice fallback.
- `runtime.py` — SQLite persistence, GitHub API, backup snapshots, workflow dispatch/monitoring, and parent/child handoff.
- `lifecycle.py` — small Python entry point for the runtime.
- `start.sh` — starts the bot in the foreground (suitable for GitHub Actions and VPS use).
- `stop.sh` — requests a graceful SIGTERM on a VPS; runtime attempts to upload a final SQLite snapshot. In GitHub Actions, stop/cancel the workflow from the Actions UI.
- `.github/workflows/run.yml` — GitHub Actions workflow.
- `requirements.txt` — Python dependencies.

## Production timing

- `RUN_SECONDS = 340 * 60`: 5 hours 40 minutes.
- `HANDOFF_AT_SECONDS = 335 * 60`: dispatch the next workflow after 5 hours 35 minutes.
- `CUTOVER_BUFFER_SECONDS = 60`: start the final database snapshot 1 minute before the 5h40m deadline, leaving roughly 4 minutes for the next run to initialize before the final sync.
- `POLL_INTERVAL_SECONDS = 3`: workflow status check interval.

The Actions job timeout remains 350 minutes, giving a 10-minute outer margin beyond the bot's 340-minute lifecycle. Do not reduce it below `RUN_SECONDS` plus a reasonable startup/cleanup margin.

## Required GitHub Actions secrets

- `TELEGRAM_BOT_TOKEN` — Telegram bot token.
- `GH_PAT` — a GitHub token with access to the bot repository's Actions workflows and the private database repository's contents. It should have only the permissions needed for these tasks.
- `DB_REPO_NAME` — private database repository name (optional; defaults to `SkyGen-Database`).

The database repository must remain private. The bot may create it if the token has permission. The workflow must be manually started once; subsequent runs are dispatched by the current run.

## VPS use

```bash
python -m pip install -r requirements.txt
chmod +x start.sh stop.sh
./start.sh
```

`start.sh` runs in the foreground. For background service management, use a proper supervisor such as systemd rather than `nohup` for a production deployment. `stop.sh` works with a process started by `start.sh` only if the PID file remains available.

## Important operational note

The same Telegram bot token should have only one active long poller. The handoff status protocol tries to ensure the child loads the final SQLite snapshot and confirms readiness before the parent exits. A short race or service interruption is still possible if GitHub queues the next workflow, API calls fail, or the child does not become ready before the deadline. GitHub Actions is not a substitute for a continuously running production VPS.
