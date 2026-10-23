# SkyGen — SQLite persistence and 10-minute handoff test

This package replaces the old repository-backup bot. It has only these commands:

- `/start` — records the Telegram user in SQLite
- `/ping` — response-time check and last-seen update
- `/id` — shows and records the user's Telegram ID

## How the 10-minute test works

1. Start the first run manually from **Actions → Run SkyGen Telegram Bot → Run workflow**.
2. The runner reads `database.sqlite` from a private database repository. If that repository does not exist, the bot creates it under the GitHub account associated with `GH_PAT`.
3. The bot handles commands and writes user records to its local SQLite file.
4. At 8 minutes, it pauses database writes, makes a consistent SQLite snapshot, commits it to the private database repository, and calls the GitHub API to dispatch the next workflow.
5. The new runner downloads and validates the committed SQLite file before starting Telegram polling.
6. When Telegram reports a polling conflict to the old runner, the old process exits. If no conflict arrives, the old process stops at 10 minutes.

The workflow intentionally has only `workflow_dispatch` as a trigger. That avoids an ordinary code push unexpectedly starting a second runner against a stale database. After changing code, push it and manually start the workflow again.

## Required GitHub Actions secrets

Open the **bot repository** → **Settings → Secrets and variables → Actions** and add:

| Secret | Value |
|---|---|
| `TELEGRAM_BOT_TOKEN` | Token from BotFather |
| `GH_PAT` | GitHub Personal Access Token |
| `DB_REPO_NAME` | Repository name for SQLite, e.g. `SkyGen-Database` |

`DB_REPO_NAME` is a repository name only, not a URL or `owner/repo`. The bot discovers the PAT owner's GitHub username. If that private repository does not exist, the bot creates it automatically. If it already exists, it must be private.

### Token permissions

For a **classic PAT**, the simplest setup for a private bot repository and private database repository is generally `repo` plus `workflow`. This is broad access; protect the token like a password and never print it in logs. Prefer a fine-grained PAT with only the required access if you can grant access to both repositories and workflow dispatch.

The PAT owner must have permission to access the repository containing this workflow. The database repository is created under the PAT owner's account.

## Important limitations

- This is a **10-minute test cycle**, not a production uptime guarantee.
- `409 Conflict` is a useful signal that another long-polling process has taken over, but it is not a formal handover acknowledgement.
- The database snapshot is uploaded **before** dispatching the next run. The next run downloads it before polling. Database writes are paused during handoff to reduce the chance of lost updates.
- Commands arriving during the brief handoff window may receive a “switching runners” reply and should be retried.
- GitHub Actions does not guarantee immediate runner startup, so a short interruption can occur.
- If a workflow dispatch fails, the current run logs the failure and stops at 10 minutes. Check Actions logs and manually run the workflow if the chain stops.
- SQLite contains Telegram user IDs, usernames, first names, timestamps, and start counts. Do not store sensitive tokens in it.
- Keep the database repository private. Do not add the SQLite file to the public bot repository.
- The current test uses an 8-minute handoff and a 10-minute hard stop. Change `HANDOFF_AT_SECONDS` and `RUN_SECONDS` in `bot.py` for a different test window.
