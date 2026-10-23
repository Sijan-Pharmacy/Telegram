import asyncio
import base64
import io
import json
import logging
import os
import sqlite3
import tempfile
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import aiohttp
from aiogram import Bot, Dispatcher
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.exceptions import TelegramConflictError
from aiogram.filters import Command, CommandStart
from aiogram.types import Message

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
GITHUB_PAT = os.getenv("GH_PAT", "").strip()
DB_REPO_NAME = os.getenv("DB_REPO_NAME", "SkyGen-Database").strip()
BOT_REPOSITORY = os.getenv("GITHUB_REPOSITORY", "").strip()
WORKFLOW_FILE = os.getenv("WORKFLOW_FILE", "run.yml").strip()
WORKFLOW_REF = os.getenv("GITHUB_REF_NAME", "main").strip()
RUN_ID = os.getenv("GITHUB_RUN_ID", "").strip()
PARENT_RUN_ID = os.getenv("PARENT_RUN_ID", "").strip()

GITHUB_API = "https://api.github.com"
DB_PATH = Path("data/database.sqlite")
RUN_SECONDS = 10 * 60
HANDOFF_AT_SECONDS = 8 * 60
POLL_INTERVAL_SECONDS = 3
START_TIME = time.monotonic()

dp = Dispatcher()
DB_LOCK = asyncio.Lock()
HANDOFF_STARTED = False
DB_OWNER = ""
DB_BRANCH = "main"
TARGET_RUN_ID = ""
PREPARED_FOR_HANDOFF = False


def log_uptime():
    seconds = int(time.monotonic() - START_TIME)
    minutes, seconds = divmod(seconds, 60)
    return f"{minutes}m {seconds}s"


def gh_headers(accept="application/vnd.github+json"):
    return {
        "Authorization": f"Bearer {GITHUB_PAT}",
        "Accept": accept,
        "X-GitHub-Api-Version": "2022-11-28",
    }


async def github_json(session, method, url, **kwargs):
    async with session.request(method, url, headers=gh_headers(), **kwargs) as response:
        body = await response.text()
        if response.status >= 400:
            # Never include request headers or secrets in logs.
            raise RuntimeError(
                f"GitHub API {method} failed with HTTP {response.status}: {body[:250]}"
            )
        if not body:
            return None
        return json.loads(body)


async def ensure_private_database_repo(http):
    if not GITHUB_PAT:
        raise RuntimeError("Missing GH_PAT secret.")

    user = await github_json(http, "GET", f"{GITHUB_API}/user")
    owner = user["login"]
    repo_url = (
        f"{GITHUB_API}/repos/{quote(owner, safe='')}/"
        f"{quote(DB_REPO_NAME, safe='')}"
    )

    async with http.get(repo_url, headers=gh_headers()) as response:
        if response.status == 200:
            repo = await response.json()
        elif response.status == 404:
            repo = None
        else:
            raise RuntimeError(f"Cannot check database repository (HTTP {response.status}).")

    if repo is None:
        repo = await github_json(
            http,
            "POST",
            f"{GITHUB_API}/user/repos",
            json={
                "name": DB_REPO_NAME,
                "private": True,
                "auto_init": True,
                "description": "Private SQLite persistence for SkyGen",
            },
        )
        logging.info("Created private database repository: %s", repo["full_name"])

    if repo.get("private") is not True:
        raise RuntimeError(
            f"Database repository {repo['full_name']} is public. Make it private first."
        )
    return owner, repo["name"], repo.get("default_branch") or "main"


def initialize_database(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT NOT NULL DEFAULT '',
                first_started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                start_count INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        connection.commit()


def check_database(path):
    with sqlite3.connect(path) as connection:
        result = connection.execute("PRAGMA integrity_check").fetchone()
        if not result or result[0] != "ok":
            raise RuntimeError("SQLite database failed integrity check.")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT NOT NULL DEFAULT '',
                first_started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                start_count INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        connection.commit()


async def download_database(http, owner, repo, branch):
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    url = f"{GITHUB_API}/repos/{quote(owner, safe='')}/{quote(repo, safe='')}/contents/database.sqlite"
    async with http.get(url, headers=gh_headers(), params={"ref": branch}) as response:
        if response.status == 404:
            logging.info("No saved SQLite database found; initializing a new one.")
            initialize_database(DB_PATH)
            return
        if response.status != 200:
            raise RuntimeError(f"Cannot download database (HTTP {response.status}).")
        metadata = await response.json()

    content = metadata.get("content", "").strip()
    if metadata.get("encoding") == "base64" and content:
        raw = base64.b64decode(content)
    else:
        async with http.get(
            url,
            headers=gh_headers("application/vnd.github.raw"),
            params={"ref": branch},
        ) as response:
            if response.status != 200:
                raise RuntimeError(f"Cannot download raw database (HTTP {response.status}).")
            raw = await response.read()

    DB_PATH.write_bytes(raw)
    check_database(DB_PATH)
    logging.info("SKYGEN_DB_LOADED bytes=%s integrity=ok", len(raw))


async def read_file_metadata(http, owner, repo, branch, filename):
    url = f"{GITHUB_API}/repos/{quote(owner, safe='')}/{quote(repo, safe='')}/contents/{quote(filename, safe='/')}"
    async with http.get(url, headers=gh_headers(), params={"ref": branch}) as response:
        if response.status == 404:
            return url, None
        if response.status != 200:
            raise RuntimeError(f"Cannot inspect {filename} (HTTP {response.status}).")
        return url, await response.json()


async def put_repo_file(http, owner, repo, branch, filename, raw, message):
    url, current = await read_file_metadata(http, owner, repo, branch, filename)
    payload = {
        "message": message,
        "content": base64.b64encode(raw).decode("ascii"),
        "branch": branch,
    }
    if current and current.get("sha"):
        payload["sha"] = current["sha"]
    async with http.put(url, headers=gh_headers(), json=payload) as response:
        if response.status not in (200, 201):
            body = await response.text()
            raise RuntimeError(f"Cannot update {filename} (HTTP {response.status}): {body[:200]}")


async def write_handoff_status(http, state, target_run_id="", parent_run_id=""):
    status = {
        "state": state,
        "target_run_id": str(target_run_id),
        "parent_run_id": str(parent_run_id),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    await put_repo_file(
        http, DB_OWNER, DB_REPO_NAME, DB_BRANCH, "handoff-status.json",
        json.dumps(status, separators=(",", ":")).encode(),
        f"SkyGen handoff status: {state} (run {RUN_ID})",
    )
    logging.info("SKYGEN_HANDOFF_STATE state=%s target_run_id=%s parent_run_id=%s", state, target_run_id, parent_run_id)


async def read_handoff_status(http):
    url, metadata = await read_file_metadata(
        http, DB_OWNER, DB_REPO_NAME, DB_BRANCH, "handoff-status.json"
    )
    if not metadata:
        return {}
    content = metadata.get("content", "").strip()
    if metadata.get("encoding") == "base64" and content:
        try:
            return json.loads(base64.b64decode(content))
        except (ValueError, json.JSONDecodeError):
            return {}
    # Small JSON files should normally be returned as base64.
    async with http.get(url, headers=gh_headers("application/vnd.github.raw"), params={"ref": DB_BRANCH}) as response:
        if response.status != 200:
            return {}
        try:
            return json.loads(await response.text())
        except json.JSONDecodeError:
            return {}


async def create_snapshot():
    fd, name = tempfile.mkstemp(suffix=".sqlite")
    os.close(fd)
    snapshot = Path(name)

    def backup_sync():
        with sqlite3.connect(DB_PATH, timeout=30) as source:
            with sqlite3.connect(snapshot) as target:
                source.backup(target)
                target.commit()
        check_database(snapshot)

    try:
        async with DB_LOCK:
            await asyncio.to_thread(backup_sync)
        return snapshot
    except Exception:
        snapshot.unlink(missing_ok=True)
        raise


async def upload_database(http, snapshot):
    raw = snapshot.read_bytes()
    await put_repo_file(
        http, DB_OWNER, DB_REPO_NAME, DB_BRANCH, "database.sqlite", raw,
        f"Update SQLite database before handoff (run {RUN_ID})",
    )
    logging.info("SKYGEN_DB_SNAPSHOT_COMMITTED bytes=%s integrity=ok", len(raw))


async def dispatch_next_run(http):
    if not BOT_REPOSITORY or "/" not in BOT_REPOSITORY:
        raise RuntimeError("GITHUB_REPOSITORY environment variable is missing or invalid.")
    url = f"{GITHUB_API}/repos/{BOT_REPOSITORY}/actions/workflows/{quote(WORKFLOW_FILE, safe='')}/dispatches"
    payload = {
        "ref": WORKFLOW_REF or "main",
        "inputs": {"parent_run_id": str(RUN_ID)},
    }

    # Current GitHub docs describe HTTP 200 with workflow_run_id; keep a fallback
    # for older/alternate API behavior that returns 204 No Content.
    dispatched_at = datetime.now(timezone.utc)
    async with http.post(url, headers=gh_headers(), json=payload) as response:
        body = await response.text()
        if response.status == 200:
            data = json.loads(body) if body else {}
            run_id = data.get("workflow_run_id")
            if run_id:
                logging.info("SKYGEN_DISPATCH_ACCEPTED target_run_id=%s", run_id)
                return str(run_id)
        elif response.status == 204:
            logging.info("Dispatch accepted with HTTP 204; locating new workflow run.")
        else:
            raise RuntimeError(f"Workflow dispatch failed (HTTP {response.status}): {body[:250]}")

    owner, repo = BOT_REPOSITORY.split("/", 1)
    runs_url = f"{GITHUB_API}/repos/{owner}/{repo}/actions/workflows/{quote(WORKFLOW_FILE, safe='')}/runs"
    for _ in range(20):
        data = await github_json(http, "GET", runs_url, params={"event": "workflow_dispatch", "branch": WORKFLOW_REF or "main", "per_page": 20})
        candidates = []
        for run in data.get("workflow_runs", []):
            if run.get("id") == int(RUN_ID or 0):
                continue
            try:
                created = datetime.fromisoformat(run["created_at"].replace("Z", "+00:00"))
            except (KeyError, ValueError):
                continue
            if created >= dispatched_at and run.get("event") == "workflow_dispatch":
                candidates.append(run)
        if candidates:
            candidate = max(candidates, key=lambda item: item.get("created_at", ""))
            run_id = str(candidate["id"])
            logging.info("SKYGEN_DISPATCH_ACCEPTED target_run_id=%s (located via runs API)", run_id)
            return run_id
        await asyncio.sleep(1)
    raise RuntimeError("Dispatch was accepted, but the new workflow Run ID could not be located.")


async def get_workflow_run(http, run_id):
    if not BOT_REPOSITORY or "/" not in BOT_REPOSITORY:
        return {}
    return await github_json(http, "GET", f"{GITHUB_API}/repos/{BOT_REPOSITORY}/actions/runs/{run_id}")


async def verify_run_job(http, run_id):
    """Verify the new run is actually executing the bot step, not merely queued."""
    data = await github_json(
        http, "GET", f"{GITHUB_API}/repos/{BOT_REPOSITORY}/actions/runs/{run_id}/jobs",
        params={"per_page": 100},
    )
    for job in data.get("jobs", []):
        if job.get("name") != "run-bot":
            continue
        for step in job.get("steps", []):
            if step.get("name") == "Run SkyGen" and step.get("status") == "in_progress":
                return True
    return False


async def inspect_run_logs(http, run_id):
    """Best-effort log check; GitHub may not expose a log archive while a job is running."""
    url = f"{GITHUB_API}/repos/{BOT_REPOSITORY}/actions/runs/{run_id}/logs"
    try:
        async with http.get(url, headers=gh_headers(), allow_redirects=True) as response:
            if response.status != 200:
                return False
            blob = await response.read()
        with zipfile.ZipFile(io.BytesIO(blob)) as archive:
            text_blob = "\n".join(
                archive.read(name).decode("utf-8", errors="ignore")
                for name in archive.namelist() if not name.endswith("/")
            )
        good = "SKYGEN_DB_LOADED" in text_blob and "SKYGEN_HANDOFF_STATE state=prepared" in text_blob
        if good:
            logging.info("SKYGEN_CHILD_LOGS_VERIFIED target_run_id=%s", run_id)
        return good
    except (aiohttp.ClientError, asyncio.TimeoutError, zipfile.BadZipFile, OSError, RuntimeError):
        return False


async def child_wait_for_parent_to_stop(http):
    """Wait for the parent's final DB snapshot, reload it, then wait for poller handoff."""
    if not PARENT_RUN_ID:
        return

    logging.info("SKYGEN_HANDOFF_WAIT parent_run_id=%s", PARENT_RUN_ID)
    deadline = time.monotonic() + 12 * 60
    final_db_loaded = False

    while time.monotonic() < deadline:
        status = await read_handoff_status(http)
        state = status.get("state")
        target = str(status.get("target_run_id", ""))
        parent_id = str(status.get("parent_run_id", ""))

        if (
            state == "final_snapshot_ready"
            and target == RUN_ID
            and parent_id == PARENT_RUN_ID
            and not final_db_loaded
        ):
            # The child initially loaded an earlier snapshot at startup. Replace it with
            # the parent's final, write-paused snapshot before becoming eligible to poll.
            for suffix in ("", "-wal", "-shm"):
                Path(f"{DB_PATH}{suffix}").unlink(missing_ok=True)
            await download_database(http, DB_OWNER, DB_REPO_NAME, DB_BRANCH)
            await write_handoff_status(
                http, "child_ready", RUN_ID, PARENT_RUN_ID
            )
            final_db_loaded = True
            logging.info("SKYGEN_CHILD_FINAL_DB_LOADED run_id=%s", RUN_ID)
            continue

        if (
            state == "parent_stopped"
            and target == RUN_ID
            and parent_id == PARENT_RUN_ID
            and final_db_loaded
        ):
            logging.info("SKYGEN_PARENT_STOP_CONFIRMED parent_run_id=%s", PARENT_RUN_ID)
            return

        parent = await get_workflow_run(http, PARENT_RUN_ID)
        if parent.get("status") == "completed":
            if not final_db_loaded:
                logging.warning("Parent completed before final marker; refreshing database before polling.")
                for suffix in ("", "-wal", "-shm"):
                    Path(f"{DB_PATH}{suffix}").unlink(missing_ok=True)
                await download_database(http, DB_OWNER, DB_REPO_NAME, DB_BRANCH)
            logging.warning("Parent run is completed; starting child poller after database refresh.")
            return

        await asyncio.sleep(POLL_INTERVAL_SECONDS)

    raise RuntimeError("Timed out waiting for previous run to stop; refusing to start polling.")


async def save_user(message: Message, is_start=False):
    """Return 'new', 'old', or None if writes are paused during handoff."""
    if not message.from_user:
        return None
    if HANDOFF_STARTED:
        return "switching"
    user = message.from_user
    async with DB_LOCK:
        if HANDOFF_STARTED:
            return "switching"
        with sqlite3.connect(DB_PATH, timeout=30) as connection:
            exists = connection.execute("SELECT 1 FROM users WHERE user_id = ?", (user.id,)).fetchone()
            status = "old" if exists else "new"
            connection.execute(
                """
                INSERT INTO users
                    (user_id, username, first_name, first_started_at, last_seen_at, start_count)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    username = excluded.username,
                    first_name = excluded.first_name,
                    last_seen_at = CURRENT_TIMESTAMP,
                    start_count = users.start_count + excluded.start_count
                """,
                (user.id, user.username, user.first_name or "", 1 if is_start else 0),
            )
            connection.commit()
    return status


async def command_start(message: Message):
    status = await save_user(message, is_start=True)
    if status is None:
        return
    if status == "switching":
        await message.answer("🔄 SkyGen is switching servers. Please wait a moment and try again.")
        return
    user = message.from_user
    name = (user.first_name or "there") if user else "there"
    label = "🆕 New User" if status == "new" else "👤 Old User"
    await message.answer(
        f"🚀 Welcome, {name}!\n{label}\n\n"
        "SkyGen is online.\n\n"
        "/start - Register/check your user record\n"
        "/ping - Check bot response\n"
        "/id - Show your Telegram user ID"
    )


async def command_ping(message: Message):
    started = time.perf_counter()
    status = await save_user(message)
    if status is None:
        return
    if status == "switching":
        await message.answer("🔄 SkyGen is switching servers. Please wait a moment.")
        return
    sent = await message.answer("🏓 Measuring...")
    latency = (time.perf_counter() - started) * 1000
    await asyncio.sleep(0.5)
    try:
        await sent.edit_text(
            "🏓 Pong!\n\n"
            f"⚡ Response time: {latency:.2f} ms\n"
            f"⏱ Uptime: {log_uptime()}\n"
            "🟢 Status: Online"
        )
    except Exception:
        logging.warning("Could not edit ping response.")


async def command_id(message: Message):
    status = await save_user(message)
    if status is None:
        return
    if status == "switching":
        await message.answer("🔄 SkyGen is switching servers. Please wait a moment.")
        return
    if message.from_user:
        await message.answer(f"🆔 Your Telegram user ID: {message.from_user.id}")


dp.message.register(command_start, CommandStart())
dp.message.register(command_ping, Command("ping"))
dp.message.register(command_id, Command("id"))


def exception_has_conflict(error):
    if isinstance(error, TelegramConflictError):
        return True
    if isinstance(error, BaseExceptionGroup):
        return any(exception_has_conflict(item) for item in error.exceptions)
    return False


async def stop_at_hard_limit():
    remaining = max(0, START_TIME + RUN_SECONDS - time.monotonic())
    if remaining:
        await asyncio.sleep(remaining)
    try:
        await dp.stop_polling()
    except RuntimeError:
        pass


async def handoff_task():
    """Prepare the child in the background while the parent keeps serving commands."""
    global HANDOFF_STARTED, TARGET_RUN_ID
    wait_seconds = max(0, HANDOFF_AT_SECONDS - (time.monotonic() - START_TIME))
    await asyncio.sleep(wait_seconds)
    logging.info("SKYGEN_HANDOFF_PREPARE_BEGIN polling_remains_active=true")

    snapshot = None
    timeout = aiohttp.ClientTimeout(total=45, connect=15, sock_read=30)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as http:
            # Dispatch early, but do NOT pause handlers or polling yet.
            await write_handoff_status(http, "dispatching", "", RUN_ID)
            TARGET_RUN_ID = await dispatch_next_run(http)
            await write_handoff_status(http, "handoff_requested", TARGET_RUN_ID, RUN_ID)
            logging.info("SKYGEN_HANDOFF_TARGET run_id=%s", TARGET_RUN_ID)

            hard_deadline = START_TIME + RUN_SECONDS
            cutover_at = START_TIME + (RUN_SECONDS - 20)
            child_prepared = False

            # Keep the old bot fully operational while GitHub queues and starts the child.
            while time.monotonic() < cutover_at:
                run = await get_workflow_run(http, TARGET_RUN_ID)
                run_status = run.get("status", "unknown")
                if run_status in ("failure", "cancelled", "timed_out") or (
                    run_status == "completed" and run.get("conclusion") != "success"
                ):
                    raise RuntimeError(
                        f"Child run failed before cutover: status={run_status}, conclusion={run.get('conclusion')}"
                    )
                status = await read_handoff_status(http)
                child_prepared = (
                    status.get("state") == "ready_for_final_sync"
                    and str(status.get("target_run_id")) == TARGET_RUN_ID
                    and str(status.get("parent_run_id")) == RUN_ID
                )
                if child_prepared and run_status == "in_progress":
                    try:
                        if await verify_run_job(http, TARGET_RUN_ID):
                            logging.info("SKYGEN_CHILD_PREPARED run_id=%s; parent keeps polling until cutover", TARGET_RUN_ID)
                    except Exception as error:
                        logging.warning("Could not verify child job yet (%s).", type(error).__name__)
                await asyncio.sleep(POLL_INTERVAL_SECONDS)

            # Last short cutover window: pause DB writes, commit a final snapshot, and
            # only then let the child load that exact snapshot and take over polling.
            HANDOFF_STARTED = True
            logging.info("SKYGEN_HANDOFF_CUTOVER_BEGIN writes_paused=true")
            snapshot = await create_snapshot()
            await upload_database(http, snapshot)
            await write_handoff_status(http, "final_snapshot_ready", TARGET_RUN_ID, RUN_ID)

            while time.monotonic() < hard_deadline:
                run = await get_workflow_run(http, TARGET_RUN_ID)
                run_status = run.get("status", "unknown")
                if run_status in ("failure", "cancelled", "timed_out") or (
                    run_status == "completed" and run.get("conclusion") != "success"
                ):
                    logging.error("SKYGEN_CHILD_RUN_FAILED run_id=%s status=%s conclusion=%s", TARGET_RUN_ID, run_status, run.get("conclusion"))
                    await stop_at_hard_limit()
                    return

                status = await read_handoff_status(http)
                child_ready = (
                    status.get("state") == "child_ready"
                    and str(status.get("target_run_id")) == TARGET_RUN_ID
                    and str(status.get("parent_run_id")) == RUN_ID
                )
                job_active = False
                if run_status == "in_progress":
                    try:
                        job_active = await verify_run_job(http, TARGET_RUN_ID)
                    except Exception as error:
                        logging.warning("Could not verify child job yet (%s).", type(error).__name__)

                if run_status == "in_progress" and child_ready and job_active:
                    logging.info("SKYGEN_CHILD_READY run_id=%s; stopping parent poller for exclusive Telegram polling", TARGET_RUN_ID)
                    try:
                        await dp.stop_polling()
                    except RuntimeError:
                        pass
                    await write_handoff_status(http, "parent_stopped", TARGET_RUN_ID, RUN_ID)
                    return

                await asyncio.sleep(1)

            logging.warning("Handoff reached the 10-minute test limit; stopping parent poller.")
            try:
                await dp.stop_polling()
            except RuntimeError:
                pass
    except Exception:
        logging.exception("SKYGEN_HANDOFF_FAILED; current run remains active until its hard limit.")
        # If handoff preparation fails, keep serving commands until the hard limit.
        # The child will wait for this run to complete and refresh the latest DB snapshot.
        HANDOFF_STARTED = False
        await stop_at_hard_limit()
    finally:
        if snapshot:
            snapshot.unlink(missing_ok=True)


async def main():
    global START_TIME, DB_OWNER, DB_REPO_NAME, DB_BRANCH, PREPARED_FOR_HANDOFF
    if not BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN secret is missing.")
    if not GITHUB_PAT:
        raise RuntimeError("GH_PAT secret is missing.")
    if not DB_REPO_NAME:
        raise RuntimeError("DB_REPO_NAME is empty.")

    api_timeout = aiohttp.ClientTimeout(total=45, connect=15, sock_read=30)
    async with aiohttp.ClientSession(timeout=api_timeout) as http:
        DB_OWNER, DB_REPO_NAME, DB_BRANCH = await ensure_private_database_repo(http)
        await download_database(http, DB_OWNER, DB_REPO_NAME, DB_BRANCH)

    START_TIME = time.monotonic()
    bot = Bot(token=BOT_TOKEN, session=AiohttpSession())
    handoff = None
    try:
        me = await bot.get_me()
        logging.info("SKYGEN_BOT_CONNECTED username=@%s run_id=%s", me.username, RUN_ID)

        if PARENT_RUN_ID:
            async with aiohttp.ClientSession(timeout=api_timeout) as http:
                await write_handoff_status(http, "ready_for_final_sync", RUN_ID, PARENT_RUN_ID)
                logging.info("SKYGEN_HANDOFF_STATE state=ready_for_final_sync target_run_id=%s parent_run_id=%s", RUN_ID, PARENT_RUN_ID)
                await child_wait_for_parent_to_stop(http)

        logging.info("SKYGEN_POLLING_START run_id=%s", RUN_ID)
        handoff = asyncio.create_task(handoff_task(), name="sqlite-handoff")
        await dp.start_polling(bot, polling_timeout=10, allowed_updates=dp.resolve_used_update_types())
    except BaseException as error:
        if exception_has_conflict(error):
            logging.info("SKYGEN_TELEGRAM_CONFLICT run_id=%s; exiting this poller cleanly", RUN_ID)
        elif isinstance(error, asyncio.CancelledError):
            raise
        else:
            raise
    finally:
        if handoff and not handoff.done():
            # Let a handoff that already stopped polling finish writing its marker.
            try:
                await asyncio.wait_for(asyncio.shield(handoff), timeout=15)
            except (asyncio.TimeoutError, Exception):
                handoff.cancel()
                try:
                    await handoff
                except asyncio.CancelledError:
                    pass
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
