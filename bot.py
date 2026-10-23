import asyncio
import base64
import logging
import os
import sqlite3
import tempfile
import time
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

GITHUB_API = "https://api.github.com"
DB_PATH = Path("data/database.sqlite")
RUN_SECONDS = 10 * 60       # Test cycle: 10 minutes
HANDOFF_AT_SECONDS = 8 * 60  # Save DB and dispatch next run at 8 minutes
START_TIME = time.monotonic()

dp = Dispatcher()
DB_LOCK = asyncio.Lock()
HANDOFF_STARTED = False
DB_OWNER = ""
DB_BRANCH = "main"


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
    async with session.request(
        method, url, headers=gh_headers(), **kwargs
    ) as response:
        body = await response.text()
        if response.status >= 400:
            # Never include request headers or token in errors/logs.
            raise RuntimeError(
                f"GitHub API {method} failed with HTTP {response.status}: "
                f"{body[:300]}"
            )
        if not body:
            return None
        import json
        return json.loads(body)


async def ensure_private_database_repo(http):
    """Find or create a private repository under the PAT owner's account."""
    if not GITHUB_PAT:
        raise RuntimeError("Missing GH_PAT secret.")

    user = await github_json(http, "GET", f"{GITHUB_API}/user")
    owner = user["login"]
    repo_url = f"{GITHUB_API}/repos/{quote(owner, safe='')}/{quote(DB_REPO_NAME, safe='')}"

    async with http.get(repo_url, headers=gh_headers()) as response:
        if response.status == 200:
            repo = await response.json()
        elif response.status == 404:
            repo = None
        else:
            raise RuntimeError(
                f"Cannot check database repository (HTTP {response.status})."
            )

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
            f"Database repository {repo['full_name']} is public. "
            "Make it private before running this bot."
        )

    branch = repo.get("default_branch") or "main"
    return owner, repo["name"], branch


async def download_database(http, owner, repo, branch):
    """Download the latest SQLite file; initialize an empty DB if absent."""
    global DB_PATH
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    url = (
        f"{GITHUB_API}/repos/{quote(owner, safe='')}/"
        f"{quote(repo, safe='')}/contents/database.sqlite"
    )

    async with http.get(
        url,
        headers=gh_headers(),
        params={"ref": branch},
    ) as response:
        if response.status == 404:
            logging.info("No saved SQLite file yet; creating a new database.")
            initialize_database(DB_PATH)
            return
        if response.status != 200:
            raise RuntimeError(
                f"Cannot download database (HTTP {response.status})."
            )
        metadata = await response.json()

    content = metadata.get("content", "").strip()
    if metadata.get("encoding") == "base64" and content:
        raw = base64.b64decode(content)
    else:
        # GitHub may omit JSON content for files larger than 1 MiB.
        async with http.get(
            url,
            headers=gh_headers("application/vnd.github.raw"),
            params={"ref": branch},
        ) as response:
            if response.status != 200:
                raise RuntimeError(
                    f"Cannot download raw database (HTTP {response.status})."
                )
            raw = await response.read()

    DB_PATH.write_bytes(raw)
    check_database(DB_PATH)
    logging.info("Loaded SQLite database (%s bytes).", len(raw))


def initialize_database(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT NOT NULL DEFAULT '',
                first_started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                start_count INTEGER NOT NULL DEFAULT 0
            )
        """)
        connection.commit()


def check_database(path):
    with sqlite3.connect(path) as connection:
        result = connection.execute("PRAGMA integrity_check").fetchone()
        if not result or result[0] != "ok":
            raise RuntimeError("Downloaded SQLite database failed integrity check.")
        connection.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT NOT NULL DEFAULT '',
                first_started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                start_count INTEGER NOT NULL DEFAULT 0
            )
        """)
        connection.commit()


async def save_user(message: Message, is_start=False):
    """Persist the Telegram user locally; writes are paused during handoff."""
    if HANDOFF_STARTED or not message.from_user:
        return False

    user = message.from_user
    async with DB_LOCK:
        if HANDOFF_STARTED:
            return False
        with sqlite3.connect(DB_PATH, timeout=30) as connection:
            connection.execute(
                """
                INSERT INTO users
                    (user_id, username, first_name, first_started_at,
                     last_seen_at, start_count)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    username = excluded.username,
                    first_name = excluded.first_name,
                    last_seen_at = CURRENT_TIMESTAMP,
                    start_count = users.start_count + excluded.start_count
                """,
                (
                    user.id,
                    user.username,
                    user.first_name or "",
                    1 if is_start else 0,
                ),
            )
            connection.commit()
    return True


async def create_snapshot():
    """Use SQLite's backup API so the snapshot is transactionally consistent."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    fd, snapshot_name = tempfile.mkstemp(suffix=".sqlite")
    os.close(fd)
    snapshot = Path(snapshot_name)

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


async def upload_database(http, owner, repo, branch, snapshot):
    """Commit database.sqlite to the private repo using GitHub Contents API."""
    url = (
        f"{GITHUB_API}/repos/{quote(owner, safe='')}/"
        f"{quote(repo, safe='')}/contents/database.sqlite"
    )

    async with http.get(
        url,
        headers=gh_headers(),
        params={"ref": branch},
    ) as response:
        if response.status == 200:
            current = await response.json()
            sha = current.get("sha")
        elif response.status == 404:
            sha = None
        else:
            raise RuntimeError(
                f"Cannot inspect existing database file (HTTP {response.status})."
            )

    raw = snapshot.read_bytes()
    import json
    payload = {
        "message": f"Update SQLite database ({int(time.time())})",
        "content": base64.b64encode(raw).decode("ascii"),
        "branch": branch,
    }
    if sha:
        payload["sha"] = sha

    async with http.put(
        url,
        headers=gh_headers(),
        json=payload,
    ) as response:
        if response.status not in (200, 201):
            body = await response.text()
            raise RuntimeError(
                f"Database upload failed (HTTP {response.status}): {body[:300]}"
            )

    logging.info("SQLite snapshot committed (%s bytes).", len(raw))


async def trigger_next_workflow(http):
    if not BOT_REPOSITORY or "/" not in BOT_REPOSITORY:
        raise RuntimeError("GITHUB_REPOSITORY environment variable is missing.")

    url = (
        f"{GITHUB_API}/repos/{BOT_REPOSITORY}/actions/workflows/"
        f"{quote(WORKFLOW_FILE, safe='')}/dispatches"
    )
    payload = {"ref": WORKFLOW_REF or "main"}

    # Retry transient API failures, without ever logging the PAT.
    last_error = None
    for attempt in range(1, 4):
        try:
            async with http.post(
                url,
                headers=gh_headers(),
                json=payload,
            ) as response:
                if response.status == 204:
                    logging.info("Next workflow dispatch accepted.")
                    return
                body = await response.text()
                last_error = f"HTTP {response.status}: {body[:250]}"
        except Exception as error:
            last_error = type(error).__name__

        logging.warning(
            "Workflow dispatch attempt %s/3 failed (%s).",
            attempt,
            last_error,
        )
        await asyncio.sleep(attempt * 2)

    raise RuntimeError("Could not trigger the next workflow after 3 attempts.")


async def handoff_task(bot):
    global HANDOFF_STARTED

    wait_seconds = max(0, HANDOFF_AT_SECONDS - (time.monotonic() - START_TIME))
    await asyncio.sleep(wait_seconds)

    HANDOFF_STARTED = True
    logging.info("Handoff started; database writes are now paused.")

    # The order is intentional: commit a complete snapshot FIRST, then dispatch.
    # The next runner downloads this committed file before starting Telegram polling.
    snapshot = None
    timeout = aiohttp.ClientTimeout(total=90, connect=20, sock_read=60)

    try:
        async with aiohttp.ClientSession(timeout=timeout) as http:
            snapshot = await create_snapshot()
            await upload_database(
                http, DB_OWNER, DB_REPO_NAME, DB_BRANCH, snapshot
            )
            await trigger_next_workflow(http)

        logging.info("Handoff requested; waiting for Telegram polling conflict.")
    except Exception:
        logging.exception("Handoff failed. Old runner will remain active until its 10-minute limit.")
        HANDOFF_STARTED = False
    finally:
        if snapshot:
            snapshot.unlink(missing_ok=True)

    # If Telegram does not terminate this poller with 409, stop at 10 minutes.
    remaining = max(0, RUN_SECONDS - (time.monotonic() - START_TIME))
    await asyncio.sleep(remaining)
    try:
        await dp.stop_polling()
    except RuntimeError:
        pass


async def command_start(message: Message):
    if HANDOFF_STARTED:
        await message.answer("🔄 SkyGen is switching runners. Please try again shortly.")
        return

    saved = await save_user(message, is_start=True)
    if not saved:
        await message.answer("🔄 SkyGen is switching runners. Please try again shortly.")
        return

    await message.answer(
        "🚀 SkyGen is online!\n\n"
        "/start - Save your user record\n"
        "/ping - Check bot response\n"
        "/id - Show your Telegram user ID"
    )


async def command_ping(message: Message):
    started = time.perf_counter()
    if not await save_user(message):
        await message.answer("🔄 SkyGen is switching runners. Please try again shortly.")
        return

    sent = await message.answer("🏓 Measuring...")
    latency = (time.perf_counter() - started) * 1000
    await asyncio.sleep(1.1)
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
    if not await save_user(message):
        await message.answer("🔄 SkyGen is switching runners. Please try again shortly.")
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


async def main():
    global START_TIME, DB_OWNER, DB_REPO_NAME, DB_BRANCH

    if not BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN secret is missing.")
    if not GITHUB_PAT:
        raise RuntimeError("GH_PAT secret is missing.")
    if not DB_REPO_NAME:
        raise RuntimeError("DB_REPO_NAME is empty.")

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    api_timeout = aiohttp.ClientTimeout(total=90, connect=20, sock_read=60)

    async with aiohttp.ClientSession(timeout=api_timeout) as http:
        DB_OWNER, DB_REPO_NAME, DB_BRANCH = await ensure_private_database_repo(http)
        await download_database(http, DB_OWNER, DB_REPO_NAME, DB_BRANCH)

    START_TIME = time.monotonic()

    bot = Bot(token=BOT_TOKEN, session=AiohttpSession())
    handoff = None

    try:
        me = await bot.get_me()
        logging.info("SkyGen online as @%s; DB repo %s/%s", me.username, DB_OWNER, DB_REPO_NAME)

        handoff = asyncio.create_task(handoff_task(bot), name="sqlite-handoff")

        try:
            await dp.start_polling(
                bot,
                polling_timeout=10,
                allowed_updates=dp.resolve_used_update_types(),
            )
        except BaseException as error:
            if exception_has_conflict(error):
                logging.info("Telegram 409 Conflict detected; another runner is polling. Old runner is exiting.")
            elif isinstance(error, asyncio.CancelledError):
                raise
            else:
                raise
    finally:
        if handoff and not handoff.done():
            handoff.cancel()
            try:
                await handoff
            except asyncio.CancelledError:
                pass
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
