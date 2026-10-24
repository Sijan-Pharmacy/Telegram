"""Telegram command handlers only. Lifecycle and persistence live in runtime.py."""
import asyncio
import logging
import time

from aiogram import Dispatcher
from aiogram.filters import Command, CommandStart
from aiogram.types import Message

import runtime

dp = Dispatcher()

async def command_start(message: Message):
    status = await runtime.save_user(message, is_start=True)
    if status is None:
        return
    if status == "switching":
        await runtime.send_switching_notice(message)
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
    status = await runtime.save_user(message)
    if status is None:
        return
    if status == "switching":
        await runtime.send_switching_notice(message)
        return
    sent = await message.answer("🏓 Measuring...")
    latency = (time.perf_counter() - started) * 1000
    await asyncio.sleep(0.5)
    try:
        await sent.edit_text(
            "🏓 Pong!\n\n"
            f"⚡ Response time: {latency:.2f} ms\n"
            f"⏱ Uptime: {runtime.log_uptime()}\n"
            "🟢 Status: Online"
        )
    except Exception:
        logging.warning("Could not edit ping response.")

async def command_id(message: Message):
    status = await runtime.save_user(message)
    if status is None:
        return
    if status == "switching":
        await runtime.send_switching_notice(message)
        return
    if message.from_user:
        await message.answer(f"🆔 Your Telegram user ID: {message.from_user.id}")

async def switching_fallback(message: Message):
    """During handoff, acknowledge non-command messages instead of silently ignoring them."""
    if runtime.HANDOFF_STARTED:
        await runtime.send_switching_notice(message)

dp.message.register(command_start, CommandStart())
dp.message.register(command_ping, Command("ping"))
dp.message.register(command_id, Command("id"))
dp.message.register(switching_fallback)
