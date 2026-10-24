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

    label = (
        "🆕 <b>New User</b>"
        if status == "new"
        else "👤 <b>Registered User</b>"
    )

    await message.reply(
        f"🚀 <b>Welcome to SkyGen, {name}!</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"{label}\n\n"
        "Your request has been received, and SkyGen is ready to assist you.\n\n"
        "📌 <b>Available Commands</b>\n\n"
        "▫️ /start — Register or check your user record\n"
        "▫️ /ping — Check bot response time and uptime\n"
        "▫️ /id — View your Telegram user ID\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "✨ <i>Thank you for using SkyGen!</i>",
        parse_mode="HTML",
    )


async def command_ping(message: Message):
    started = time.perf_counter()

    status = await runtime.save_user(message)

    if status is None:
        return

    if status == "switching":
        await runtime.send_switching_notice(message)
        return

    try:
        sent = await message.reply(
            "🏓 <b>Pinging...</b>\n",
            parse_mode="HTML",
        )

        latency = (time.perf_counter() - started) * 1000

        await sent.edit_text(
            "🏓 <b>Pong! SkyGen is responding.</b>\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
            f"⚡ <b>Response Time:</b> <code>{latency:.2f} ms</code>\n"
            f"⏱ <b>Bot Uptime:</b> <code>{runtime.log_uptime()}</code>\n"
            "🟢 <b>System Status:</b> Online\n\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "✅ <i>Your connection test completed successfully.</i>",
            parse_mode="HTML",
        )

    except Exception:
        logging.exception("Could not complete ping response.")


async def command_id(message: Message):
    status = await runtime.save_user(message)

    if status is None:
        return

    if status == "switching":
        await runtime.send_switching_notice(message)
        return

    user = message.from_user

    if not user:
        return

    username = (
        f"@{user.username}"
        if user.username
        else "<i>Not set</i>"
    )

    await message.reply(
        "🆔 <b>Telegram Account Information</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        "👤 <b>First Name:</b> "
        f"{user.first_name or 'Not available'}\n"
        "🆔 <b>User ID:</b>\n"
        f"<code>{user.id}</code>\n\n"
        "🔗 <b>Username:</b> "
        f"{username}\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "💡 <b>Need your Telegram ID?</b>\n"
        "You can copy the numeric ID above and use it wherever a Telegram user ID is required.\n\n"
        "🔒 <i>Keep your account secure and never share login codes or passwords.</i>",
        parse_mode="HTML",
    )


async def switching_fallback(message: Message):
    """During handoff, acknowledge non-command messages instead of silently ignoring them."""
    if runtime.HANDOFF_STARTED:
        await runtime.send_switching_notice(message)


dp.message.register(command_start, CommandStart())
dp.message.register(command_ping, Command("ping"))
dp.message.register(command_id, Command("id"))
dp.message.register(switching_fallback)
