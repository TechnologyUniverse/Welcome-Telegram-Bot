import os
from dotenv import load_dotenv
import asyncio
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery
)
from aiogram.types import ChatPermissions
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
import logging
import time
from dataclasses import dataclass
import signal

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","message":"%(message)s"}'
)


VERSION = "1.2.16"
# FEATURE:
# Welcome message supports optional image via WELCOME_IMAGE_URL
# FINAL RELEASE:
# Версия 1.2.15 является финальной.
# Ветка 1.2.x официально закрыта.
# Допускаются только критические security-fix при необходимости.

START_TIME = time.time()

# ================== CONFIG LOADER ==================
@dataclass(frozen=True)
class Config:
    bot_token: str
    project_name: str
    storage_url: str
    auto_delete_seconds: int
    mute_new_users: bool
    mute_seconds: int
    admin_ids: set[int]
    allowed_chat_ids: set[int]
    welcome_delay_seconds: int
    faq_url: str | None
    support_url: str | None
    welcome_message_ttl: int
    rules_message_ttl: int
    bot_mode: str
    welcome_image_url: str | None


def _env_bool(key: str, default: bool) -> bool:
    return os.getenv(key, str(default)).lower() == "true"


def load_config() -> Config:
    bot_token = os.getenv("BOT_TOKEN")
    if not bot_token:
        raise RuntimeError(
            "BOT_TOKEN не найден. Проверь файл .env и переменную BOT_TOKEN"
        )

    project_name = os.getenv("PROJECT_NAME", "Technology Universe")
    storage_url = os.getenv("STORAGE_URL", "https://example.com/storage")

    try:
        auto_delete_seconds = int(os.getenv("AUTO_DELETE_SECONDS", "60"))
        mute_seconds = int(os.getenv("MUTE_SECONDS", "120"))
    except ValueError:
        raise RuntimeError("AUTO_DELETE_SECONDS и MUTE_SECONDS должны быть числами")

    try:
        welcome_delay_seconds = int(os.getenv("WELCOME_DELAY_SECONDS", "3"))
    except ValueError:
        raise RuntimeError("WELCOME_DELAY_SECONDS должен быть числом")

    try:
        welcome_message_ttl = int(os.getenv("WELCOME_MESSAGE_TTL", "180"))
        rules_message_ttl = int(os.getenv("RULES_MESSAGE_TTL", "300"))
    except ValueError:
        raise RuntimeError("WELCOME_MESSAGE_TTL и RULES_MESSAGE_TTL должны быть числами")

    mute_new_users = _env_bool("MUTE_NEW_USERS", True)

    admin_ids: set[int] = set()
    raw_admin_ids = os.getenv("ADMIN_IDS", "")
    for x in raw_admin_ids.split(","):
        x = x.strip()
        if not x:
            continue
        try:
            admin_ids.add(int(x))
        except ValueError:
            logging.warning(f"ENV | invalid admin id ignored: {x}")

    allowed_chat_ids: set[int] = set()
    raw_chat_ids = os.getenv("ALLOWED_CHAT_IDS", "")
    for x in raw_chat_ids.split(","):
        x = x.strip()
        if not x:
            continue
        try:
            allowed_chat_ids.add(int(x))
        except ValueError:
            logging.warning(f"ENV | invalid chat id ignored: {x}")

    faq_url = os.getenv("FAQ_URL")
    support_url = os.getenv("SUPPORT_URL")

    bot_mode = os.getenv("BOT_MODE", "prod").lower()
    if bot_mode not in {"prod", "test"}:
        raise RuntimeError("BOT_MODE должен быть prod или test")

    welcome_image_url = os.getenv("WELCOME_IMAGE_URL")

    return Config(
        bot_token=bot_token,
        project_name=project_name,
        storage_url=storage_url,
        auto_delete_seconds=auto_delete_seconds,
        mute_new_users=mute_new_users,
        mute_seconds=mute_seconds,
        admin_ids=admin_ids,
        allowed_chat_ids=allowed_chat_ids,
        welcome_delay_seconds=welcome_delay_seconds,
        faq_url=faq_url,
        support_url=support_url,
        welcome_message_ttl=welcome_message_ttl,
        rules_message_ttl=rules_message_ttl,
        bot_mode=bot_mode,
        welcome_image_url=welcome_image_url,
    )
# ================================================

CFG = load_config()

# ================== RUNTIME STATE ==================
# user_id -> last_welcome_timestamp
WELCOME_CACHE: dict[int, float] = {}
WELCOME_CACHE_MAX = 10_000
WELCOME_TTL_SECONDS = 300  # 5 минут защита от повторного welcome

# user_id -> last_rules_timestamp
RULES_CACHE: dict[int, float] = {}
RULES_CACHE_MAX = 10_000
RULES_TTL_SECONDS = 300  # 5 минут антиспам для правил
# ================================================

# bot_message_id -> (timestamp, message_type)
BOT_MESSAGES: dict[int, tuple[float, str]] = {}

BOT_MESSAGES_CHAT_ID: dict[int, int] = {}

BOT_MESSAGES_LOCK = asyncio.Lock()

# ================== LOCALIZATION ==================
SUPPORTED_LANGS = {"ru", "en"}
DEFAULT_LANG = "ru"

TEXTS = {
    "ru": {
        "welcome": (
            "👋 <b>Добро пожаловать в закрытое Telegram-сообщество проекта {project}</b>\n\n"
            "Сообщество предназначено для профессионального общения и получения актуальной информации "
            "по продуктам <b>Apple</b>, операционным системам <b>Apple</b> и <b>Microsoft</b>, "
            "а также по программному обеспечению.\n\n"
            "Здесь вы найдёте:\n"
            "• экспертную аналитику и технологические обзоры\n"
            "• тестирование решений и разборы ошибок\n"
            "• практические рекомендации и решения проблем с программным обеспечением\n"
            "• ответы на технические вопросы и индивидуальную техническую поддержку\n\n"
            "Сообщество создано для обмена опытом, обсуждения обновлений и получения проверенной информации "
            "по технологиям и продуктам проекта.\n\n"
            "<b>Спасибо за подписку. Оставайтесь с нами.</b>\n\n"
            "⬇️ <i>Выберите действие ниже.</i>"
        ),
        "rules": (
            "📜 <b>Правила чата Technology Universe:</b>\n\n"
            "1️⃣ <b>Тематика сообщества</b>\n"
            "Обсуждаем продукты Apple, операционные системы Apple и Microsoft, "
            "программное обеспечение, обновления, тестирование и решение технических проблем.\n\n"
            "2️⃣ <b>Уважительное общение</b>\n"
            "Запрещены оскорбления, токсичность, троллинг и переходы на личности.\n\n"
            "3️⃣ <b>Без спама и рекламы</b>\n"
            "Реклама, самопиар и сторонние проекты запрещены без согласования с администрацией.\n\n"
            "4️⃣ <b>Вопросы по делу</b>\n"
            "Формулируйте вопросы чётко и по существу, при необходимости указывайте версию ОС и ПО.\n\n"
            "5️⃣ <b>Флуд и оффтоп</b>\n"
            "Флуд, мемы и сообщения не по теме сообщества запрещены.\n\n"
            "6️⃣ <b>Запрещённый контент</b>\n"
            "Запрещено публиковать вредоносные ссылки и материалы, нарушающие правила Telegram "
            "и действующее законодательство.\n\n"
            "7️⃣ <b>Решения администрации</b>\n"
            "Решения администрации и модераторов обязательны к исполнению.\n\n"
            "8️⃣ <b>Поддержка и доступ</b>\n"
            "В сообществе вы получаете профессиональную помощь в решении технических проблем, "
            "а также доступ к хранилищу материалов проекта."
        ),
        "btn_storage": "📦 Хранилище",
        "btn_rules": "📜 Правила",
        "health_ok": "✅ <b>Welcome Bot — OK</b>",
    },
    "en": {
        "welcome": (
            "👋 <b>Welcome, {name}!</b>\n\n"
            "You have joined the official community of "
            "<b>{project}</b>.\n\n"
            "📌 <b>About this chat:</b>\n"
            "• Updates and releases discussion\n"
            "• Technical support\n"
            "• Official information\n\n"
            "Choose an option below ⬇️"
        ),
        "rules": (
            "📜 <b>Chat rules:</b>\n\n"
            "1️⃣ No spam or advertising\n"
            "2️⃣ Stay on topic\n"
            "3️⃣ Be respectful\n"
            "4️⃣ No flooding or off-topic\n"
            "5️⃣ Follow Telegram rules"
        ),
        "btn_storage": "📦 Storage",
        "btn_rules": "📜 Rules",
        "health_ok": "✅ <b>Welcome Bot — OK</b>",
    },
}
# ================================================

bot = Bot(
    token=CFG.bot_token,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()


async def bot_has_permissions(chat_id: int) -> dict[str, bool]:
    try:
        me = await bot.get_me()
        member = await bot.get_chat_member(chat_id, me.id)

        can_delete = getattr(member, "can_delete_messages", False)
        can_restrict = getattr(member, "can_restrict_members", False)

        return {
            "delete": bool(can_delete),
            "restrict": bool(can_restrict),
        }
    except Exception as e:
        logging.warning(
            f"PERMISSIONS | failed to fetch | chat={chat_id} | error={e}"
        )
        return {
            "delete": False,
            "restrict": False,
        }


def is_admin(user_id: int) -> bool:
    return user_id in CFG.admin_ids if CFG.admin_ids else False


def is_allowed_chat(chat_id: int) -> bool:
    return chat_id in CFG.allowed_chat_ids if CFG.allowed_chat_ids else True

def is_test_mode() -> bool:
    return CFG.bot_mode == "test"


def detect_lang(user_lang: str | None) -> str:
    if not user_lang:
        return DEFAULT_LANG
    lang = user_lang.split("-")[0].lower()
    return lang if lang in SUPPORTED_LANGS else DEFAULT_LANG


def t(lang: str, key: str) -> str:
    return TEXTS.get(lang, TEXTS[DEFAULT_LANG])[key]


def welcome_keyboard(lang: str) -> InlineKeyboardMarkup:
    buttons = [
        [
            InlineKeyboardButton(
                text=t(lang, "btn_storage"),
                url=CFG.storage_url
            ),
            InlineKeyboardButton(
                text=t(lang, "btn_rules"),
                callback_data=f"rules:{lang}"
            )
        ]
    ]

    extra = []
    if CFG.faq_url:
        extra.append(InlineKeyboardButton(text="❓ FAQ", url=CFG.faq_url))
    if CFG.support_url:
        extra.append(InlineKeyboardButton(text="🆘 Support", url=CFG.support_url))

    if extra:
        buttons.append(extra)

    return InlineKeyboardMarkup(inline_keyboard=buttons)


def is_paid_like_chat(message: Message) -> bool:
    """
    UX-эвристика для платных / закрытых чатов.
    Не является платёжной логикой.
    """
    chat = message.chat
    return bool(
        getattr(chat, "has_protected_content", False)
        or getattr(chat, "join_by_request", False)
        or getattr(chat, "join_to_send_messages", False)
    )


@dp.message(F.new_chat_members)
async def welcome_new_user(message: Message):
    # Проверка разрешённого чата
    if not is_allowed_chat(message.chat.id):
        logging.info(
            f"SKIP chat | chat_id={message.chat.id} | not allowed"
        )
        return

    perms = await bot_has_permissions(message.chat.id)

    paid_like = is_paid_like_chat(message)

    if paid_like:
        logging.info(
            f"PAID_LIKE_CHAT | chat={message.chat.id} | mute/autodelete disabled"
        )

    if not perms["delete"] or not perms["restrict"]:
        logging.warning(
            f"PERMISSIONS | chat={message.chat.id} "
            f"delete={perms['delete']} restrict={perms['restrict']}"
        )

    # Удаляем service-сообщение "пользователь вошёл"
    if perms["delete"]:
        try:
            await message.delete()
        except Exception:
            pass

    if not message.new_chat_members:
        return

    for user in message.new_chat_members:
        if user.is_bot:
            continue

        now = time.time()
        last_time = WELCOME_CACHE.get(user.id)

        if last_time and (now - last_time) < WELCOME_TTL_SECONDS:
            logging.info(
                f"SKIP welcome | user={user.id} | duplicate join"
            )
            continue

        WELCOME_CACHE[user.id] = now
        if len(WELCOME_CACHE) > WELCOME_CACHE_MAX:
            WELCOME_CACHE.clear()
            logging.warning("CACHE | WELCOME_CACHE cleared (limit exceeded)")

        if (
            CFG.mute_new_users
            and perms["restrict"]
            and not is_test_mode()
            and not paid_like
        ):
            try:
                await bot.restrict_chat_member(
                    chat_id=message.chat.id,
                    user_id=user.id,
                    permissions=ChatPermissions(
                        can_send_messages=False,
                        can_send_media_messages=False,
                        can_send_other_messages=False,
                        can_add_web_page_previews=False
                    ),
                    until_date=int(time.time()) + CFG.mute_seconds
                )
                logging.info(
                    f"MUTED | user={user.id} | seconds={CFG.mute_seconds}"
                )
            except Exception as e:
                logging.warning(
                    f"MUTE FAILED | user={user.id} | error={e}"
                )

        logging.info(
            f"WELCOME | user={user.id}"
        )

        lang = detect_lang(user.language_code)
        safe_name = user.full_name or "User"
        text = t(lang, "welcome").format(
            name=safe_name,
            project=CFG.project_name
        )

        if is_test_mode():
            text = "🧪 <i>Test mode</i>\n\n" + text

        if CFG.welcome_delay_seconds > 0:
            await asyncio.sleep(CFG.welcome_delay_seconds)

        if CFG.welcome_image_url:
            msg = await bot.send_photo(
                chat_id=message.chat.id,
                photo=CFG.welcome_image_url,
                caption=text,
                reply_markup=welcome_keyboard(lang)
            )
        else:
            msg = await message.answer(
                text,
                reply_markup=welcome_keyboard(lang)
            )

        async with BOT_MESSAGES_LOCK:
            BOT_MESSAGES[msg.message_id] = (time.time(), "welcome")
            BOT_MESSAGES_CHAT_ID[msg.message_id] = message.chat.id

        if (
            CFG.auto_delete_seconds > 0
            and not is_test_mode()
            and not paid_like
        ):
            await asyncio.sleep(CFG.auto_delete_seconds)
            await msg.delete()



@dp.callback_query(F.data.startswith("rules:"))
async def show_rules(callback: CallbackQuery):
    if not callback.data or not callback.message or not callback.from_user:
        logging.warning("CALLBACK | invalid payload")
        return

    # Always answer callback once (Telegram requirement)
    try:
        await callback.answer()
    except Exception:
        return

    parts = callback.data.split(":", 1)
    lang = parts[1] if len(parts) == 2 else DEFAULT_LANG

    user_id = callback.from_user.id
    now = time.time()

    # Silent anti-spam protection
    last_time = RULES_CACHE.get(user_id)
    if last_time and (now - last_time) < RULES_TTL_SECONDS:
        return

    RULES_CACHE[user_id] = now
    if len(RULES_CACHE) > RULES_CACHE_MAX:
        RULES_CACHE.clear()
        logging.warning("CACHE | RULES_CACHE cleared")

    rules_text = t(lang, "rules")
    if is_test_mode():
        rules_text = "🧪 <i>Test mode</i>\n\n" + rules_text

    msg = await callback.message.answer(rules_text)

    async with BOT_MESSAGES_LOCK:
        BOT_MESSAGES[msg.message_id] = (time.time(), "rules")
        BOT_MESSAGES_CHAT_ID[msg.message_id] = callback.message.chat.id



@dp.message(F.text == "/version")
async def version_cmd(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return
    await message.answer(
        "ℹ️ <b>Welcome Bot</b>\n"
        f"Version: {VERSION}\n"
        "Channel: Stable (1.2.x)"
    )


@dp.message(F.text == "/health")
async def health_check(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return

    uptime = int(time.time() - START_TIME)
    perms = await bot_has_permissions(message.chat.id)

    warnings = []

    if not perms["delete"]:
        warnings.append("No permission to delete messages")
    if not perms["restrict"]:
        warnings.append("No permission to restrict members")
    if not CFG.admin_ids:
        warnings.append("ADMIN_IDS is empty")
    if not CFG.allowed_chat_ids:
        warnings.append("ALLOWED_CHAT_IDS is empty (all chats allowed)")

    status = "OK" if not warnings else "WARN"

    text = (
        f"🩺 <b>Welcome Bot — Health</b>\n\n"
        f"Status: {'✅ OK' if status == 'OK' else '⚠️ WARN'}\n"
        f"Version: {VERSION}\n"
        f"Mode: {CFG.bot_mode}\n"
        f"Uptime: {uptime}s\n\n"
        "Permissions:\n"
        f"• Delete messages: {perms['delete']}\n"
        f"• Restrict members: {perms['restrict']}\n\n"
        "Runtime:\n"
        f"• Active welcome messages: {sum(1 for m in BOT_MESSAGES.values() if m[1] == 'welcome')}\n"
        f"• Active rules messages: {sum(1 for m in BOT_MESSAGES.values() if m[1] == 'rules')}\n"
    )

    if warnings:
        text += "\n⚠️ <b>Warnings:</b>\n"
        for w in warnings:
            text += f"• {w}\n"

    await message.answer(text)
@dp.message(F.text.startswith("/"))
async def unknown_command(message: Message):
    if not message.from_user:
        return

    if is_admin(message.from_user.id):
        await message.answer(
            "ℹ️ Неизвестная команда\n"
            "Используйте /health или /version"
        )


async def cleanup_bot_messages():
    while not shutdown_event.is_set():
        now = time.time()
        to_delete: list[int] = []

        async with BOT_MESSAGES_LOCK:
            for msg_id, (ts, msg_type) in BOT_MESSAGES.items():
                ttl = (
                    CFG.welcome_message_ttl
                    if msg_type == "welcome"
                    else CFG.rules_message_ttl
                )
                if (now - ts) > ttl:
                    to_delete.append(msg_id)

        for msg_id in to_delete:
            chat_id = BOT_MESSAGES_CHAT_ID.get(msg_id)
            if not chat_id:
                continue
            try:
                await bot.delete_message(
                    chat_id=chat_id,
                    message_id=msg_id
                )
            except Exception as e:
                logging.warning(f"CLEANUP | delete failed | msg_id={msg_id} | error={e}")

            async with BOT_MESSAGES_LOCK:
                BOT_MESSAGES.pop(msg_id, None)
                BOT_MESSAGES_CHAT_ID.pop(msg_id, None)

        await asyncio.sleep(60)

async def cleanup_caches():
    while not shutdown_event.is_set():
        now = time.time()

        try:
            # welcome cache
            for user_id, ts in list(WELCOME_CACHE.items()):
                if (now - ts) > WELCOME_TTL_SECONDS:
                    WELCOME_CACHE.pop(user_id, None)

            # rules cache
            for user_id, ts in list(RULES_CACHE.items()):
                if (now - ts) > RULES_TTL_SECONDS:
                    RULES_CACHE.pop(user_id, None)
        except Exception as e:
            logging.warning(f"CACHE | cleanup failed | error={e}")

        await asyncio.sleep(300)  # каждые 5 минут

shutdown_event = asyncio.Event()


def _handle_shutdown():
    logging.info("SHUTDOWN | signal received")
    shutdown_event.set()

async def main():
    logging.info(
        f"STARTUP | version={VERSION} "
        f"mute={CFG.mute_new_users} "
        f"delay={CFG.welcome_delay_seconds}s "
        f"autodelete={CFG.auto_delete_seconds}s"
    )
    logging.info(f"BUILD | version={VERSION} channel=stable")
    if not CFG.admin_ids:
        logging.warning("ENV | ADMIN_IDS is empty")

    if not CFG.allowed_chat_ids:
        logging.warning("ENV | ALLOWED_CHAT_IDS is empty (bot allowed in all chats)")
    logging.info(f"FINAL | {VERSION} production ready (official final)")
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_shutdown)
        except NotImplementedError:
            pass
    logging.info("RUNTIME | async lifecycle guards enabled")
    tasks = []

    if not is_test_mode():
        tasks.append(asyncio.create_task(cleanup_bot_messages()))
        tasks.append(asyncio.create_task(cleanup_caches()))

    polling = asyncio.create_task(dp.start_polling(bot))
    tasks.append(polling)

    await shutdown_event.wait()

    for task in tasks:
        task.cancel()

    for task in tasks:
        try:
            await task
        except asyncio.CancelledError:
            pass

    logging.info("SHUTDOWN | all tasks stopped cleanly")


if __name__ == "__main__":
    asyncio.run(main())