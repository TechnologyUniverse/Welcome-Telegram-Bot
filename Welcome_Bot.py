import os
from dotenv import load_dotenv
import asyncio
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    ChatMemberUpdated
)
from aiogram.types import ChatPermissions
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
import logging
import time
from dataclasses import dataclass
import signal
from typing import cast

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","message":"%(message)s"}'
)


# ================== VERSION ==================
VERSION = "1.4.9.02"
# v1.4.9.02 — Fix Pylance ConvertibleToInt (TypedDict registry)

# ================== 1.4.5 UX & FLOOD SAFETY ==================
# Simple single-instance lock to avoid parallel polling
LOCK_FILE = "/tmp/welcome_bot.lock"
# =============================================================
def acquire_startup_lock() -> bool:
    if os.path.exists(LOCK_FILE):
        logging.error("STARTUP | lock exists, another instance is running")
        return False
    try:
        with open(LOCK_FILE, "w") as f:
            f.write(str(os.getpid()))
        return True
    except Exception as e:
        logging.error(f"STARTUP | failed to create lock | error={e}")
        return False
# ================== FEATURE FLAGS (1.3.x) ==================
# ================== FEATURE FLAGS (1.3.x) ==================
FEATURE_WELCOME_ENABLED = True
FEATURE_MUTE_ENABLED = True
FEATURE_AUTODELETE_ENABLED = True

# ================== FEATURE STATE (1.3.5) ==================
FEATURE_STATE = {
    "welcome": True,
    "mute": True,
    "autodelete": True,
}


# ================== FEATURE STORE ABSTRACTION (1.3.8) ==================
class FeatureStore:
    def load(self) -> dict:
        raise NotImplementedError

    def save(self, state: dict):
        raise NotImplementedError


class InMemoryFeatureStore(FeatureStore):
    def load(self) -> dict:
        return FEATURE_STATE.copy()

    def save(self, state: dict):
        FEATURE_STATE.update(state)


FEATURE_STORE = InMemoryFeatureStore()

def sync_feature_flags():
    global FEATURE_WELCOME_ENABLED, FEATURE_MUTE_ENABLED, FEATURE_AUTODELETE_ENABLED
    FEATURE_WELCOME_ENABLED = FEATURE_STATE["welcome"]
    FEATURE_MUTE_ENABLED = FEATURE_STATE["mute"]
    FEATURE_AUTODELETE_ENABLED = FEATURE_STATE["autodelete"]
    FEATURE_STORE.save(FEATURE_STATE)
# ===========================================================
# FEATURE:
# Welcome message supports optional image via WELCOME_IMAGE_URL
# FINAL RELEASE:
# Версия 1.2.15 является финальной.
# Ветка 1.2.x официально закрыта.
# Допускаются только критические security-fix при необходимости.

# ================== RELEASE STATUS ==================
# Version 1.3.9
# Branch 1.3.x frozen
# Only critical fixes allowed
# ================================================

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
        bot_mode=bot_mode,
        welcome_image_url=welcome_image_url,
    )
# ================================================


CFG = load_config()

# ===== Unified UX timing for admin/test UX messages =====
UX_TTL_SECONDS = 60

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

from typing import TypedDict, Set

# ================== USER REGISTRY (1.4.1) ==================
# user_id -> {"source": str, "labels": set[str], "first_seen": float, "chat_id": int}
class UserRegistryItem(TypedDict):
    source: str
    labels: Set[str]
    first_seen: float
    chat_id: int

USER_REGISTRY: dict[int, UserRegistryItem] = {}
# ===========================================================

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
            "Сообщество предназначено для общения по вопросам\n"
            "настройки, использования, тестирования и устранения проблем\n"
            "в программном обеспечении и операционных системах Apple и Microsoft.\n\n"
            "Спасибо за подписку. Оставайтесь с нами.\n\n"
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
        # Admin panel localization
        "admin_panel_title": "⚙️ <b>Панель управления Welcome Bot</b>",
        "admin_no_access": "⛔ Нет доступа",
        "admin_welcome_on": "✅ Welcome-сообщения включены",
        "admin_welcome_off": "⛔ Welcome-сообщения отключены",
        "admin_mute_on": "✅ Mute новых пользователей включён",
        "admin_mute_off": "⛔ Mute новых пользователей отключён",
        "admin_autodelete_on": "✅ Auto-delete включён",
        "admin_autodelete_off": "⛔ Auto-delete отключён",
        # v1.3.4 — about
        "about": (
            "ℹ️ <b>О сообществе Technology Universe</b>\n\n"
            "Здесь вы найдёте:\n\n"
            "• экспертную аналитику и технологические обзоры\n"
            "• тестирование решений и разборы ошибок\n"
            "• практические рекомендации и помощь в решении проблем\n"
            "• ответы на технические вопросы и индивидуальную поддержку\n\n"
            "Сообщество создано для обмена опытом, обсуждения обновлений\n"
            "и получения проверенной информации по технологиям\n"
            "и продуктам проекта."
        ),
        "btn_about": "ℹ️ О сообществе",
        # v1.3.6 — admin state/UX
        "state_on": "Включено ✅",
        "state_off": "Выключено ⛔",
        "ux_welcome_on": "Welcome-сообщения включены",
        "ux_welcome_off": "Welcome-сообщения отключены",
        "ux_mute_on": "Mute новых пользователей включён",
        "ux_mute_off": "Mute новых пользователей отключён",
        "ux_autodelete_on": "Auto-delete включён",
        "ux_autodelete_off": "Auto-delete отключён",
    },
    "en": {
        "welcome": (
            "👋 <b>Welcome to the private Telegram community of the Technology Universe project</b>\n\n"
            "This community is intended for discussions about\n"
            "setup, usage, testing, and troubleshooting\n"
            "software and operating systems by Apple and Microsoft.\n\n"
            "Thank you for joining. Stay with us.\n\n"
            "⬇️ <i>Choose an option below.</i>"
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
        # Admin panel localization
        "admin_panel_title": "⚙️ <b>Welcome Bot Control Panel</b>",
        "admin_no_access": "⛔ Access denied",
        "admin_welcome_on": "✅ Welcome messages enabled",
        "admin_welcome_off": "⛔ Welcome messages disabled",
        "admin_mute_on": "✅ New user mute enabled",
        "admin_mute_off": "⛔ New user mute disabled",
        "admin_autodelete_on": "✅ Auto-delete enabled",
        "admin_autodelete_off": "⛔ Auto-delete disabled",
        # v1.3.4 — about
        "about": (
            "ℹ️ <b>About Technology Universe</b>\n\n"
            "Here you will find:\n\n"
            "• expert analytics and technology reviews\n"
            "• solution testing and issue breakdowns\n"
            "• practical recommendations and troubleshooting assistance\n"
            "• answers to technical questions and individual support\n\n"
            "This community is created for experience sharing,\n"
            "update discussions, and access to verified information\n"
            "about technologies and project products."
        ),
        "btn_about": "ℹ️ About",
        # v1.3.6 — admin state/UX
        "state_on": "Enabled ✅",
        "state_off": "Disabled ⛔",
        "ux_welcome_on": "Welcome messages enabled",
        "ux_welcome_off": "Welcome messages disabled",
        "ux_mute_on": "New user mute enabled",
        "ux_mute_off": "New user mute disabled",
        "ux_autodelete_on": "Auto-delete enabled",
        "ux_autodelete_off": "Auto-delete disabled",
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


# Unified TTL helper for UX messages (v1.3.16)
def get_message_ttl(msg_type: str) -> int:
    if is_test_mode():
        return 60
    return 300


# Ограничение доступа к /control
def is_control_allowed(message: Message) -> bool:
    # /control разрешён всегда в личке
    if message.chat.type == "private":
        return True
    # В группах — только если чат разрешён
    return is_allowed_chat(cast(int, message.chat.id))


# Admin reply helper
async def admin_reply(message: Message, text: str):
    if not is_test_mode():
        return

    msg = await message.answer(text)

    async with BOT_MESSAGES_LOCK:
        BOT_MESSAGES[msg.message_id] = (time.time(), "admin")
        BOT_MESSAGES_CHAT_ID[cast(int, msg.message_id)] = cast(int, message.chat.id)


def admin_control_keyboard(lang: str) -> InlineKeyboardMarkup:
    def state(flag: bool) -> str:
        return t(lang, "state_on") if flag else t(lang, "state_off")

    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"Welcome: {state(FEATURE_WELCOME_ENABLED)}", callback_data="admin:welcome")],
        [InlineKeyboardButton(text=f"Mute: {state(FEATURE_MUTE_ENABLED)}", callback_data="admin:mute")],
        [InlineKeyboardButton(text=f"Auto-delete: {state(FEATURE_AUTODELETE_ENABLED)}", callback_data="admin:autodelete")],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="admin:refresh")]
    ])
@dp.message(F.text == "/control")
async def admin_control_panel(message: Message):
    if not is_control_allowed(message):
        return
    if not message.from_user:
        return
    if not is_admin(message.from_user.id):
        await message.answer(t(detect_lang(message.from_user.language_code), "admin_no_access"))
        return

    lang = detect_lang(message.from_user.language_code)
    await message.answer(
        t(lang, "admin_panel_title"),
        reply_markup=admin_control_keyboard(lang)
    )



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
            )
        ],
        [
            InlineKeyboardButton(
                text=t(lang, "btn_rules"),
                callback_data=f"rules:{lang}"
            ),
            InlineKeyboardButton(
                text=t(lang, "btn_about"),
                callback_data=f"about:{lang}"
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

# v1.3.4 — show_about callback
@dp.callback_query(F.data.startswith("about:"))
async def show_about(callback: CallbackQuery):
    if not callback.message or not callback.from_user:
        return

    try:
        await callback.answer()
    except Exception:
        return

    data = callback.data or ""
    parts = data.split(":", 1)
    lang = parts[1] if len(parts) == 2 else DEFAULT_LANG
    text = t(lang, "about")

    if is_test_mode():
        text = "🧪 <i>Test mode</i>\n\n" + text

    msg = await callback.message.answer(text)

    async with BOT_MESSAGES_LOCK:
        BOT_MESSAGES[msg.message_id] = (time.time(), "about")
        BOT_MESSAGES_CHAT_ID[cast(int, msg.message_id)] = cast(int, callback.message.chat.id)


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

# Helper: detect join source from message (1.4.3)
def detect_join_source_from_message(message: Message) -> str:
    source = "telegram"

    invite = getattr(message, "invite_link", None)
    if invite:
        name = (invite.name or "").lower()
        if "discord" in name:
            source = "discord"
        else:
            source = "invite_link"

    if getattr(message.chat, "join_by_request", False):
        source = "request"

    return source


@dp.message(F.new_chat_members)
async def welcome_new_user(message: Message):
    # Проверка разрешённого чата
    if not is_allowed_chat(cast(int, message.chat.id)):
        logging.info(
            f"SKIP chat | chat_id={message.chat.id} | not allowed"
        )
        return

    perms = await bot_has_permissions(cast(int, message.chat.id))

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

    # --- 1.4.1: detect join source
    source = detect_join_source_from_message(message)
    logging.info(
        f"JOIN_SOURCE | user={getattr(message.from_user, 'id', '?')} | source={source}"
    )
    if source.startswith("discord"):
        logging.info(
            f"DISCORD_USER | user={getattr(message.from_user, 'id', '?')} | chat={message.chat.id}"
        )

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

        # --- 1.4.2: user registry with chat_id
        if user.id not in USER_REGISTRY:
            labels: Set[str] = set()
            if source.startswith("discord"):
                labels.add("discord_member")
            if paid_like:
                labels.add("paid_member")

            USER_REGISTRY[user.id] = {
                "source": source,
                "labels": labels,
                "first_seen": now,
                "chat_id": cast(int, message.chat.id)
            }
            logging.info(
                f"USER_JOIN | user={user.id} | source={source}"
            )

        if (
            FEATURE_MUTE_ENABLED
            and CFG.mute_new_users
            and perms["restrict"]
            and not is_test_mode()
            and not paid_like
        ):
            try:
                await bot.restrict_chat_member(
                    chat_id=cast(int, message.chat.id),
                    user_id=cast(int, user.id),
                    permissions=ChatPermissions(
                        can_send_messages=False,
                        can_send_media_messages=False,
                        can_send_other_messages=False,
                        can_add_web_page_previews=False
                    ),
                    until_date=int(time.time()) + int(CFG.mute_seconds)
                )
                logging.info(
                    f"MUTED | user={user.id} | seconds={CFG.mute_seconds}"
                )
            except Exception as e:
                logging.warning(
                    f"MUTE FAILED | user={user.id} | error={e}"
                )

        if FEATURE_WELCOME_ENABLED:
            logging.info(
                f"WELCOME | user={user.id}"
            )

            lang = detect_lang(user.language_code)
            safe_name = user.full_name or "User"
            text = t(lang, "welcome").format(
                name=safe_name,
                project=CFG.project_name
            )
            if (not is_test_mode()) and source.startswith("discord"):
                text = (
                    text
                    + "\n\n<i>Вы получили доступ как участник Discord‑сообщества проекта.</i>"
                )

            if is_test_mode():
                text = (
                    "🧪 <i>Test mode</i>\n"
                    f"🧪 <i>Source: {source}</i>\n\n"
                    + text
                )

            if CFG.welcome_delay_seconds > 0:
                await asyncio.sleep(float(CFG.welcome_delay_seconds))

            if CFG.welcome_image_url:
                msg = await bot.send_photo(
                    chat_id=cast(int, message.chat.id),
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
                BOT_MESSAGES_CHAT_ID[cast(int, msg.message_id)] = cast(int, message.chat.id)

            if (
                FEATURE_AUTODELETE_ENABLED
                and CFG.auto_delete_seconds > 0
                and not is_test_mode()
                and not paid_like
            ):
                await asyncio.sleep(float(CFG.auto_delete_seconds))
                await msg.delete()


# --- v1.3.9.18: Welcome for invite link & paid join approval ---
@dp.chat_member()
async def welcome_on_approved_join(event: ChatMemberUpdated):
    """
    Welcome users who joined via:
    - invite link
    - paid / join request approval
    """
    # --- 1.4.1: detect join source from event
    source = detect_join_source_from_member_event(event)

    if event.old_chat_member.status in {"left", "kicked"} and event.new_chat_member.status == "member":
        user = event.new_chat_member.user
        chat = event.chat

        # Проверка разрешённого чата
        if not is_allowed_chat(cast(int, chat.id)):
            logging.info(f"SKIP approved join | chat_id={chat.id} | not allowed")
            return

        # Антидубль (используем тот же cache)
        now = time.time()
        last_time = WELCOME_CACHE.get(user.id)
        if last_time and (now - last_time) < WELCOME_TTL_SECONDS:
            logging.info(f"SKIP approved welcome | user={user.id} | duplicate")
            return

        WELCOME_CACHE[user.id] = now
        if len(WELCOME_CACHE) > WELCOME_CACHE_MAX:
            WELCOME_CACHE.clear()
            logging.warning("CACHE | WELCOME_CACHE cleared (limit exceeded)")

        # --- 1.4.2: user registry with chat_id
        if user.id not in USER_REGISTRY:
            labels: Set[str] = set()
            if source.startswith("discord"):
                labels.add("discord_member")
            if source == "paid":
                labels.add("paid_member")

            USER_REGISTRY[user.id] = {
                "source": source,
                "labels": labels,
                "first_seen": now,
                "chat_id": cast(int, chat.id)
            }
            logging.info(
                f"USER_JOIN | user={user.id} | source={source}"
            )
            if source.startswith("discord"):
                logging.info(
                    f"DISCORD_USER | user={user.id} | chat={chat.id}"
                )

        if not FEATURE_WELCOME_ENABLED:
            return

        lang = detect_lang(user.language_code)
        text = t(lang, "welcome").format(
            name=user.full_name or "User",
            project=CFG.project_name
        )
        if (not is_test_mode()) and source.startswith("discord"):
            text = (
                text
                + "\n\n<i>Вы получили доступ как участник Discord‑сообщества проекта.</i>"
            )
        if is_test_mode():
            text = (
                "🧪 <i>Test mode</i>\n"
                f"🧪 <i>Source: {source}</i>\n\n"
                + text
            )

        try:
            msg = await bot.send_message(
                chat_id=cast(int, chat.id),
                text=text,
                reply_markup=welcome_keyboard(lang)
            )

            async with BOT_MESSAGES_LOCK:
                BOT_MESSAGES[msg.message_id] = (time.time(), "welcome")
                BOT_MESSAGES_CHAT_ID[cast(int, msg.message_id)] = cast(int, chat.id)

        except Exception as e:
            logging.warning(f"WELCOME APPROVED FAILED | user={user.id} | error={e}")

# --- 1.4.3: Helper for join source from member event ---
def detect_join_source_from_member_event(event: ChatMemberUpdated) -> str:
    if (
        event.old_chat_member.status in {"left", "kicked"}
        and event.new_chat_member.status == "member"
    ):
        if getattr(event.chat, "join_by_request", False):
            return "discord_request"
        return "paid"
    return "telegram"



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

    data = callback.data or ""
    parts = data.split(":", 1)
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
        BOT_MESSAGES_CHAT_ID[cast(int, msg.message_id)] = cast(int, callback.message.chat.id)


@dp.callback_query(F.data.startswith("admin:"))
async def admin_control_callback(callback: CallbackQuery):
    if not callback.from_user or not is_admin(callback.from_user.id):
        try:
            lang = detect_lang(callback.from_user.language_code)
            await callback.answer(t(lang, "admin_no_access"), show_alert=True)
        except Exception:
            pass
        return

    lang = detect_lang(callback.from_user.language_code)
    global FEATURE_WELCOME_ENABLED, FEATURE_MUTE_ENABLED, FEATURE_AUTODELETE_ENABLED

    data = callback.data or ""
    parts = data.split(":", 1)
    if len(parts) != 2:
        return
    action = parts[1]

    if action == "welcome":
        FEATURE_STATE["welcome"] = not FEATURE_STATE["welcome"]
        sync_feature_flags()
        if is_test_mode():
            await callback.answer(t(lang, "ux_welcome_on") if FEATURE_STATE["welcome"] else t(lang, "ux_welcome_off"))
        else:
            await callback.answer()
    elif action == "mute":
        FEATURE_STATE["mute"] = not FEATURE_STATE["mute"]
        sync_feature_flags()
        if is_test_mode():
            await callback.answer(t(lang, "ux_mute_on") if FEATURE_STATE["mute"] else t(lang, "ux_mute_off"))
        else:
            await callback.answer()
    elif action == "autodelete":
        FEATURE_STATE["autodelete"] = not FEATURE_STATE["autodelete"]
        sync_feature_flags()
        if is_test_mode():
            await callback.answer(t(lang, "ux_autodelete_on") if FEATURE_STATE["autodelete"] else t(lang, "ux_autodelete_off"))
        else:
            await callback.answer()
    elif action == "refresh":
        # No UX response for refresh
        await callback.answer()  # Always safe to call, no text

    # Fix for Pylance: only call edit_reply_markup if Message, not InaccessibleMessage
    if callback.message and isinstance(callback.message, Message):
        try:
            await callback.message.edit_reply_markup(
                reply_markup=admin_control_keyboard(lang)
            )
        except Exception as e:
            # Ignore Telegram error when markup is not changed
            if "message is not modified" not in str(e):
                raise



@dp.message(F.text == "/version")
async def version_cmd(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return
    await message.answer(
        "ℹ️ <b>Welcome Bot</b>\n"
        f"Version: {VERSION}\n"
        "Channel: Stable (1.3.x)"
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


# ===== Admin Control Commands =====

@dp.message(F.text.startswith("/welcome "))
async def welcome_toggle(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return

    text = message.text or ""
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        await admin_reply(message, "ℹ️ Использование: /welcome on|off")
        return

    arg = parts[1].lower()
    lang = detect_lang(message.from_user.language_code)

    if arg == "on":
        FEATURE_STATE["welcome"] = True
        sync_feature_flags()
        await admin_reply(message, t(lang, "admin_welcome_on"))
    elif arg == "off":
        FEATURE_STATE["welcome"] = False
        sync_feature_flags()
        await admin_reply(message, t(lang, "admin_welcome_off"))
    else:
        await admin_reply(message, "ℹ️ Использование: /welcome on|off")


@dp.message(F.text.startswith("/mute "))
async def mute_toggle(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return

    text = message.text or ""
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        await admin_reply(message, "ℹ️ Использование: /mute on|off")
        return

    arg = parts[1].lower()
    lang = detect_lang(message.from_user.language_code)

    if arg == "on":
        FEATURE_STATE["mute"] = True
        sync_feature_flags()
        await admin_reply(message, t(lang, "admin_mute_on"))
    elif arg == "off":
        FEATURE_STATE["mute"] = False
        sync_feature_flags()
        await admin_reply(message, t(lang, "admin_mute_off"))
    else:
        await admin_reply(message, "ℹ️ Использование: /mute on|off")



@dp.message(F.text.startswith("/autodelete "))
async def autodelete_toggle(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return

    text = message.text or ""
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        await admin_reply(message, "ℹ️ Использование: /autodelete on|off")
        return

    arg = parts[1].lower()
    lang = detect_lang(message.from_user.language_code)

    if arg == "on":
        FEATURE_STATE["autodelete"] = True
        sync_feature_flags()
        await admin_reply(message, t(lang, "admin_autodelete_on"))
    elif arg == "off":
        FEATURE_STATE["autodelete"] = False
        sync_feature_flags()
        await admin_reply(message, t(lang, "admin_autodelete_off"))
    else:
        await admin_reply(message, "ℹ️ Использование: /autodelete on|off")

# ===== /whois admin command =====
@dp.message(F.text.startswith("/whois "))
async def whois_cmd(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return
    text = message.text or ""
    parts = text.strip().split(maxsplit=1)
    if len(parts) < 2:
        await admin_reply(message, "ℹ️ Usage: /whois <user_id>")
        return
    user_id_arg = parts[1].strip()
    try:
        user_id = int(user_id_arg)
    except ValueError:
        await admin_reply(message, "ℹ️ Usage: /whois <user_id>")
        return
    user_info = USER_REGISTRY.get(user_id)
    if not user_info:
        await admin_reply(message, "ℹ️ User not found in registry")
        return
    source = user_info.get("source", "—")
    labels_set = user_info.get("labels", set())
    if isinstance(labels_set, set):
        labels = ", ".join(sorted(labels_set)) if labels_set else "—"
    else:
        labels = str(labels_set) if labels_set else "—"
    first_seen = user_info["first_seen"]
    chat_id = user_info["chat_id"]
    reply_text = (
        f"<b>User info</b>\n"
        f"• user_id: <code>{user_id}</code>\n"
        f"• source: <code>{source}</code>\n"
        f"• labels: <code>{labels}</code>\n"
        f"• first_seen: <code>{int(first_seen)}</code>\n"
        f"• chat_id: <code>{chat_id}</code>"
    )
    await admin_reply(message, reply_text)

# ===== /export_registry admin command =====
@dp.message(F.text == "/export_registry")
async def export_registry_cmd(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return

    if not USER_REGISTRY:
        await admin_reply(message, "ℹ️ Registry is empty")
        return

    lines = []
    for uid, info in USER_REGISTRY.items():
        source = info.get("source", "—")
        labels = info.get("labels", set())
        if isinstance(labels, set):
            labels_str = ",".join(sorted(labels)) if labels else "—"
        else:
            labels_str = str(labels)
        first_seen = int(info["first_seen"])
        chat_id = info["chat_id"]
        lines.append(
            f"{uid} | {source} | {labels_str} | {first_seen} | {chat_id}"
        )

    text = "<b>User Registry Export</b>\n\n" + "\n".join(lines)
    await admin_reply(message, text)
@dp.message(F.text.startswith("/"))
async def unknown_command(message: Message):
    if not message.from_user:
        return

    if is_admin(message.from_user.id):
        await message.answer(
            "ℹ️ Неизвестная команда\n"
            "Используйте /health, /version или /control"
        )


async def cleanup_bot_messages():
    while not shutdown_event.is_set():
        now = time.time()
        to_delete: list[int] = []

        async with BOT_MESSAGES_LOCK:
            for msg_id, (ts, msg_type) in BOT_MESSAGES.items():
                ttl = get_message_ttl(msg_type)
                if (now - ts) > ttl:
                    to_delete.append(msg_id)

        for msg_id in to_delete:
            chat_id = BOT_MESSAGES_CHAT_ID.get(msg_id)
            if not chat_id:
                continue
            try:
                await bot.delete_message(
                    chat_id=cast(int, chat_id),
                    message_id=cast(int, msg_id)
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
            expired = [uid for uid, ts in WELCOME_CACHE.items() if (now - ts) > WELCOME_TTL_SECONDS]
            for uid in expired:
                WELCOME_CACHE.pop(uid, None)

            # rules cache
            expired = [uid for uid, ts in RULES_CACHE.items() if (now - ts) > RULES_TTL_SECONDS]
            for uid in expired:
                RULES_CACHE.pop(uid, None)
        except Exception as e:
            logging.warning(f"CACHE | cleanup failed | error={e}")

        await asyncio.sleep(300)  # каждые 5 минут

shutdown_event = asyncio.Event()


def _handle_shutdown():
    logging.info("SHUTDOWN | signal received")
    shutdown_event.set()

async def main():
    if not acquire_startup_lock():
        return
    logging.info(
        f"STARTUP | version={VERSION} "
        f"mute={CFG.mute_new_users} "
        f"delay={CFG.welcome_delay_seconds}s "
        f"autodelete={CFG.auto_delete_seconds}s"
    )
    logging.info(f"BUILD | version={VERSION} channel=stable-1.3.x")
    if not CFG.admin_ids:
        logging.warning("ENV | ADMIN_IDS is empty")

    if not CFG.allowed_chat_ids:
        logging.warning("ENV | ALLOWED_CHAT_IDS is empty (bot allowed in all chats)")
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_shutdown)
        except NotImplementedError:
            pass
    logging.info("RUNTIME | async lifecycle guards enabled")
    tasks = []

    # Cleanup tasks enabled in all modes (safe for test-mode)
    tasks.append(asyncio.create_task(cleanup_bot_messages()))
    tasks.append(asyncio.create_task(cleanup_caches()))

    await asyncio.sleep(1)  # anti-flood startup delay
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

    try:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
    except Exception:
        pass

    logging.info("SHUTDOWN | all tasks stopped cleanly")


if __name__ == "__main__":
    asyncio.run(main())