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
import html

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","message":"%(message)s"}'
)


# ================== VERSION ==================
# v1.5.9.810 — Discord Join Fix (invite_link detection + approved join sync)
VERSION = "1.5.9.810"
# v1.5.2 — Source → Badge (UX)
# Branch 1.5.x started
# Goal: user context, source attribution, badges, persistence preparation

# ================== 1.4.5 UX & FLOOD SAFETY ==================
# Simple single-instance lock to avoid parallel polling
LOCK_FILE = "/tmp/welcome_bot.lock"
# =============================================================
import errno

def acquire_startup_lock() -> bool:
    if os.path.exists(LOCK_FILE):
        try:
            with open(LOCK_FILE, "r") as f:
                pid = int(f.read().strip())

            # Проверяем, жив ли процесс
            os.kill(pid, 0)
            logging.error(
                f"STARTUP | lock exists, process alive | pid={pid}"
            )
            return False

        except ValueError:
            logging.warning("STARTUP | invalid lock file, recreating")
        except ProcessLookupError:
            logging.warning("STARTUP | stale lock detected, recreating")
        except PermissionError:
            logging.error("STARTUP | no permission to check PID")
            return False
        except OSError as e:
            if e.errno != errno.ESRCH:
                logging.error(f"STARTUP | lock check failed | error={e}")
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

bot = Bot(
    token=CFG.bot_token,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()

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

# ================== USER REGISTRY (1.5.0 foundation) ==================
REGISTRY_READ_ONLY = True  # v1.5.4 — protect existing records from modification
# user_id -> {"source": str, "labels": set[str], "first_seen": float, "chat_id": int}
class UserRegistryItem(TypedDict):
    source: str
    labels: Set[str]
    first_seen: float
    chat_id: int

class JoinSource:
    TELEGRAM = "telegram"
    INVITE_LINK = "invite_link"
    DISCORD = "discord"
    PAID = "paid"
    REQUEST = "request"

# UX Badges for join source (v1.5.2)
SOURCE_BADGES = {
    JoinSource.DISCORD: "🟣 Discord member",
    JoinSource.PAID: "💳 Paid access",
    JoinSource.INVITE_LINK: "🔗 Invite link",
    JoinSource.REQUEST: "📝 Join request",
}

# ================== USER REGISTRY STORAGE (1.5.3) ==================
USER_REGISTRY: dict[int, UserRegistryItem] = {}
USER_REGISTRY_FILE = "user_registry.json"

# --- Registry schema versioning ---
REGISTRY_SCHEMA_VERSION = 1
REGISTRY_META_KEY = "_schema_version"

# --- v1.5.5: audit log for registry mutations ---
def log_registry_mutation(admin_id: int, user_id: int, action: str, details: str):
    logging.info(
        f"REGISTRY_MUTATION | admin={admin_id} | user={user_id} | action={action} | {details}"
    )

def save_user_registry():
    try:
        import json
        data = {
            REGISTRY_META_KEY: REGISTRY_SCHEMA_VERSION,
            "users": {
                str(uid): {
                    "source": info["source"],
                    "labels": list(info["labels"]),
                    "first_seen": info["first_seen"],
                    "chat_id": info["chat_id"],
                }
                for uid, info in USER_REGISTRY.items()
            }
        }
        with open(USER_REGISTRY_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.error(f"REGISTRY | save failed | error={e}")

def load_user_registry():
    if not os.path.exists(USER_REGISTRY_FILE):
        return
    try:
        import json
        with open(USER_REGISTRY_FILE, "r", encoding="utf-8") as f:
            raw = json.load(f)

        schema_version = raw.get(REGISTRY_META_KEY, 0)
        if schema_version != REGISTRY_SCHEMA_VERSION:
            logging.warning(
                f"REGISTRY | schema mismatch detected | file=v{schema_version} code=v{REGISTRY_SCHEMA_VERSION}"
            )
            # safe auto-upgrade: only update meta version without altering user data
            raw[REGISTRY_META_KEY] = REGISTRY_SCHEMA_VERSION
            try:
                with open(USER_REGISTRY_FILE, "w", encoding="utf-8") as wf:
                    json.dump(raw, wf, ensure_ascii=False, indent=2)
                logging.info("REGISTRY | schema version auto-upgraded safely")
            except Exception as e:
                logging.error(f"REGISTRY | auto-upgrade failed | error={e}")
        users = raw.get("users", {})

        for uid, data in users.items():
            USER_REGISTRY[int(uid)] = {
                "source": data.get("source", JoinSource.TELEGRAM),
                "labels": set(data.get("labels", [])),
                "first_seen": float(data.get("first_seen", time.time())),
                "chat_id": int(data.get("chat_id", 0)),
            }

        logging.info(
            f"REGISTRY | loaded {len(USER_REGISTRY)} users | schema=v{schema_version}"
        )
    except Exception as e:
        logging.error(f"REGISTRY | load failed | error={e}")

# --- Registry schema validator ---
def validate_registry_schema() -> tuple[bool, str]:
    if not os.path.exists(USER_REGISTRY_FILE):
        return True, "Registry file not found"

    try:
        import json
        with open(USER_REGISTRY_FILE, "r", encoding="utf-8") as f:
            raw = json.load(f)

        if REGISTRY_META_KEY not in raw:
            return False, "Missing schema version"

        if raw[REGISTRY_META_KEY] != REGISTRY_SCHEMA_VERSION:
            return False, f"Schema mismatch: {raw[REGISTRY_META_KEY]} != {REGISTRY_SCHEMA_VERSION}"

        if "users" not in raw or not isinstance(raw["users"], dict):
            return False, "Invalid users section"

        return True, "Schema valid"
    except Exception as e:
        return False, f"Validation error: {e}"

# --- Dry-run migration stub ---
def dry_run_migration(target_version: int) -> str:
    affected = len(USER_REGISTRY)
    return (
        f"🧪 Dry-run migration\n\n"
        f"From: v{REGISTRY_SCHEMA_VERSION}\n"
        f"To: v{target_version}\n"
        f"Users affected: {affected}\n\n"
        f"❗ No data was modified"
    )

# --- Apply migration stub (blocked) ---
def apply_migration(target_version: int) -> str:
    return (
        f"⛔ Migration blocked\n\n"
        f"Target version: v{target_version}\n"
        f"Reason: apply_migration is disabled in v1.5.9\n"
        f"Use --dry-run only"
    )

# --- v1.5.9: controlled migration apply (preview, blocked by flag) ---
MIGRATION_APPLY_ENABLED = False  # hard safety switch

def apply_migration_controlled(target_version: int, admin_id: int) -> str:
    if REGISTRY_READ_ONLY:
        return (
            "⛔ Migration blocked\n\n"
            "Reason: REGISTRY_READ_ONLY = True"
        )
    if not MIGRATION_APPLY_ENABLED:
        return (
            "⛔ Migration apply is disabled\n\n"
            f"Target version: v{target_version}\n"
            "Reason: MIGRATION_APPLY_ENABLED = False"
        )
    log_registry_mutation(
        admin_id,
        0,
        "apply_migration",
        f"to=v{target_version}"
    )
    return (
        "⚠️ Migration apply stub\n\n"
        f"Target version: v{target_version}\n"
        "No changes were made"
    )
# ===== Registry admin commands =====

# --- Registry schema check command ---
@dp.message(F.text == "/registry_schema")
async def registry_schema_cmd(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        await admin_reply(message, "🔒 Admin command available only in private chat")
        return

    ok, info = validate_registry_schema()
    status = "✅ OK" if ok else "⚠️ INVALID"

    await admin_reply(
        message,
        f"<b>Registry schema</b>\n"
        f"Version: v{REGISTRY_SCHEMA_VERSION}\n"
        f"Status: {status}\n"
        f"Info: {info}"
    )

# --- v1.5.8: Registry migration plan preview command ---
@dp.message(F.text == "/registry_plan")
async def registry_plan_cmd(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        await admin_reply(message, "🔒 Admin command available only in private chat")
        return

    plan = (
        "🧭 <b>Registry migration plan</b>\n\n"
        "1️⃣ Backup user_registry.json\n"
        "2️⃣ Run /registry_schema\n"
        "3️⃣ Run /registry_migrate <version> --dry-run\n"
        "4️⃣ Set REGISTRY_READ_ONLY = False\n"
        "5️⃣ Enable MIGRATION_APPLY_ENABLED\n"
        "6️⃣ Run /registry_apply <version> (private chat)\n"
        "7️⃣ Verify integrity\n"
        "8️⃣ Disable MIGRATION_APPLY_ENABLED\n"
        "9️⃣ Set REGISTRY_READ_ONLY = True\n\n"
        "⚠️ No data is modified by this command"
    )

    await admin_reply(message, plan)

# --- Registry migration dry-run command ---
@dp.message(F.text.startswith("/registry_migrate "))
async def registry_migrate_cmd(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        await admin_reply(message, "🔒 Admin command available only in private chat")
        return

    text = message.text or ""
    parts = text.split()
    if len(parts) < 2:
        await admin_reply(message, "ℹ️ Usage: /registry_migrate <version> [--dry-run]")
        return

    try:
        target_version = int(parts[1])
    except ValueError:
        await admin_reply(message, "❌ Invalid target version")
        return

    is_dry = "--dry-run" in parts

    if not is_dry:
        text = apply_migration(target_version)
        await admin_reply(message, text)
        return

    text = dry_run_migration(target_version)
    log_registry_mutation(
        message.from_user.id,
        0,
        "dry_run_migration",
        f"to=v{target_version}"
    )
    await admin_reply(message, text)

# --- v1.5.9: Registry controlled apply command (preview, blocked by flag) ---
@dp.message(F.text.startswith("/registry_apply "))
async def registry_apply_cmd(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        return

    text = message.text or ""
    parts = text.split()
    if len(parts) < 2:
        await admin_reply(message, "ℹ️ Usage: /registry_apply <version>")
        return

    try:
        target_version = int(parts[1])
    except ValueError:
        await admin_reply(message, "❌ Invalid target version")
        return

    text = apply_migration_controlled(target_version, message.from_user.id)
    await admin_reply(message, text)
# ===========================================================
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
            "👋 <b>{name}, добро пожаловать в закрытое Telegram‑сообщество проекта {project}</b>\n\n"
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
            "👋 <b>{name}, welcome to the private Telegram community of the {project} project</b>\n\n"
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


def get_message_ttl(msg_type: str) -> int:
    """
    v1.5.9.x — Unified auto-delete policy

    • In TEST mode → short TTL (60s)
    • In PROD mode → ALL bot messages use CFG.auto_delete_seconds
    """

    # Test mode → fixed short TTL
    if is_test_mode():
        return 60

    # Prod mode → everything deletes using configured AUTO_DELETE_SECONDS
    return CFG.auto_delete_seconds


# Ограничение доступа к /control
def is_control_allowed(message: Message) -> bool:
    # /control разрешён всегда в личке
    if message.chat.type == "private":
        return True
    # В группах — только если чат разрешён
    return is_allowed_chat(cast(int, message.chat.id))


# Admin reply helper
async def admin_reply(message: Message, text: str):
    # v1.5.9.210 — Ephemeral admin replies

    try:
        msg = await message.answer(text)
    except Exception:
        return

    # В группе — автоудаление через 10 секунд
    if message.chat.type != "private":
        try:
            await asyncio.sleep(10)
            await msg.delete()
            await message.delete()
        except Exception:
            pass
        return

    # В личке — стандартная TTL-логика
    async with BOT_MESSAGES_LOCK:
        BOT_MESSAGES[msg.message_id] = (time.time(), "admin")
        BOT_MESSAGES_CHAT_ID[msg.message_id] = message.chat.id


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
    if not message.from_user:
        return
    if not is_admin(message.from_user.id):
        return
    if message.chat.type != "private":
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


def is_paid_like_chat(chat) -> bool:
    """
    UX-эвристика для платных / закрытых чатов.
    Принимает chat-объект (Message.chat или ChatMemberUpdated.chat).
    Не является платёжной логикой.
    """
    return bool(
        getattr(chat, "has_protected_content", False)
        or getattr(chat, "join_by_request", False)
        or getattr(chat, "join_to_send_messages", False)
    )

# Helper: detect join source from message (1.5.1)
def detect_join_source_from_message(message: Message) -> str:
    if getattr(message.chat, "join_by_request", False):
        return JoinSource.REQUEST

    invite = getattr(message, "invite_link", None)
    if invite:
        name = (invite.name or "").lower()
        if "discord" in name:
            return JoinSource.DISCORD
        return JoinSource.INVITE_LINK

    return JoinSource.TELEGRAM


@dp.message(F.new_chat_members)
async def welcome_new_user(message: Message):
    # Проверка разрешённого чата
    if not is_allowed_chat(cast(int, message.chat.id)):
        logging.info(
            f"SKIP chat | chat_id={message.chat.id} | not allowed"
        )
        return

    perms = await bot_has_permissions(cast(int, message.chat.id))

    paid_like = is_paid_like_chat(message.chat)

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

    for user in message.new_chat_members:
        if user.is_bot:
            continue
        logging.info(
            f"JOIN_SOURCE | user={user.id} | source={source}"
        )

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
            if source == JoinSource.DISCORD:
                labels.add("discord_member")
            if source == JoinSource.PAID:
                labels.add("paid_member")

            USER_REGISTRY[user.id] = {
                "source": source,
                "labels": labels,
                "first_seen": now,
                "chat_id": cast(int, message.chat.id)
            }
            save_user_registry()
            logging.info(
                f"USER_JOIN | user={user.id} | source={source}"
            )
        else:
            record = USER_REGISTRY[user.id]
            if source != record.get("source"):
                logging.info(
                    f"REGISTRY | source updated | user={user.id} | {record.get('source')} → {source}"
                )
                if not REGISTRY_READ_ONLY:
                    record["source"] = source
                    if source == JoinSource.DISCORD:
                        record["labels"].add("discord_member")
                    if source == JoinSource.PAID:
                        record["labels"].add("paid_member")
                    save_user_registry()
            else:
                logging.info(f"REGISTRY | read-only skip | user={user.id}")

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
                f"WELCOME_SENT | user={user.id} | source={source} | chat={message.chat.id}"
            )

            lang = detect_lang(user.language_code)
            raw_name = user.full_name or "User"
            safe_name = html.escape(raw_name)
            text = t(lang, "welcome").format(
                name=safe_name,
                project=CFG.project_name
            )
            # v1.5.2: Add badge after welcome text
            badge = SOURCE_BADGES.get(source)
            if badge and not is_test_mode():
                text = text + f"\n\n<b>{badge}</b>"
            if (not is_test_mode()) and source == JoinSource.DISCORD:
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
                if FEATURE_AUTODELETE_ENABLED and not paid_like:
                    BOT_MESSAGES[msg.message_id] = (time.time(), "welcome")
                    BOT_MESSAGES_CHAT_ID[msg.message_id] = cast(int, message.chat.id)


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
            if source == JoinSource.DISCORD:
                labels.add("discord_member")
            if source == JoinSource.PAID:
                labels.add("paid_member")

            USER_REGISTRY[user.id] = {
                "source": source,
                "labels": labels,
                "first_seen": now,
                "chat_id": cast(int, chat.id)
            }
            save_user_registry()
            logging.info(
                f"USER_JOIN | user={user.id} | source={source}"
            )
            if source == JoinSource.DISCORD:
                logging.info(
                    f"DISCORD_USER | user={user.id} | chat={chat.id}"
                )
        else:
            logging.info(f"REGISTRY | read-only skip | user={user.id}")

        # --- sync mute logic for approved joins ---
        perms = await bot_has_permissions(cast(int, chat.id))
        # paid-like detection must use chat context, not ChatMember object
        paid_like = is_paid_like_chat(chat)

        if (
            FEATURE_MUTE_ENABLED
            and CFG.mute_new_users
            and perms["restrict"]
            and not is_test_mode()
            and not paid_like
        ):
            try:
                await bot.restrict_chat_member(
                    chat_id=cast(int, chat.id),
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

        if not FEATURE_WELCOME_ENABLED:
            return

        lang = detect_lang(user.language_code)
        raw_name = user.full_name or "User"
        safe_name = html.escape(raw_name)
        text = t(lang, "welcome").format(
            name=safe_name,
            project=CFG.project_name
        )
        # v1.5.2: Add badge after welcome text
        badge = SOURCE_BADGES.get(source)
        if badge and not is_test_mode():
            text = text + f"\n\n<b>{badge}</b>"
        if (not is_test_mode()) and source == JoinSource.DISCORD:
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

            logging.info(
                f"WELCOME_SENT | user={user.id} | source={source} | chat={chat.id}"
            )

            async with BOT_MESSAGES_LOCK:
                if FEATURE_AUTODELETE_ENABLED:
                    BOT_MESSAGES[msg.message_id] = (time.time(), "welcome")
                    BOT_MESSAGES_CHAT_ID[msg.message_id] = cast(int, chat.id)

        except Exception as e:
            logging.warning(f"WELCOME APPROVED FAILED | user={user.id} | error={e}")

# --- 1.5.1: Helper for join source from member event ---
def detect_join_source_from_member_event(event: ChatMemberUpdated) -> str:
    # Trigger only on real join transition
    if (
        event.old_chat_member.status in {"left", "kicked"}
        and event.new_chat_member.status == "member"
    ):
        # 1️⃣ Join by request (paid / approval flow)
        if getattr(event.chat, "join_by_request", False):
            return JoinSource.PAID

        # 2️⃣ Invite link detection (like in message handler)
        invite = getattr(event, "invite_link", None)
        if invite:
            name = (invite.name or "").lower()
            if "discord" in name:
                return JoinSource.DISCORD
            return JoinSource.INVITE_LINK

        # 3️⃣ Fallback
        return JoinSource.TELEGRAM

    return JoinSource.TELEGRAM



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
    if message.chat.type != "private":
        return

    await message.answer(
        "ℹ️ <b>Welcome Bot</b>\n"
        f"Version: {VERSION}\n"
        "Channel: Stable (1.5.x)"
    )

@dp.message(F.text == "/health")
async def health_check(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return
    if message.chat.type != "private":
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
    if message.chat.type != "private":
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
    if message.chat.type != "private":
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
    if message.chat.type != "private":
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

@dp.message(F.text.startswith("/registry_set "))
async def registry_set_cmd(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        await admin_reply(message, "🔒 Admin command available only in private chat")
        return

    if REGISTRY_READ_ONLY:
        await admin_reply(
            message,
            "⛔ Registry is read-only. Mutations are disabled."
        )
        return

    parts = (message.text or "").split()
    if len(parts) < 4:
        await admin_reply(
            message,
            "ℹ️ Usage:\n"
            "/registry_set <user_id> source <value>\n"
            "/registry_set <user_id> add_label <label>\n"
            "/registry_set <user_id> remove_label <label>"
        )
        return

    try:
        target_user = int(parts[1])
    except ValueError:
        await admin_reply(message, "❌ Invalid user_id")
        return

    action = parts[2]
    value = parts[3]

    if target_user not in USER_REGISTRY:
        await admin_reply(message, "❌ User not found in registry")
        return

    record = USER_REGISTRY[target_user]

    if action == "source":
        old = record["source"]
        record["source"] = value
        save_user_registry()
        log_registry_mutation(
            message.from_user.id,
            target_user,
            "set_source",
            f"{old} → {value}"
        )
        await admin_reply(message, f"✅ source updated: {old} → {value}")
        return

    if action == "add_label":
        labels = record["labels"]
        if value in labels:
            await admin_reply(message, "ℹ️ Label already exists")
            return
        labels.add(value)
        save_user_registry()
        log_registry_mutation(
            message.from_user.id,
            target_user,
            "add_label",
            value
        )
        await admin_reply(message, f"✅ label added: {value}")
        return

    if action == "remove_label":
        labels = record["labels"]
        if value not in labels:
            await admin_reply(message, "ℹ️ Label not present")
            return
        labels.remove(value)
        save_user_registry()
        log_registry_mutation(
            message.from_user.id,
            target_user,
            "remove_label",
            value
        )
        await admin_reply(message, f"✅ label removed: {value}")
        return

    await admin_reply(message, "❌ Unknown action")

# ===== /whois admin command =====
@dp.message(F.text.startswith("/whois "))
async def whois_cmd(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        await admin_reply(message, "🔒 Admin command available only in private chat")
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
# ===== /export_registry admin command =====
@dp.message(F.text == "/export_registry")
async def export_registry_cmd(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        await admin_reply(message, "🔒 Admin command available only in private chat")
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

# ===== /registry_backup admin command =====
@dp.message(F.text == "/registry_backup")
async def registry_backup_cmd(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        await admin_reply(message, "🔒 Admin command available only in private chat")
        return

    if not os.path.exists(USER_REGISTRY_FILE):
        await admin_reply(message, "ℹ️ Registry file not found")
        return

    backup_name = f"user_registry_backup_{int(time.time())}.json"
    try:
        import shutil
        shutil.copy(USER_REGISTRY_FILE, backup_name)
        await admin_reply(message, f"✅ Backup created:\n<code>{backup_name}</code>")
    except Exception as e:
        await admin_reply(message, f"❌ Backup failed: {e}")

# ===== /registry_stats admin command =====
@dp.message(F.text == "/registry_stats")
async def registry_stats_cmd(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        await admin_reply(message, "🔒 Admin command available only in private chat")
        return

    total = len(USER_REGISTRY)
    sources = {}
    for info in USER_REGISTRY.values():
        src = info.get("source", "unknown")
        sources[src] = sources.get(src, 0) + 1

    lines = [f"👥 Total users: {total}\n"]
    for src, count in sources.items():
        lines.append(f"• {src}: {count}")

    await admin_reply(message, "<b>Registry stats</b>\n\n" + "\n".join(lines))

# ===== Admin helper: get photo file_id (test-mode only) =====
@dp.message(F.photo)
async def get_photo_file_id(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return
    if not is_test_mode():
        return

    photos = message.photo
    if not photos:
        return

    photo = photos[-1]  # highest resolution
    file_id = photo.file_id

    await message.answer(
        "🆔 <b>Telegram file_id</b>\n\n"
        f"<code>{file_id}</code>\n\n"
        "ℹ️ Используйте этот file_id в WELCOME_IMAGE_URL"
    )

# ===== v1.5.9.500 — Keyword trigger: "Хранилище" =====

# --- Storage trigger anti-spam cache (v1.5.9.510) ---
STORAGE_TRIGGER_CACHE: dict[int, float] = {}  # chat_id -> last trigger timestamp

# TTL зависит от режима (test/prod)
def get_storage_trigger_ttl() -> int:
    return 60 if is_test_mode() else 300  # 1 минута в test, 5 минут в prod
@dp.message(F.text)
async def storage_keyword_trigger(message: Message):
    if not message.text:
        return
    chat_id = cast(int, message.chat.id)
    now = time.time()

    import re

    text_lower = message.text.lower()

    # React only if the word "хранилище" (any ending) exists
    if not re.search(r"\bхранилищ\w*\b", text_lower):
        return

    ttl = get_storage_trigger_ttl()
    last = STORAGE_TRIGGER_CACHE.get(chat_id)
    if last and (now - last) < ttl:
        return

    STORAGE_TRIGGER_CACHE[chat_id] = now

    # Register user message for unified TTL deletion
    async with BOT_MESSAGES_LOCK:
        BOT_MESSAGES[message.message_id] = (time.time(), "storage_user")
        BOT_MESSAGES_CHAT_ID[message.message_id] = chat_id
    logging.info(f"STORAGE_TRIGGER | chat={chat_id} | ttl={ttl}")

    # Ignore commands
    if message.text.startswith("/"):
        return

    # Ignore if chat not allowed
    if not is_allowed_chat(cast(int, message.chat.id)):
        return

    lang = DEFAULT_LANG
    if message.from_user:
        lang = detect_lang(message.from_user.language_code)

    try:
        msg = await message.answer(
            "📦 <b>Хранилище проекта</b>\n\n"
            "Доступ к материалам доступен по кнопке ниже:",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text=t(lang, "btn_storage"),
                            url=CFG.storage_url
                        )
                    ]
                ]
            )
        )

        # --- unified auto-delete via BOT_MESSAGES (TTL split aware) ---
        async with BOT_MESSAGES_LOCK:
            BOT_MESSAGES[msg.message_id] = (time.time(), "storage")
            BOT_MESSAGES_CHAT_ID[msg.message_id] = chat_id

    except Exception as e:
        logging.warning(f"STORAGE_TRIGGER | failed | error={e}")
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
                # unified TTL policy for all message types (bot + user-triggered)
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

        # check more frequently for better TTL precision
        await asyncio.sleep(5)

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

            # storage trigger cache (chat-based)
            ttl = get_storage_trigger_ttl()
            expired_chats = [
                cid for cid, ts in STORAGE_TRIGGER_CACHE.items()
                if (now - ts) > ttl
            ]
            for cid in expired_chats:
                STORAGE_TRIGGER_CACHE.pop(cid, None)
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
    load_user_registry()
    logging.info(f"REGISTRY | read_only={REGISTRY_READ_ONLY}")
    logging.info(
        f"STARTUP | version={VERSION} "
        f"mute={CFG.mute_new_users} "
        f"delay={CFG.welcome_delay_seconds}s "
        f"autodelete={CFG.auto_delete_seconds}s"
    )
    logging.info(f"BUILD | version={VERSION} channel=stable-1.5.x")
    if not CFG.admin_ids:
        logging.warning("ENV | ADMIN_IDS is empty")

    if not CFG.allowed_chat_ids:
        logging.warning("ENV | ALLOWED_CHAT_IDS is empty (bot allowed in all chats)")
    # Signal handling (safe fallback for macOS local run)
    try:
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, _handle_shutdown)
        loop.add_signal_handler(signal.SIGINT, _handle_shutdown)
    except Exception:
        pass
    logging.info("RUNTIME | async lifecycle guards enabled")
    tasks = []

    # Cleanup tasks enabled in all modes (safe for test-mode)
    tasks.append(asyncio.create_task(cleanup_bot_messages()))
    tasks.append(asyncio.create_task(cleanup_caches()))

    await asyncio.sleep(1)  # anti-flood startup delay
    polling = asyncio.create_task(dp.start_polling(bot))
    tasks.append(polling)

    try:
        await shutdown_event.wait()
    except (asyncio.CancelledError, KeyboardInterrupt):
        logging.info("SHUTDOWN | interruption received inside main")
        shutdown_event.set()
    finally:
        for task in tasks:
            task.cancel()

        for task in tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass

    save_user_registry()
    try:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
    except Exception:
        pass

    logging.info("SHUTDOWN | all tasks stopped cleanly")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("SHUTDOWN | KeyboardInterrupt received (Ctrl+C)")
        shutdown_event.set()
        try:
            if os.path.exists(LOCK_FILE):
                os.remove(LOCK_FILE)
        except Exception:
            pass