import json
from dataclasses import replace
from types import SimpleNamespace

import pytest
from conftest import TEST_BOT_TOKEN

import Welcome_Bot as app
import healthcheck


def test_bot_token_is_required(monkeypatch):
    monkeypatch.delenv("BOT_TOKEN", raising=False)
    with pytest.raises(RuntimeError, match="BOT_TOKEN"):
        app.load_config()


def test_config_parses_ids_data_dir_and_timing(monkeypatch, tmp_path):
    monkeypatch.setenv("BOT_TOKEN", TEST_BOT_TOKEN)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("ADMIN_IDS", "12, 34")
    monkeypatch.setenv("ALLOWED_CHAT_IDS", "-1001, -1002")
    monkeypatch.setenv("MUTE_NEW_USERS", "false")
    monkeypatch.setenv("MUTE_SECONDS", "240")
    monkeypatch.setenv("AUTO_DELETE_SECONDS", "90")
    cfg = app.load_config()
    assert cfg.data_dir == str(tmp_path)
    assert cfg.admin_ids == {12, 34}
    assert cfg.allowed_chat_ids == {-1001, -1002}
    assert cfg.mute_new_users is False
    assert cfg.mute_seconds == 240
    assert cfg.auto_delete_seconds == 90


def test_data_dir_defaults_to_data(monkeypatch):
    monkeypatch.setenv("BOT_TOKEN", TEST_BOT_TOKEN)
    monkeypatch.delenv("DATA_DIR", raising=False)
    assert app.load_config().data_dir == "/data"


@pytest.fixture
def isolated_registry(monkeypatch, tmp_path):
    data_dir = tmp_path / "data"
    monkeypatch.setattr(app, "DATA_DIR", data_dir)
    monkeypatch.setattr(app, "USER_REGISTRY_FILE", str(data_dir / "user_registry.json"))
    app.USER_REGISTRY.clear()
    monkeypatch.setattr(app, "REGISTRY_PERSISTENCE_BLOCKED", False)
    return data_dir / "user_registry.json"


def test_registry_save_is_atomic_and_loadable(isolated_registry):
    app.USER_REGISTRY[123] = {
        "source": app.JoinSource.TELEGRAM,
        "labels": {"new"},
        "first_seen": 1.5,
        "chat_id": -1001,
    }
    app.save_user_registry()
    assert isolated_registry.exists()
    assert isolated_registry.stat().st_mode & 0o777 == 0o600
    assert not list(isolated_registry.parent.glob("tmp*"))
    app.USER_REGISTRY.clear()
    app.load_user_registry()
    assert app.USER_REGISTRY[123]["labels"] == {"new"}
    assert app.USER_REGISTRY[123]["chat_id"] == -1001


def test_registry_backup_is_atomic_private_and_preserves_registry(isolated_registry):
    isolated_registry.parent.mkdir(parents=True)
    original = '{"users":{"123":{}}}'
    isolated_registry.write_text(original, encoding="utf-8")
    backup = app.create_registry_backup()
    assert backup.parent == isolated_registry.parent / "backups"
    assert backup.read_text(encoding="utf-8") == original
    assert isolated_registry.read_text(encoding="utf-8") == original
    assert backup.stat().st_mode & 0o777 == 0o600
    assert not list(backup.parent.glob(".registry-backup-*"))


def test_control_handler_serves_admin_panel_without_telegram_api(monkeypatch):
    import asyncio

    monkeypatch.setattr(app, "CFG", replace(app.CFG, admin_ids={42}))

    class FakeMessage:
        from_user = SimpleNamespace(id=42, language_code="ru")
        chat = SimpleNamespace(type="private")

        def __init__(self):
            self.answer_args = None

        async def answer(self, *args, **kwargs):
            self.answer_args = (args, kwargs)

    message = FakeMessage()
    asyncio.run(app.admin_control_panel(message))
    assert message.answer_args
    assert message.answer_args[1]["reply_markup"].inline_keyboard


def test_missing_data_directory_is_created_on_save(isolated_registry):
    assert not isolated_registry.parent.exists()
    app.USER_REGISTRY.clear()
    app.save_user_registry()
    assert isolated_registry.exists()


def test_corrupt_registry_is_rejected_without_crashing(isolated_registry):
    isolated_registry.parent.mkdir(parents=True)
    isolated_registry.write_text("{broken", encoding="utf-8")
    app.USER_REGISTRY.clear()
    app.load_user_registry()
    assert app.USER_REGISTRY == {}
    assert app.REGISTRY_PERSISTENCE_BLOCKED is True
    app.save_user_registry()
    assert isolated_registry.read_text(encoding="utf-8") == "{broken"


def test_oversized_registry_is_rejected(monkeypatch, isolated_registry):
    isolated_registry.parent.mkdir(parents=True)
    isolated_registry.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(app, "MAX_REGISTRY_BYTES", 1)
    app.USER_REGISTRY.clear()
    app.load_user_registry()
    assert app.USER_REGISTRY == {}
    app.save_user_registry()
    assert isolated_registry.read_text(encoding="utf-8") == "{}"


def test_registry_structure_is_checked(isolated_registry):
    isolated_registry.parent.mkdir(parents=True)
    isolated_registry.write_text(json.dumps({"users": []}), encoding="utf-8")
    app.USER_REGISTRY.clear()
    app.load_user_registry()
    assert app.USER_REGISTRY == {}
    app.save_user_registry()
    assert json.loads(isolated_registry.read_text(encoding="utf-8")) == {"users": []}


def test_legacy_schema_upgrade_replaces_file_atomically(monkeypatch, isolated_registry):
    isolated_registry.parent.mkdir(parents=True)
    isolated_registry.write_text(json.dumps({"users": {"123": {"labels": []}}}), encoding="utf-8")
    replaced = []
    original_replace = app.os.replace

    def recording_replace(source, destination):
        replaced.append((source, destination))
        return original_replace(source, destination)

    monkeypatch.setattr(app.os, "replace", recording_replace)
    app.USER_REGISTRY.clear()
    app.load_user_registry()
    data = json.loads(isolated_registry.read_text(encoding="utf-8"))
    assert data[app.REGISTRY_META_KEY] == app.REGISTRY_SCHEMA_VERSION
    assert replaced


def test_flat_legacy_registry_is_migrated_without_data_loss(monkeypatch, isolated_registry):
    isolated_registry.parent.mkdir(parents=True)
    legacy = {"123": {"source": "invite_link", "labels": ["member"], "chat_id": -1001}}
    isolated_registry.write_text(json.dumps(legacy), encoding="utf-8")
    app.USER_REGISTRY.clear()
    app.load_user_registry()
    assert app.USER_REGISTRY[123]["source"] == "invite_link"
    assert app.USER_REGISTRY[123]["labels"] == {"member"}
    saved = json.loads(isolated_registry.read_text(encoding="utf-8"))
    assert saved["users"]["123"]["source"] == "invite_link"


def test_log_redacts_token_shaped_values(caplog):
    app.logging.warning(
        "request failed for token=%s url=%s authorization=%s",
        TEST_BOT_TOKEN,
        "https://api.example/?access_token=hidden",
        "Bearer authorization-secret-value",
    )
    assert TEST_BOT_TOKEN not in caplog.text
    assert "access_token=hidden" not in caplog.text
    assert "authorization-secret-value" not in caplog.text
    assert "[REDACTED_TOKEN]" in caplog.text


def test_log_redacts_token_in_exception_traceback(caplog):
    try:
        raise RuntimeError(f"request URL contained {TEST_BOT_TOKEN}")
    except RuntimeError:
        app.logging.exception("Telegram request failed")
    assert TEST_BOT_TOKEN not in caplog.text
    assert "[REDACTED_TOKEN]" in caplog.text


def test_project_name_is_html_escaped(monkeypatch):
    monkeypatch.setattr(app, "CFG", replace(app.CFG, project_name="<TU & Co>"))
    message = app.build_welcome_text(SimpleNamespace(full_name="A < B"), app.JoinSource.TELEGRAM, "en")
    assert "&lt;TU &amp; Co&gt;" in message
    assert "A &lt; B" in message


@pytest.mark.parametrize("url", ["javascript:alert(1)", "file:///etc/passwd", "https://user:pass@example.org"])
def test_unsafe_urls_are_rejected(url):
    with pytest.raises(RuntimeError):
        app.validate_url(url, "STORAGE_URL")


@pytest.mark.parametrize("url", ["https://example.org/a", "http://localhost:8080", "tg://resolve?domain=example"])
def test_valid_urls_are_preserved(url):
    assert app.validate_url(url, "FAQ_URL") == url


def test_welcome_image_accepts_file_id_and_https_but_rejects_unsafe_scheme():
    assert app.validate_welcome_image("AgACAgQAAxkBAA") == "AgACAgQAAxkBAA"
    assert app.validate_welcome_image("https://example.org/welcome.jpg") == "https://example.org/welcome.jpg"
    with pytest.raises(RuntimeError, match="WELCOME_IMAGE_URL"):
        app.validate_welcome_image("javascript:alert(1)//")


def test_startup_lock_is_exclusive(monkeypatch, tmp_path):
    monkeypatch.setattr(app, "LOCK_FILE", str(tmp_path / "bot.lock"))
    assert app.acquire_startup_lock() is True
    assert app.acquire_startup_lock() is False
    app.release_startup_lock()
    assert app.acquire_startup_lock() is True
    app.release_startup_lock()


def test_config_rejects_invalid_ids_bool_and_timing(monkeypatch):
    monkeypatch.setenv("BOT_TOKEN", TEST_BOT_TOKEN)
    monkeypatch.setenv("ADMIN_IDS", TEST_BOT_TOKEN)
    with pytest.raises(RuntimeError, match="ADMIN_IDS") as error:
        app.load_config()
    assert TEST_BOT_TOKEN not in str(error.value)
    monkeypatch.setenv("ADMIN_IDS", "")
    monkeypatch.setenv("ALLOWED_CHAT_IDS", "broken")
    with pytest.raises(RuntimeError, match="ALLOWED_CHAT_IDS"):
        app.load_config()
    monkeypatch.setenv("ALLOWED_CHAT_IDS", "")
    monkeypatch.setenv("MUTE_NEW_USERS", "sometimes")
    with pytest.raises(RuntimeError, match="MUTE_NEW_USERS"):
        app.load_config()
    monkeypatch.setenv("MUTE_NEW_USERS", "true")
    monkeypatch.setenv("AUTO_DELETE_SECONDS", "-1")
    with pytest.raises(RuntimeError, match="AUTO_DELETE_SECONDS"):
        app.load_config()


def test_version_command_uses_single_version_constant(monkeypatch):
    import asyncio

    monkeypatch.setattr(app, "CFG", replace(app.CFG, admin_ids={42}))
    monkeypatch.setattr(app, "VERSION", "1.6.0")

    class FakeMessage:
        from_user = SimpleNamespace(id=42)
        chat = SimpleNamespace(type="private")

        def __init__(self):
            self.text = None

        async def answer(self, text):
            self.text = text

    message = FakeMessage()
    asyncio.run(app.version_cmd(message))
    assert app.VERSION == "1.6.0"
    assert "Version: 1.6.0" in message.text


def test_shutdown_signal_sets_event(monkeypatch):
    import asyncio

    event = asyncio.Event()
    monkeypatch.setattr(app, "shutdown_event", event)
    app._handle_shutdown()
    assert event.is_set()


def test_healthcheck_requires_recent_heartbeat_from_live_process(monkeypatch, tmp_path):
    marker = tmp_path / "heartbeat"
    monkeypatch.setattr(healthcheck, "HEALTH_FILE", marker)
    marker.write_text(str(app.os.getpid()), encoding="ascii")
    assert healthcheck.main() == 0
    old_time = app.time.time() - healthcheck.MAX_AGE_SECONDS - 1
    app.os.utime(marker, (old_time, old_time))
    assert healthcheck.main() == 1


def test_healthcheck_rejects_stopped_process(monkeypatch, tmp_path):
    marker = tmp_path / "heartbeat"
    monkeypatch.setattr(healthcheck, "HEALTH_FILE", marker)
    marker.write_text("999999999", encoding="ascii")
    assert healthcheck.main() == 1


def test_healthcheck_rejects_symlink_marker(monkeypatch, tmp_path):
    marker = tmp_path / "heartbeat"
    target = tmp_path / "target"
    target.write_text(str(app.os.getpid()), encoding="ascii")
    marker.symlink_to(target)
    monkeypatch.setattr(healthcheck, "HEALTH_FILE", marker)
    assert healthcheck.main() == 1


def test_blank_optional_storage_url_disables_storage_button(monkeypatch):
    monkeypatch.setattr(app, "CFG", replace(app.CFG, storage_url=None))
    markup = app.welcome_keyboard("en")
    callbacks = [button.callback_data for row in markup.inline_keyboard for button in row]
    urls = [button.url for row in markup.inline_keyboard for button in row if button.url]
    assert "rules:en" in callbacks
    assert "https://example.com/storage" not in urls


def test_startup_lock_release_closes_descriptor(monkeypatch, tmp_path):
    lock_path = tmp_path / "bot.lock"
    monkeypatch.setattr(app, "LOCK_FILE", str(lock_path))
    assert app.acquire_startup_lock()
    held_fd = app._LOCK_FD
    app.release_startup_lock()
    assert app._LOCK_FD is None
    with pytest.raises(OSError):
        app.os.fstat(held_fd)
    assert app.acquire_startup_lock()
    app.release_startup_lock()


def test_stale_lock_file_is_recoverable_after_process_exit(monkeypatch, tmp_path):
    lock_path = tmp_path / "bot.lock"
    monkeypatch.setattr(app, "LOCK_FILE", str(lock_path))
    assert app.acquire_startup_lock()
    stale_fd = app._LOCK_FD
    app._LOCK_FD = None
    app.os.close(stale_fd)
    assert lock_path.exists()
    assert app.acquire_startup_lock()
    app.release_startup_lock()


def test_version_constant_is_160():
    assert app.VERSION == "1.6.0"
