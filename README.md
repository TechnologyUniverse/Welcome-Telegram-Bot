# Welcome Telegram Bot

**Version:** `1.6.0`

**Python:** `3.13.4`

**Version source:** `VERSION` constant in `Welcome_Bot.py`

Telegram welcome and basic moderation bot for Technology Universe.

## Features

- Russian and English welcome messages and rules.
- Welcomes for regular joins, invite links, approved joins and paid-like join flows.
- Optional welcome image, storage/FAQ/support links and community information.
- Configurable new-member mute and message auto-delete.
- Admin panel and commands: `/control`, `/health`, `/version`, `/welcome`, `/mute`, `/autodelete`.
- Persistent user registry, registry backup/export and protected migration controls.
- Long-poll retry with backoff and graceful shutdown.

Paid-like chat detection is a Telegram UX heuristic. Payment processing is not implemented.

## Configuration

Copy `.env.example` to `.env` for local use. `BOT_TOKEN` is required and secret; keep it out of Git and container build arguments. The example contains no real credentials. See its comments for every supported environment variable and defaults.

`ALLOWED_CHAT_IDS` empty means **all chats are allowed**. Set it deliberately in production. `ADMIN_IDS` empty means admin commands are unavailable. Set at least one administrator ID before production use.

Only `http://`, `https://` and Telegram `tg://` links are accepted for configured buttons. An empty `STORAGE_URL`, `FAQ_URL` or `SUPPORT_URL` disables that link/button.

## Local development

1. Install Python `3.13.4` and create a virtual environment.
2. Install runtime packages with `python -m pip install --require-hashes -r requirements.lock`.
3. Copy `.env.example` to `.env`; set a development bot token and a writable `DATA_DIR` such as `./data`.
4. Run `python Welcome_Bot.py`.
5. Install locked test dependencies with `python -m pip install --require-hashes -r requirements-dev.lock`, then run `python -m pytest -q`.

Unit tests use a syntactically valid dummy token. They do not call Telegram API and can run offline after dependencies are installed.

## Persistent storage and backup

Runtime data is stored only under `DATA_DIR`:

- Registry: `user_registry.json`
- Backups: `backups/user_registry_backup_<UTC timestamp>_<unique id>.json`

Registry writes stage a temporary file in the same filesystem, flush and `fsync` it, then replace the destination atomically. File size and JSON structure are checked before loading. On a corrupt registry, the original file is kept and persistence is blocked so shutdown cannot overwrite it with an empty registry.

Before moving an existing deployment to the container, copy its current `user_registry.json` into `/srv/storage/telegram/welcome-telegram-bot/user_registry.json` and preserve a separate backup. Create the target directory with owner UID/GID `10001` before starting the container. The application does not discover legacy files through its working directory.

To restore, stop the container first and make a separate copy of the current registry. Copy the selected file from `/data/backups/` to `/data/user_registry.json`, set owner UID/GID `10001` and mode `0600`, then start the container and verify `/health` and registry validation. Never restore while the bot is writing the registry.

## Docker

The image uses `python:3.13.4-slim`, runs as UID/GID `10001`, and includes only the bot code and locked runtime dependencies. Build locally with `docker build -t welcome-telegram-bot:1.6.0 .`.

`docker-compose.yml` is a standalone service. It has a read-only root filesystem, writable `/data` volume and `/tmp` tmpfs, dropped capabilities, `no-new-privileges`, CPU/RAM/PID limits, no published ports, and no Docker socket. It does not require PostgreSQL or Redis and is not joined to `novaryn-network`. Network access is outbound to Telegram Bot API over HTTPS.

For production, set `WELCOME_BOT_IMAGE` in the protected local `.env` to the image digest printed by the Docker workflow, for example `ghcr.io/technologyuniverse/welcome-telegram-bot@sha256:<digest>`. Do not use `latest` or a mutable version tag as the deployed reference. Supply `BOT_TOKEN` through the protected environment; never add it to the image.

## Healthcheck

Docker runs a local healthcheck against `/tmp/welcome_bot_health`, refreshed by the asyncio event loop. It verifies the heartbeat age and process, without an HTTP endpoint or Telegram API request. `/health` remains an admin-only Telegram command. `/tmp` is a tmpfs, so this works with the read-only root filesystem.

## GitHub Actions and GHCR

`ci.yml` runs on pull requests and `main`: locked dependency install, syntax check, unit tests, workflow YAML validation, Compose validation, patch whitespace check and a Docker build. CI does not publish or deploy.

`docker.yml` verifies pushes to `main` and the `v1.6.0` tag, then publishes to GHCR. The tag produces `v1.6.0` and a full commit SHA tag (`sha-<full-commit>`); the workflow also prints the resulting image digest. Actions are pinned to full commit SHAs. It uses only `GITHUB_TOKEN` with `contents: read` and `packages: write`. No SSH deployment or VPS changes are configured.

## Production notes

The bot is a standalone long-polling container. It uses Telegram Bot API over outbound HTTPS; it needs no inbound ports, PostgreSQL, Redis or Docker socket. Back up `/data` separately and verify restores before relying on it for production data.
