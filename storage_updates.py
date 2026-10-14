"""Storage Update Manager 1.1. Pure state machine; network adapters injected."""
import asyncio
import hashlib
import html
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.parse import urlsplit

LOG = logging.getLogger(__name__)
DENIED = {'.env', 'config', 'configs', 'configuration', 'backup', 'backups', 'secrets', 'private', 'credentials', 'node_modules', '__pycache__'}
SUFFIXES = {'.env', '.pem', '.key', '.p12', '.pfx', '.sqlite', '.db', '.sql', '.dump', '.toml', '.ini', '.yaml', '.yml', '.conf', '.config', '.json', '.part', '.partial', '.tmp', '.crdownload', '.download', '.swp'}

def safe_path(value):
    if not isinstance(value, str) or not value or len(value) > 2048 or '\\' in value or any(ord(c) < 32 for c in value):
        return False
    p = PurePosixPath(value)
    return not p.is_absolute() and all(x not in {'.', '..'} and not x.startswith('.') and x.lower() not in DENIED for x in value.split('/')) and p.suffix.lower() not in SUFFIXES and p.stem.lower() not in DENIED | {'token', 'tokens', 'password', 'passwords'}

@dataclass
class Config:
    enabled: bool = False
    watch_path: str = 'Для канала/'
    public_url: str = 'https://universe-files.online'
    chat_id: int = 0
    delay: int = 60
    max_wait: int = 600
    max_files: int = 10
    cover: bool = True
    admin_alerts: bool = False
    api_url: str = ''
    api_token: str = ''
    poll: int = 15
    timezone: str = 'Europe/Minsk'
    template: str = ''

    @classmethod
    def environment(cls):
        def flag(k, d): return os.getenv(k, str(d)).lower() == 'true'
        c = cls(enabled=flag('STORAGE_NOTIFICATIONS_ENABLED', False),
                watch_path=os.getenv('STORAGE_WATCH_PATH', 'Для канала/'),
                public_url=os.getenv('STORAGE_PUBLIC_URL', 'https://universe-files.online'),
                chat_id=int(os.getenv('STORAGE_NOTIFICATION_CHAT_ID', '0')),
                delay=int(os.getenv('STORAGE_NOTIFICATION_DELAY', '60')),
                max_wait=int(os.getenv('STORAGE_MAX_BATCH_WAIT', '600')),
                max_files=int(os.getenv('STORAGE_MAX_FILES_IN_POST', '10')),
                cover=flag('STORAGE_COVER_ENABLED', True), admin_alerts=flag('STORAGE_ADMIN_ALERTS_ENABLED', False),
                api_url=os.getenv('STORAGE_EVENTS_API_URL', ''), api_token=os.getenv('STORAGE_EVENTS_API_TOKEN', ''),
                template=os.getenv('STORAGE_COVER_TEMPLATE', ''))
        if c.delay < 1 or c.max_wait < c.delay or not 1 <= c.max_files <= 10:
            raise ValueError('Invalid storage batching limits')
        for value in (c.public_url, c.api_url):
            if value:
                u = urlsplit(value)
                if u.scheme != 'https' or not u.hostname or u.username or u.password or u.query or u.fragment:
                    raise ValueError('Storage URLs must be HTTPS without credentials/query')
        if c.enabled and (not c.api_url or len(c.api_token) < 32 or not c.chat_id):
            raise ValueError('Storage configuration incomplete')
        return c

class State:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.data = {'schema': 1, 'baseline': False, 'seen': {}, 'candidates': {}, 'pending': {}, 'outbox': [], 'history': [], 'errors': [], 'last_check': None, 'last_publish': None}
        if self.path.exists():
            self.data = json.loads(self.path.read_text())  # corrupt state fails closed; never silently rebaseline
            if self.data.get('schema') != 1: raise ValueError('Unsupported storage state schema')
        for job in self.data['outbox']:
            if job['status'] == 'sending': job['status'] = 'uncertain'
        self.save()

    def save(self):
        tmp = self.path.with_suffix('.tmp')
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, 'w') as f:
            json.dump(self.data, f, ensure_ascii=False)
            f.flush(); os.fsync(f.fileno())
        os.replace(tmp, self.path)
        fd = os.open(self.path.parent, os.O_RDONLY)
        try: os.fsync(fd)
        finally: os.close(fd)

    def error(self, code, now):
        self.data['errors'] = (self.data['errors'] + [{'at': now, 'code': code}])[-100:]
        LOG.warning('storage_update_error code=%s', code)
        self.save()

class RetryDelivery(Exception):
    def __init__(self, delay=30): self.delay = delay
class UncertainDelivery(Exception): pass
class PermanentDelivery(Exception): pass

class Manager:
    def __init__(self, config, state, publisher, verifier, clock=time.time):
        self.config, self.state, self.publisher, self.verifier, self.clock = config, state, publisher, verifier, clock
        self.lock = asyncio.Lock()
        self.health = 'initializing'

    @property
    def enabled(self): return self.state.data.get('enabled', self.config.enabled)

    async def set_enabled(self, value):
        async with self.lock:
            d = self.state.data
            d['enabled'] = value
            if not value:
                d['pending'] = {}; d['candidates'] = {}
                for j in d['outbox']:
                    if j['status'] == 'queued': j['status'] = 'cancelled'
            else:
                # Re-enable starts a clean baseline, never replays downtime uploads.
                d['baseline'] = False
            self.state.save()

    @staticmethod
    def version(f): return hashlib.sha256(json.dumps([f['size'], f['etag']], separators=(',', ':')).encode()).hexdigest()

    async def observe(self, snapshot):
        async with self.lock:
            d, now = self.state.data, self.clock()
            if snapshot.get('root') != self.config.watch_path or not snapshot.get('ready'):
                raise ValueError('Feed unavailable or root mismatch')
            files = {f['id']: f for f in snapshot['files'] if safe_path(f.get('path')) and isinstance(f.get('size'), int) and f['size'] > 0 and f.get('etag')}
            d['last_check'] = now
            if not d['baseline'] or not self.enabled:
                d['seen'] = {i: self.version(f) for i, f in files.items()}
                d['pending'] = {}; d['candidates'] = {}; d['baseline'] = True
                self.health = 'disabled' if not self.enabled else 'watching'
                self.state.save(); return
            for i in list(d['pending']):
                if i not in files: del d['pending'][i]
                elif self.version(files[i]) != d['pending'][i]['version'] or files[i].get('busy'):
                    del d['pending'][i]
                else: d['pending'][i]['path'] = files[i]['path']
            for i in list(d['seen']):
                if i not in files: del d['seen'][i]
            for i in list(d['candidates']):
                if i not in files: del d['candidates'][i]
            for i, f in files.items():
                v = self.version(f)
                if f.get('busy'):
                    d['candidates'].pop(i, None); continue
                if d['seen'].get(i) == v: continue  # stable ID rename isn't a new upload
                c = d['candidates'].get(i)
                if not c or c['version'] != v:
                    d['candidates'][i] = {'version': v, 'first': now}; continue
                if now - c['first'] < self.config.poll: continue
                if not await self.verifier(f): continue
                d['pending'][i] = {**f, 'version': v, 'detected': c['first'], 'completed': now, 'updated': i in d['seen']}
                d['seen'][i] = v; del d['candidates'][i]
            pending = list(d['pending'].values())
            if pending:
                quiet = now - max(f['completed'] for f in pending) >= self.config.delay
                deadline = now - min(f['completed'] for f in pending) >= self.config.max_wait
                # At max wait publish completed files only; slow uploads remain pending for later batches.
                if (quiet and not d['candidates'] and not snapshot.get('uploading')) or deadline:
                    d['outbox'].append({'id': str(uuid.uuid4()), 'files': pending, 'status': 'queued', 'created': now, 'next': now, 'attempts': 0})
                    d['pending'] = {}
            self.health = 'watching'; self.state.save()
            await self.deliver(files)

    async def deliver(self, current):
        d, now = self.state.data, self.clock()
        if not self.enabled: return
        for job in d['outbox']:
            if job['status'] != 'queued' or job['next'] > now: continue
            valid = []
            for f in job['files']:
                live = current.get(f['id'])
                if live and not live.get('busy') and self.version(live) == f['version'] and await self.verifier(live):
                    valid.append({**f, 'path': live['path']})
            if not valid:
                job['status'] = 'cancelled'; self.state.save(); continue
            job['status'] = 'sending'; job['attempts'] += 1; self.state.save()
            try:
                message_id = await self.publisher(valid, now)
            except RetryDelivery as e:
                job['status'] = 'queued'; job['next'] = now + max(e.delay, min(900, 2 ** min(job['attempts'], 9)))
                self.state.error('telegram_retry', now)
            except PermanentDelivery:
                job['status'] = 'failed'; self.state.error('telegram_rejected', now)
            except Exception:
                # Telegram has no idempotency key: timeout may mean delivered. Quarantine, don't duplicate.
                job['status'] = 'uncertain'; self.state.error('telegram_delivery_uncertain', now)
            else:
                job['status'] = 'sent'; job['message_id'] = message_id; job['published'] = now
                d['last_publish'] = now
                d['history'] = (d['history'] + [{'id': job['id'], 'message_id': message_id, 'published': now, 'count': len(valid), 'files': valid}])[-100:]
            self.state.save()
        d['outbox'] = [j for j in d['outbox'] if j['status'] in {'queued', 'sending', 'failed', 'uncertain'}]
        self.state.save()

def post(files, now, config, test=False):
    date = datetime.fromtimestamp(now, ZoneInfo(config.timezone)).strftime('%d.%m.%Y')
    size = sum(f['size'] for f in files)
    unit = 'Б'
    for u in ('КиБ', 'МиБ', 'ГиБ', 'ТиБ'):
        if size < 1024: break
        size /= 1024; unit = u
    names = '\n'.join(('🔄 ' if f.get('updated') else '📁 ') + html.escape(PurePosixPath(f['path']).name.encode('utf-16-le')[:80].decode('utf-16-le', errors='ignore')) for f in files[:config.max_files])
    remaining = len(files) - config.max_files
    if remaining > 0: names += f'\n📦 И ещё {remaining} файлов'
    text = f"🌌 <b>TECHNOLOGY UNIVERSE</b>\n\n{'🧪 ТЕСТОВАЯ ПУБЛИКАЦИЯ' if test else '🚀 <b>ХРАНИЛИЩЕ ОБНОВЛЕНО!</b>'}\n\n📦 <b>НОВЫЕ И ОБНОВЛЁННЫЕ ФАЙЛЫ</b>\n{names}\n\n📊 Файлов: {len(files)}\n💾 Общий размер: {size:.1f} {unit}\n📅 {date}\n\n✨ Материалы доступны участникам сообщества.\nTechnology Universe — технологии без границ."
    # HTML entities count as one displayed character in Telegram.
    return text
