"""Official Apple release discovery and durable, conservative delivery state."""
from __future__ import annotations
import hashlib
import json
import re
import sqlite3
import time
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlparse

FEED = 'https://developer.apple.com/news/releases/rss/releases.rss'
MAX_FEED = 2_000_000
PLATFORMS = {'ios':'iOS', 'ipados':'iPadOS', 'macos':'macOS', 'watchos':'watchOS', 'tvos':'tvOS', 'visionos':'visionOS', 'homepod software':'HomePod Software'}
TITLE = re.compile(r'^(iOS|iPadOS|macOS|watchOS|tvOS|visionOS|HomePod Software)\s+(\d+(?:\.\d+){0,4})(?:\s+(RC|Release Candidate)(?:\s*(\d+))?)?(?:\s+\(([A-Za-z0-9]+)\))?$', re.I)

@dataclass(frozen=True)
class Release:
    platform: str
    version: str
    kind: str
    rc: int | None
    build: str | None
    date: str
    source: str
    source_id: str
    @property
    def key(self) -> str:
        values = [self.platform.lower(), self.version, self.kind, self.rc or 0, self.build or '', self.source_id if not self.build else '']
        return hashlib.sha256(json.dumps(values, ensure_ascii=False).encode()).hexdigest()

def parse_feed(raw: bytes, now: float | None = None) -> list[Release]:
    if len(raw) > MAX_FEED or re.search(br'<!\s*(DOCTYPE|ENTITY)', raw, re.I):
        raise ValueError('invalid_feed')
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as e:
        raise ValueError('invalid_feed') from e
    if root.tag != 'rss' or root.find('channel') is None:
        raise ValueError('invalid_feed_structure')
    items = root.findall('./channel/item')
    if not items: raise ValueError('empty_feed')
    out = []
    for item in items[:1000]:
        title = (item.findtext('title') or '').strip()
        match = TITLE.fullmatch(title)
        if not match: continue
        source = (item.findtext('link') or '').strip()
        u = urlparse(source)
        try:
            if u.scheme != 'https' or u.hostname != 'developer.apple.com' or u.username or u.password or u.port not in (None,443) or not u.path.startswith('/news/releases/'):
                continue
        except ValueError:
            continue
        try:
            dt = parsedate_to_datetime(item.findtext('pubDate') or '')
            if dt.tzinfo is None: continue
            if dt.timestamp() > (time.time() if now is None else now) + 300: continue
        except (ValueError, TypeError, OverflowError): continue
        platform, version, rc_name, revision, build = match.groups()
        source_id = (item.findtext('guid') or source).strip()
        out.append(Release(PLATFORMS[platform.lower()],version,'RC' if rc_name else 'RELEASE',int(revision or 1) if rc_name else None,build,dt.astimezone(timezone.utc).isoformat(),source,source_id))
    return out

def message_for(releases: list[Release]) -> str:
    if not releases: raise ValueError('empty_group')
    types = {r.kind for r in releases}
    title = '🍎 Apple представила обновления программного обеспечения' if len(types)>1 else ('🍎 Apple выпустила Release Candidate' if 'RC' in types else '🍎 Apple выпустила обновления программного обеспечения')
    lines=[title,'']
    for r in releases:
        suffix = f' RC{(" " + str(r.rc)) if r.rc and r.rc > 1 else ""}' if r.kind == 'RC' else ''
        lines.append(f'• {r.platform} {r.version}{suffix}' + (f' · сборка {r.build}' if r.build else ''))
    lines += ['', 'Подробный список изменений пока не подготовлен для публикации.', 'Сведения об обновлениях проверены по официальной ленте Apple.', '', 'Technology Universe | Новости технологий']
    return '\n'.join(lines)

class ReleaseStore:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True,exist_ok=True)
        self.db=sqlite3.connect(path, timeout=10, isolation_level=None)
        self.db.row_factory=sqlite3.Row
        self.db.executescript('''PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
        CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS releases(key TEXT PRIMARY KEY,payload TEXT NOT NULL,status TEXT NOT NULL,first_seen REAL NOT NULL,group_id TEXT);
        CREATE TABLE IF NOT EXISTS history(id INTEGER PRIMARY KEY,key TEXT NOT NULL,status TEXT NOT NULL,at REAL NOT NULL,detail TEXT);
        CREATE TABLE IF NOT EXISTS publications(id TEXT PRIMARY KEY,status TEXT NOT NULL,created REAL NOT NULL,message_id INTEGER,error TEXT);
        ''')
        self.db.execute("UPDATE publications SET status='manual_review_required',error='restart_during_send' WHERE status='publishing'")
        self.db.execute("UPDATE releases SET status='manual_review_required' WHERE group_id IN (SELECT id FROM publications WHERE status='manual_review_required')")
    def get(self,key,default=None):
        r=self.db.execute('SELECT value FROM settings WHERE key=?',(key,)).fetchone();return json.loads(r[0]) if r else default
    def set(self,key,value): self.db.execute('INSERT OR REPLACE INTO settings VALUES(?,?)',(key,json.dumps(value)))
    def observe(self, releases, now=None):
        now=time.time() if now is None else now
        self.db.execute('BEGIN IMMEDIATE')
        try:
            baseline=not self.get('baseline',False)
            for r in releases:
                status='detected' if baseline else 'verified'
                cur=self.db.execute('INSERT OR IGNORE INTO releases(key,payload,status,first_seen) VALUES(?,?,?,?)',(r.key,json.dumps(asdict(r),ensure_ascii=False),status,now))
                if cur.rowcount: self.db.execute('INSERT INTO history(key,status,at,detail) VALUES(?,?,?,?)',(r.key,status,now,'initial_baseline' if baseline else 'official_feed'))
            self.set('baseline',True);self.set('last_check',now);self.set('last_error',None)
            self.db.execute('COMMIT')
        except BaseException:
            self.db.execute('ROLLBACK'); raise
        return baseline
    def reserve(self, window=1800, now=None):
        now=time.time() if now is None else now
        self.db.execute('BEGIN IMMEDIATE')
        try:
            first=self.db.execute("SELECT first_seen FROM releases WHERE status='verified' ORDER BY first_seen LIMIT 1").fetchone()
            if not first or first['first_seen'] + window > now:
                self.db.execute('COMMIT'); return None
            rows=self.db.execute("SELECT * FROM releases WHERE status='verified' AND first_seen<=? ORDER BY first_seen LIMIT 25",(first['first_seen']+window,)).fetchall()
            if not rows: self.db.execute('COMMIT');return None
            group=hashlib.sha256(''.join(sorted(x['key'] for x in rows)).encode()).hexdigest()
            self.db.execute("INSERT INTO publications VALUES(?,'publishing',?,NULL,NULL)",(group,now))
            for x in rows:
                self.db.execute("UPDATE releases SET status='publishing',group_id=? WHERE key=? AND status='verified'",(group,x['key']))
                self.db.execute('INSERT INTO history(key,status,at,detail) VALUES(?,?,?,?)',(x['key'],'publishing',now,group))
            self.db.execute('COMMIT');return group,[Release(**json.loads(x['payload'])) for x in rows]
        except BaseException:
            self.db.execute('ROLLBACK');raise
    def finish(self, group,status,message_id=None,error=None):
        if status not in ('published','failed','manual_review_required'): raise ValueError('invalid_status')
        now=time.time(); self.db.execute('BEGIN IMMEDIATE')
        try:
            self.db.execute('UPDATE publications SET status=?,message_id=?,error=? WHERE id=? AND status=\'publishing\'',(status,message_id,error,group))
            rows=self.db.execute('SELECT key FROM releases WHERE group_id=?',(group,)).fetchall()
            self.db.execute('UPDATE releases SET status=? WHERE group_id=?',(status,group))
            for r in rows:self.db.execute('INSERT INTO history(key,status,at,detail) VALUES(?,?,?,?)',(r['key'],status,now,error))
            self.db.execute('COMMIT')
        except BaseException:self.db.execute('ROLLBACK');raise
    def close(self): self.db.close()
