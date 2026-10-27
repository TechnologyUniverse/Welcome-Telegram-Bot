"""Safe-mode integration using the existing aiogram Bot and administrator IDs."""
import asyncio
import logging
import hashlib
import io
import os
import time
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from aiohttp import ClientSession, ClientTimeout
from aiogram.filters import Command
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from aiogram.types import BufferedInputFile
from .core import FEED, MAX_FEED, Release, ReleaseStore, message_for, parse_feed
from .cover import make_cover
from . import library

log=logging.getLogger('welcome.apple_release')
class Monitor:
    def __init__(self,bot,admin_ids,data_dir):
        self.bot=bot; self.admins=set(admin_ids); self.store=ReleaseStore(Path(data_dir)/'apple-releases.sqlite3');self.lock=asyncio.Lock();self.stopped=asyncio.Event()
        if self.store.get('enabled') is None:
            self.store.set('enabled',os.getenv('APPLE_MONITOR_ENABLED','true').lower() in ('true','1','yes'))
        self.data_dir=Path(data_dir);self.assets=self.data_dir/'apple-release-assets'
        self.target=(os.getenv('APPLE_RELEASE_CHANNEL_ID') or os.getenv('STORAGE_NOTIFICATION_CHAT_ID','')).strip()
        self.interval=max(300,int(os.getenv('APPLE_MONITOR_INTERVAL','900')))
        self.interval=max(300,min(86400,int(self.store.get('interval',self.interval))))
        self.window=max(0,int(os.getenv('APPLE_RELEASE_GROUP_WINDOW','1800')))
    async def check(self):
        if self.lock.locked(): return 'Проверка уже выполняется.'
        async with self.lock:
            try:
                timeout=ClientTimeout(total=15)
                async with ClientSession(timeout=timeout) as session:
                    async with session.get(FEED,allow_redirects=False) as r:
                        if r.status!=200: raise ValueError('apple_http_'+str(r.status))
                        raw=bytearray()
                        async for chunk in r.content.iter_chunked(65536):
                            raw.extend(chunk)
                            if len(raw)>MAX_FEED: raise ValueError('apple_feed_too_large')
                        raw=bytes(raw)
                releases=parse_feed(raw)
                if not releases: raise ValueError('no_supported_releases')
                baseline=self.store.observe(releases)
                return f'Проверено: {len(releases)} подтверждённых записей.' + (' Начальная история сохранена без публикации.' if baseline else '')
            except Exception as e:
                self.store.set('last_error',type(e).__name__);log.warning('Apple release check failed: %s',type(e).__name__)
                return 'Источник Apple временно недоступен или формат ленты изменился.'
    def render(self,releases):
        logo=Path('/app/assets/apple_release/brand-original.png')
        device=self.assets/'devices-approved.png'; manifest=self.assets/'device-manifest.json'
        with tempfile.TemporaryDirectory(prefix='apple-cover-') as tmp:
            output=Path(tmp)/'cover.png';make_cover(releases,logo,device,output,manifest)
            return output.read_bytes()
    async def publish(self):
        if not self.target or not self.store.get('auto_publish',False) or not self.store.get('preview_verified',False): return
        try:
            bot_user=await self.bot.get_me()
            chat=await self.bot.get_chat(self.target)
            member=await self.bot.get_chat_member(self.target,bot_user.id)
            channel_denied=chat.type=='channel' and member.status=='administrator' and not getattr(member,'can_post_messages',False)
            if chat.type not in ('channel','group','supergroup') or member.status not in ('creator','administrator') or channel_denied:
                self.store.set('last_error','channel_post_permission');self.store.set('auto_publish',False);return
        except Exception:
            self.store.set('last_error','channel_permission_check');return
        group=self.store.reserve(self.window)
        if not group: return
        key,releases=group
        try:
            photo=await asyncio.to_thread(self.render,releases)
            body=message_for(releases)
            if len(body.encode('utf-16-le'))//2 > 1024:
                # Do not risk a partial multi-message publication; quarantine for review.
                self.store.finish(key,'manual_review_required',error='caption_exceeds_limit');return
            sent=await self.bot.send_photo(chat_id=self.target,photo=BufferedInputFile(photo,filename='apple-update.png'),caption=body,parse_mode=None)
        except (TelegramBadRequest,TelegramForbiddenError) as e:
            self.store.finish(key,'failed',error=type(e).__name__)
        except TelegramRetryAfter:
            self.store.finish(key,'failed',error='rate_limited')
        except Exception as e:
            # A network exception after starting sendPhoto is ambiguous. Never blindly retry.
            self.store.finish(key,'manual_review_required',error=type(e).__name__)
        else:self.store.finish(key,'published',message_id=sent.message_id)
    async def command(self,message):
        if not message.from_user or message.from_user.id not in self.admins or message.chat.type!='private': return
        args=(message.text or message.caption or '').split(maxsplit=1); cmd=args[0].split('@')[0] if args else ''
        if cmd=='/apple_check': await message.answer(await self.check());return
        if cmd=='/apple_monitor_on': self.store.set('enabled',True);await message.answer('Мониторинг включён; автопубликация отдельно выключена.');return
        if cmd=='/apple_monitor_off': self.store.set('enabled',False);await message.answer('Мониторинг выключен.');return
        if cmd=='/apple_test':
            example=Release('iOS','28.0','RC',1,None,datetime.now(timezone.utc).isoformat(),FEED,'test')
            try: image=await asyncio.to_thread(self.render,[example])
            except Exception:
                await message.answer('Тест не отправлен: нужны проверенный прозрачный шаблон устройств и маски экранов.');return
            try:
                await self.bot.send_photo(chat_id=message.chat.id,photo=BufferedInputFile(image,filename='apple-test.png'),caption='ТЕСТОВЫЙ ПРИМЕР\n\n'+message_for([example]),parse_mode=None)
            except Exception:
                await message.answer('Не удалось отправить тест в личный чат.');return
            self.store.set('preview_verified',True);await message.answer('Тестовое изображение отправлено вам в личный чат.');return
        if cmd=='/apple_autopublish_on':
            if not self.target or not self.store.get('preview_verified',False):
                await message.answer('Для включения укажите канал и сначала выполните /apple_test.');return
            self.store.set('auto_publish',True);await message.answer('Автопубликация включена; права в канале проверяются перед отправкой.');return
        if cmd=='/apple_autopublish_off': self.store.set('auto_publish',False);await message.answer('Автопубликация выключена.');return
        if cmd=='/apple_wallpapers':
            items=library.list_items(self.assets/'apple_wallpapers')[-15:]
            lines=[f"{x['platform']} {x['generation']} · {'подтверждено' if x['verified'] else 'ожидает проверки'}\nSHA-256: {x['sha256']}" for x in items]
            await message.answer('🖼 Библиотека изображений\n'+('\n'.join(lines) if lines else 'Пока пуста.'));return
        if cmd=='/apple_wallpaper_add':
            try:
                if not message.document: raise ValueError('Отправьте изображение файлом с командой в подписи.')
                if (message.document.file_size or 0)>library.MAX_BYTES:raise ValueError('Файл слишком большой.')
                parts=args[1].split('|',1); fields=parts[0].split(); platform,generation,source=fields
                if len(parts)!=2:raise ValueError('Укажите сведения о правах после |.')
                stream=io.BytesIO();await self.bot.download(message.document,destination=stream)
                item=library.add(self.assets/'apple_wallpapers',platform,int(generation),source,parts[1],stream.getvalue())
                await message.answer(f"Сохранено для проверки. SHA-256: {item['sha256']}\nПроверьте источник и права, затем подтвердите командой /apple_wallpaper_approve SHA256.")
            except (ValueError,IndexError) as e:await message.answer(str(e) or 'Неверные параметры.')
            except Exception:await message.answer('Не удалось получить изображение.');return
            return
        if cmd=='/apple_wallpaper_approve':
            try:
                item=library.approve(self.assets/'apple_wallpapers',args[1].strip())
                await message.answer(f"Подтверждено: {item['platform']} {item['generation']}.")
            except (ValueError,IndexError,OSError):await message.answer('Не удалось подтвердить изображение: проверьте SHA-256 и файл.')
            return
        if cmd=='/apple_interval':
            try:
                seconds=int(args[1]);assert 300<=seconds<=86400
            except (IndexError,ValueError,AssertionError):
                await message.answer('Укажите интервал в секундах: от 300 до 86400. Например: /apple_interval 900');return
            self.interval=seconds;self.store.set('interval',seconds);await message.answer(f'Интервал проверки: {seconds} сек.');return
        if cmd=='/apple_recent':
            import json
            rows=self.store.db.execute('SELECT payload,status FROM releases ORDER BY first_seen DESC LIMIT 10').fetchall()
            lines=[]
            for row in rows:
                r=json.loads(row['payload']);lines.append(f"{r['platform']} {r['version']} {r['kind']} · {row['status']}")
            await message.answer('Последние релизы:\n'+('\n'.join(lines) if lines else 'Пока нет записей.'));return
        if cmd=='/apple_status':
            rows=self.store.db.execute('SELECT status,count(*) n FROM releases GROUP BY status').fetchall()
            summary=', '.join(f'{r["status"]}: {r["n"]}' for r in rows) or 'нет записей'
            await message.answer(f'Мониторинг: {"включён" if self.store.get("enabled",False) else "выключен"}\nАвтопубликация: {"включена" if self.store.get("auto_publish",False) else "выключена"}\nРелизы: {summary}\nПоследняя ошибка: {self.store.get("last_error") or "нет"}');return
        if cmd=='/apple_history':
            rows=self.store.db.execute('SELECT status,created,message_id FROM publications ORDER BY created DESC LIMIT 10').fetchall()
            await message.answer('История публикаций: '+(', '.join(r['status'] for r in rows) if rows else 'пока пуста'));return
        await message.answer('🍎 Apple Release Monitor\n/apple_status — состояние\n/apple_check — проверить\n/apple_monitor_on — включить\n/apple_monitor_off — выключить\n/apple_history — история\n/apple_wallpapers — библиотека\n/apple_wallpaper_add — добавить файл\n/apple_wallpaper_approve — подтвердить\n/apple_recent — последние релизы\n/apple_interval 900 — интервал\n/apple_test — тест в личный чат\n/apple_autopublish_on — включить автопубликацию\n/apple_autopublish_off — отключить автопубликацию')
    async def run(self,shutdown):
        while not shutdown.is_set():
            if self.store.get('enabled',False):
                try:
                    await self.check()
                    await self.publish()
                except Exception as e:
                    self.store.set('last_error',type(e).__name__)
                    log.exception('Apple release worker error: %s',type(e).__name__)
            try: await asyncio.wait_for(shutdown.wait(),timeout=self.interval)
            except asyncio.TimeoutError: pass

def install(dp,bot,admin_ids,data_dir):
    monitor=Monitor(bot,admin_ids,data_dir)
    dp.message.register(monitor.command,Command('apple','apple_status','apple_check','apple_monitor_on','apple_monitor_off','apple_autopublish_off','apple_autopublish_on','apple_test','apple_history','apple_interval','apple_recent','apple_wallpapers','apple_wallpaper_add','apple_wallpaper_approve'))
    return monitor
