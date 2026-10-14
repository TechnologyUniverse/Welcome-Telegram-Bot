"""aiogram integration: same Bot instance, no polling/webhook changes."""
import asyncio
import io
import logging
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import aiohttp
from aiogram import F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile
from aiogram.exceptions import TelegramRetryAfter, TelegramNetworkError, TelegramServerError, TelegramBadRequest, TelegramForbiddenError
from storage_updates import Config, State, Manager, RetryDelivery, UncertainDelivery, PermanentDelivery, post


def cover(files, now, config):
    if config.template:
        return Path(config.template).read_bytes()
    from PIL import Image, ImageDraw, ImageFont
    image = Image.new('RGB', (1200, 630), '#0a1025')
    draw = ImageDraw.Draw(image)
    for y in range(630):
        if not config.template:
            draw.line((0, y, 1200, y), fill=(10 + y//35, 16, 37 + y//12))
    draw.rounded_rectangle((48, 44, 1152, 586), radius=30, outline='#465ac2', width=2)
    draw.line((88, 114, 330, 114), fill='#62c7ff', width=5)
    font_path = '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf'
    def font(n): return ImageFont.truetype(font_path, n)
    draw.text((88, 70), 'TECHNOLOGY UNIVERSE', font=font(25), fill='#9ecbff')
    draw.text((82, 190), 'STORAGE UPDATED', font=font(76), fill='#ffffff')
    draw.text((88, 295), 'Новые файлы доступны', font=font(43), fill='#c6b8ff')
    date = datetime.fromtimestamp(now, ZoneInfo(config.timezone)).strftime('%d.%m.%Y')
    draw.text((88, 470), f'Файлов: {len(files)}    •    {date}', font=font(32), fill='#9edaff')
    data = io.BytesIO(); image.save(data, format='JPEG', quality=90)
    return data.getvalue()

class Service:
    def __init__(self, bot, cfg, admin_ids, data_dir):
        self.bot, self.cfg, self.admin_ids = bot, cfg, admin_ids
        self.state = State(Path(data_dir) / 'storage_updates.json')
        self.manager = Manager(cfg, self.state, self.publish, self.verify)
        self.session = None
        self.commands = asyncio.Lock()
        self.command_times = {}

    async def request(self, params=None):
        if not self.cfg.api_url or len(self.cfg.api_token) < 32: raise ValueError('feed_not_configured')
        async with self.session.get(self.cfg.api_url, params=params, headers={'Authorization': 'Bearer ' + self.cfg.api_token}, allow_redirects=False) as r:
            if r.status != 200: raise ValueError('feed_http_' + str(r.status))
            raw = bytearray()
            async for chunk in r.content.iter_chunked(65536):
                raw.extend(chunk)
                if len(raw) > 16 * 1024 * 1024: raise ValueError('feed_too_large')
            import json
            return json.loads(raw)

    async def verify(self, f):
        value = await self.request({'id': f['id']})
        return value.get('available') and value.get('size') == f['size'] and value.get('etag') == f['etag']

    def button(self):
        return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='📥 ОТКРЫТЬ ХРАНИЛИЩЕ', url=self.cfg.public_url)]])

    async def destination(self):
        chat = await self.bot.get_chat(self.cfg.chat_id)
        if chat.type not in {'group', 'supergroup'}: raise PermanentDelivery('destination_not_group')
        me = await self.bot.get_me()
        member = await self.bot.get_chat_member(chat.id, me.id)
        if member.status in {'left', 'kicked'}: raise PermanentDelivery('bot_not_member')
        if member.status not in {'creator', 'administrator'}:
            perms = chat.permissions
            if not perms or not perms.can_send_messages: raise PermanentDelivery('no_send_permission')
            if self.cfg.cover and not perms.can_send_photos: raise PermanentDelivery('no_photo_permission')
        return chat

    async def publish(self, files, now, test=False):
        try:
            await self.destination()  # No publication has been attempted at this stage.
        except TelegramRetryAfter as e: raise RetryDelivery(e.retry_after + 1) from None
        except (TelegramNetworkError, TelegramServerError): raise RetryDelivery(30) from None
        except (TelegramBadRequest, TelegramForbiddenError): raise PermanentDelivery() from None
        text = post(files, now, self.cfg, test)
        art = None
        if self.cfg.cover:
            try: art = await asyncio.to_thread(cover, files, now, self.cfg)
            except Exception: self.state.error('cover_generation_failed', now)
        try:
            if art:
                result = await self.bot.send_photo(self.cfg.chat_id, BufferedInputFile(art, filename='storage-update.jpg'), caption=text, parse_mode='HTML', reply_markup=self.button())
            else:
                result = await self.bot.send_message(self.cfg.chat_id, text, parse_mode='HTML', reply_markup=self.button())
            return result.message_id
        except TelegramRetryAfter as e: raise RetryDelivery(e.retry_after + 1) from None
        except (TelegramNetworkError, TelegramServerError): raise UncertainDelivery() from None
        except (TelegramBadRequest, TelegramForbiddenError): raise PermanentDelivery() from None

    async def allowed(self, message):
        if not message.from_user or message.from_user.id not in self.admin_ids: return False
        if message.chat.id != self.cfg.chat_id and message.chat.type != 'private': return False
        try:
            member = await self.bot.get_chat_member(self.cfg.chat_id, message.from_user.id)
            return member.status in {'creator', 'administrator'}
        except Exception: return False

    async def command(self, message):
        name = message.text.split()[0].split('@')[0]
        if name != '/storage' and not await self.allowed(message):
            await message.answer('Команда доступна только администраторам чата.'); return
        if name == '/storage' and message.chat.id != self.cfg.chat_id and message.chat.type != 'private': return
        key = (message.from_user.id if message.from_user else 0, name)
        now = time.time()
        if now - self.command_times.get(key, 0) < (60 if name == '/storage_test' else 3): return
        if len(self.command_times) > 2000: self.command_times.clear()
        self.command_times[key] = now
        async with self.commands:
            d = self.state.data
            if name == '/storage':
                last = self.date(d['last_publish'])
                await message.answer(f'📦 <b>Хранилище Technology Universe</b>\nПоследнее обновление: {last}', reply_markup=self.button())
            elif name == '/storage_enable':
                try:
                    await self.destination()
                    snapshot = await self.request()
                    if not snapshot.get('ready') or snapshot.get('root') != self.cfg.watch_path: raise ValueError()
                    await self.manager.set_enabled(True)
                    await self.manager.observe(snapshot)
                except Exception:
                    await self.manager.set_enabled(False)
                    await message.answer('Не удалось включить модуль: проверьте чат, права и подключение хранилища.'); return
                await message.answer('Автоуведомления включены. Исходный снимок создан без публикации старых файлов.')
            elif name == '/storage_disable':
                await self.manager.set_enabled(False)
                await message.answer('Автоматические уведомления отключены.')
            elif name == '/storage_status':
                unresolved = sum(j['status'] in {'failed', 'uncertain'} for j in d['outbox'])
                await message.answer(f"Мониторинг: {'включён' if self.manager.enabled else 'выключен'}\nСостояние: {self.manager.health}\nПроверка: {self.date(d['last_check'])}\nПубликация: {self.date(d['last_publish'])}\nНужна проверка доставки: {unresolved}")
            elif name == '/storage_history':
                lines = [f"{self.date(h['published'])} · {h['count']} файлов · message_id {h['message_id']}" for h in d['history'][-10:][::-1]]
                await message.answer('📜 Последние публикации\n' + ('\n'.join(lines) or 'История пуста'))
            elif name == '/storage_test':
                try:
                    async with self.manager.lock:
                        mid = await self.publish([{'path':'Демонстрационный файл', 'size':0}], now, True)
                    await message.answer(f'Тестовая публикация отправлена в чат. Message ID: {mid}')
                except Exception:
                    await message.answer('Доставка не подтверждена. Проверьте чат перед повтором.'); self.state.error('test_delivery_failed', now)

    def date(self, value):
        return datetime.fromtimestamp(value, ZoneInfo(self.cfg.timezone)).strftime('%d.%m.%Y %H:%M') if value else 'ещё нет'

    async def run(self, stop):
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            self.session = session
            while not stop.is_set():
                try:
                    if self.cfg.api_url:
                        await self.manager.observe(await self.request())
                    else: self.manager.health = 'not_configured'
                except asyncio.CancelledError: raise
                except Exception as e:
                    self.manager.health = 'error'
                    self.state.error(type(e).__name__, time.time())
                    # Optional admin alerts go only to explicitly configured owner IDs, not a channel.
                    if self.cfg.admin_alerts and time.time() - getattr(self, 'last_alert', 0) > 3600:
                        self.last_alert = time.time()
                        for user_id in self.admin_ids:
                            try: await self.bot.send_message(user_id, 'Storage Update Manager: ошибка мониторинга. /storage_status')
                            except Exception: pass
                try: await asyncio.wait_for(stop.wait(), self.cfg.poll)
                except asyncio.TimeoutError: pass
            self.session = None


def install(dispatcher, bot, admin_ids, data_dir):
    try:
        service = Service(bot, Config.environment(), admin_ids, data_dir)
    except Exception:
        logging.error('Storage Update Manager initialization failed; existing bot remains active')
        return None
    dispatcher.message.register(service.command, Command('storage', 'storage_status', 'storage_enable', 'storage_disable', 'storage_test', 'storage_history'))
    return service
