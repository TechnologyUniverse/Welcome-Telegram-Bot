"""Compatibility worker for Welcome Bot 1.6.1. Never starts Telegram polling.

Default SHADOW=true: no Telegram writes, storage polling, registry mutations,
or legacy dispatch. Production requires an explicit cutover.
"""
import asyncio
import importlib.util
import json
import os
from pathlib import Path
import secrets
import signal
import time
from aiohttp import web
from aiogram.types import Update, Message, ChatMemberUpdated
from aiogram.exceptions import TelegramRetryAfter, TelegramBadRequest, TelegramForbiddenError
from queue_store import Store

SHADOW = os.getenv('SHADOW', 'true').lower() != 'false'
SECRET = os.environ['BRIDGE_SECRET']
if len(SECRET) < 48:
    raise RuntimeError('Bridge secret must contain at least 48 characters')
spec = importlib.util.spec_from_file_location('legacy', '/app/Welcome_Bot.py')
legacy = importlib.util.module_from_spec(spec)
import sys
sys.path.insert(0, '/app')
sys.modules['legacy'] = legacy
spec.loader.exec_module(legacy)
legacy.load_user_registry()
TARGET = int(os.environ['STORAGE_NOTIFICATION_CHAT_ID'])
TEST_CHAT_ID = int(os.getenv('TEST_CHAT_ID', '0'))
DATA = Path(os.environ.get('BRIDGE_DATA_DIR', '/bridge-data'))
DATA.mkdir(parents=True, exist_ok=True)
os.chmod(DATA, 0o700)
store = Store(DATA / 'queue.sqlite')
lock = asyncio.Lock()


def validate_update(raw):
    update = Update.model_validate(raw)
    obj = update.message or update.chat_member or update.callback_query
    chat = getattr(obj, 'chat', None) or getattr(getattr(obj, 'message', None), 'chat', None)
    if not chat or (chat.id != TARGET and chat.type != 'private'):
        raise web.HTTPForbidden()
    if TEST_CHAT_ID and (chat.type != 'private' or chat.id != TEST_CHAT_ID):
        raise web.HTTPForbidden()
    if chat.type == 'private':
        user = getattr(obj, 'from_user', None)
        if not user or (TEST_CHAT_ID and not legacy.is_admin(user.id)):
            raise web.HTTPForbidden()
    return update, obj, chat


def make_plan(key, kind, chat, **extra):
    return {'key':key, 'kind':kind, 'test':legacy.is_test_mode(), **extra}


async def prepare(request):
    raw = (await request.json()).get('update')
    try:
        update, obj, chat = validate_update(raw)
    except web.HTTPForbidden:
        # A valid Telegram update outside our chat scope is acknowledged and
        # ignored; rejecting it would make Telegram retry it indefinitely.
        return web.json_response({'plans':[], 'mode':'ignored'})
    except (ValueError, TypeError):
        raise web.HTTPBadRequest()
    async with lock:
        with store.atomic():
            cached = store.cached(update.update_id)
            if cached is not None:
                return web.json_response(cached)
            plans = []
            legacy_dispatch = False
            if update.message and update.message.new_chat_members or update.chat_member:
                if update.chat_member:
                    event = update.chat_member
                    if event.old_chat_member.status not in {'left','kicked'} or event.new_chat_member.status != 'member':
                        store.cache(update.update_id, {'plans':[], 'mode':'shadow' if SHADOW else 'live'})
                        return web.json_response({'plans':[]})
                    users = [event.new_chat_member.user]
                    source = legacy.detect_join_source_from_member_event(event)
                else:
                    users = obj.new_chat_members
                    source = legacy.detect_join_source_from_message(obj)
                    if not SHADOW:
                        perms = await legacy.bot_has_permissions(chat.id)
                        if perms['delete']:
                            store.queue_delete(f'service:{update.update_id}', chat.id, obj.message_id)
                for user in users:
                    if user.is_bot or not store.claim('welcome:' + str(user.id), 300):
                        continue
                    if not SHADOW:
                        record = legacy.USER_REGISTRY.get(user.id)
                        if record is None:
                            labels = {source + '_member'} if source in {'paid','discord'} else set()
                            legacy.USER_REGISTRY[user.id] = {'source':source,'labels':labels,'first_seen':time.time(),'chat_id':chat.id}
                            legacy.save_user_registry()
                        elif update.chat_member and source == 'paid':
                            record['source'] = source
                            record['labels'].add('paid_member')
                            legacy.save_user_registry()
                        perms = await legacy.bot_has_permissions(chat.id)
                        await legacy.apply_mute_if_needed(chat.id, user.id, source, perms, legacy.is_paid_like_chat(chat))
                    if not legacy.FEATURE_WELCOME_ENABLED:
                        continue
                    cfg = legacy.CFG
                    key = f'welcome:{update.update_id}:{user.id}'
                    auto = legacy.FEATURE_AUTODELETE_ENABLED and (bool(update.chat_member) or not legacy.is_paid_like_chat(chat))
                    store.intent(key, chat.id, cfg.welcome_delay_seconds if update.message else 0, legacy.get_message_ttl('welcome') if auto else None)
                    invite = getattr(obj, 'invite_link', None)
                    plans.append(make_plan(key,'welcome',chat,user=user.model_dump(mode='json'),source=source,language=user.language_code,
                        project=cfg.project_name,storage_url=cfg.storage_url,faq_url=cfg.faq_url,support_url=cfg.support_url,
                        invite_url=invite.invite_link if invite else None,photo=cfg.welcome_image_url if update.message else None))
            elif update.callback_query and (obj.data or '').split(':')[0] in {'rules','about'}:
                kind, _, language = obj.data.partition(':')
                if kind != 'rules' or store.claim('rules:' + str(obj.from_user.id),300):
                    key = f'{kind}:{update.update_id}'
                    store.intent(key,chat.id,0,legacy.get_message_ttl(kind))
                    plans.append(make_plan(key,kind,chat,language=language))
            else:
                legacy_dispatch = True
            result = {'plans':plans, 'mode':'shadow' if SHADOW else 'live'}
            if legacy_dispatch and not SHADOW and not TEST_CHAT_ID:
                # Mark before the call. Network ambiguity must never repeat admin toggles
                # or payment-adjacent commands on a webhook retry.
                store.cache(update.update_id, {'plans':[], 'mode':'uncertain_legacy'})
                store.checkpoint()
                if update.message and (obj.text or '').split('@')[0].split(' ')[0] in {
                    '/storage','/storage_status','/storage_history','/storage_enable','/storage_disable','/storage_test'}:
                    await legacy.storage_manager.command(obj.as_(legacy.bot))
                else:
                    await legacy.dp.feed_update(legacy.bot,update)
            store.cache(update.update_id,result)
            return web.json_response(result)


async def enqueue(request):
    data = await request.json()
    content = data.get('content',{})
    text = content.get('text')
    if not isinstance(text,str) or not text or len(text.encode('utf-16-le')) // 2 > (1024 if content.get('photo') else 4096):
        raise web.HTTPBadRequest(text='invalid_message_length')
    if set(content) - {'text','reply_markup','photo'}:
        raise web.HTTPBadRequest()
    async with lock:
        try:
            store.enqueue(data['key'],content)
        except (KeyError,ValueError):
            raise web.HTTPConflict()
    return web.json_response({'queued':True,'mode':'shadow' if SHADOW else 'live'})


async def deliver():
    while True:
        if not SHADOW:
            async with lock:
                for job in store.pending(time.time()):
                    content = json.loads(job['content'])
                    store.mark(job['key'],'sending')
                    try:
                        params = {'chat_id':job['chat'],'parse_mode':'HTML','reply_markup':content.get('reply_markup')}
                        if content.get('photo'):
                            sent = await legacy.bot.send_photo(photo=content['photo'],caption=content['text'],**params)
                        else:
                            sent = await legacy.bot.send_message(text=content['text'],**params)
                    except TelegramRetryAfter as e:
                        store.mark(job['key'],'queued',due=time.time() + e.retry_after + 1,error='rate_limited')
                    except (TelegramBadRequest,TelegramForbiddenError):
                        store.mark(job['key'],'failed',error='telegram_rejected')
                    except Exception:
                        store.mark(job['key'],'uncertain',error='telegram_delivery_uncertain')
                    else:
                        store.mark(job['key'],'sent',message_id=sent.message_id,delete_due=time.time() + job['ttl'] if job['ttl'] is not None else None)
                for job in store.deletions(time.time()):
                    try:
                        await legacy.bot.delete_message(job['chat'],job['message_id'])
                    except TelegramRetryAfter as e:
                        store.mark(job['key'],'sent',delete_due=time.time() + e.retry_after + 1)
                    except (TelegramBadRequest,TelegramForbiddenError):
                        store.mark(job['key'],'delete_failed',error='delete_rejected')
                    except Exception:
                        store.mark(job['key'],'sent',delete_due=time.time()+30)
                    else:
                        store.mark(job['key'],'deleted')
        await asyncio.sleep(1)


@web.middleware
async def authenticate(request, handler):
    if request.path == '/health' and request.method == 'GET':
        return web.json_response({'ok':True,'mode':'shadow' if SHADOW else ('live-test' if TEST_CHAT_ID else 'live'),'queue':store.stats()})
    if not secrets.compare_digest(request.headers.get('Authorization',''),'Bearer ' + SECRET):
        raise web.HTTPUnauthorized()
    if request.content_type != 'application/json':
        raise web.HTTPUnsupportedMediaType()
    return await handler(request)


async def lifecycle(app):
    tasks = [asyncio.create_task(deliver())]
    if not SHADOW and not TEST_CHAT_ID:
        tasks += [asyncio.create_task(legacy.cleanup_bot_messages()),asyncio.create_task(legacy.cleanup_caches()),asyncio.create_task(legacy.write_health_heartbeat())]
        if legacy.storage_manager:
            tasks.append(asyncio.create_task(legacy.storage_manager.run(legacy.shutdown_event)))
        if legacy.apple_monitor:
            tasks.append(asyncio.create_task(legacy.apple_monitor.run(legacy.shutdown_event)))
    yield
    legacy.shutdown_event.set()
    for task in tasks:task.cancel()
    await asyncio.gather(*tasks,return_exceptions=True)
    await legacy.bot.session.close()
    store.db.close()


app = web.Application(middlewares=[authenticate],client_max_size=128*1024)
app.add_routes([web.get('/health',lambda request: web.json_response({'ok':True})),web.post('/prepare',prepare),web.post('/enqueue',enqueue)])
app.cleanup_ctx.append(lifecycle)
if __name__ == '__main__':
    web.run_app(app,host='0.0.0.0',port=8098,access_log=None)
