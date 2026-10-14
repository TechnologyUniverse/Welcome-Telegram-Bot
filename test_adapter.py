import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch
from types import SimpleNamespace
from storage_adapter import Service
from storage_updates import Config, PermanentDelivery

class AdapterTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.bot=SimpleNamespace(get_chat=AsyncMock(return_value=SimpleNamespace(type='supergroup',id=-1001938728395)),get_me=AsyncMock(return_value=SimpleNamespace(id=99)),get_chat_member=AsyncMock(return_value=SimpleNamespace(status='administrator')),send_message=AsyncMock(return_value=SimpleNamespace(message_id=42)),send_photo=AsyncMock(return_value=SimpleNamespace(message_id=43)))
        self.s=Service(self.bot,Config(chat_id=-1001938728395),{123},self.tmp.name)
    async def test_channel_never_receives(self):
        self.bot.get_chat.return_value.type='channel'
        with self.assertRaises(PermanentDelivery):await self.s.publish([{'path':'x.dmg','size':1}],1000)
        self.bot.send_message.assert_not_awaited();self.bot.send_photo.assert_not_awaited()
    async def test_cover_failure_text_fallback(self):
        with patch('storage_adapter.cover',side_effect=RuntimeError()):
            self.assertEqual(await self.s.publish([{'path':'x.dmg','size':1}],1000),42)
        self.bot.send_message.assert_awaited_once();self.assertEqual(self.bot.send_message.call_args.args[0],-1001938728395)
    async def test_photo_uses_existing_bot(self):
        with patch('storage_adapter.cover',return_value=b'art'):
            self.assertEqual(await self.s.publish([{'path':'x.dmg','size':1}],1000),43)
        self.bot.send_photo.assert_awaited_once()
    async def test_both_id_and_group_admin_required(self):
        m=SimpleNamespace(from_user=SimpleNamespace(id=123),chat=SimpleNamespace(type='private',id=123))
        self.assertTrue(await self.s.allowed(m))
        self.bot.get_chat_member.return_value.status='member';self.assertFalse(await self.s.allowed(m))
        m.from_user.id=999;self.assertFalse(await self.s.allowed(m))
    async def test_restart_persisted_disable(self):
        await self.s.manager.set_enabled(False)
        other=Service(self.bot,Config(enabled=True,chat_id=-1001938728395),{123},self.tmp.name)
        self.assertFalse(other.manager.enabled)

    async def test_chunked_feed_response(self):
        class Content:
            async def iter_chunked(self, size):
                for chunk in [b'{"ready":', b'true,"files":', b'[]}']:
                    yield chunk
        class Response:
            status = 200
            content = Content()
            async def __aenter__(self): return self
            async def __aexit__(self, *args): pass
        self.s.cfg.api_url = 'https://example.com/feed'
        self.s.cfg.api_token = 'x' * 48
        self.s.session = SimpleNamespace(get=lambda *a, **kw: Response())
        self.assertEqual(await self.s.request(), {'ready': True, 'files': []})
