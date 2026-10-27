import asyncio,tempfile,unittest
from types import SimpleNamespace
from apple_release.adapter import Monitor
class Message:
    def __init__(self,user,text,chat_type='private'):
        self.from_user=SimpleNamespace(id=user);self.chat=SimpleNamespace(id=user,type=chat_type);self.text=text;self.answers=[]
    async def answer(self,text):self.answers.append(text)
class Adapter(unittest.IsolatedAsyncioTestCase):
    async def test_admin_only_and_safe_defaults(self):
        with tempfile.TemporaryDirectory() as d:
            m=Monitor(SimpleNamespace(),{11},d);m.store.set('enabled',False)
            outsider=Message(22,'/apple_monitor_on');await m.command(outsider)
            self.assertFalse(m.store.get('enabled',False));self.assertEqual(outsider.answers,[])
            owner=Message(11,'/apple_monitor_on');await m.command(owner)
            self.assertTrue(m.store.get('enabled',False));self.assertFalse(m.store.get('auto_publish',False))
            owner.text='/apple_autopublish_on';await m.command(owner)
            self.assertFalse(m.store.get('auto_publish',False))
            owner.text='/apple_test';await m.command(owner)
            self.assertFalse(m.store.get('preview_verified',False));m.store.close()
    async def test_private_only(self):
        with tempfile.TemporaryDirectory() as d:
            m=Monitor(SimpleNamespace(),{11},d);m.store.set('enabled',False);msg=Message(11,'/apple_monitor_on','supergroup')
            await m.command(msg);self.assertFalse(m.store.get('enabled',False));m.store.close()
if __name__=='__main__':unittest.main()

class Publisher(unittest.IsolatedAsyncioTestCase):
    async def test_success_and_ambiguous_delivery(self):
        from unittest.mock import AsyncMock
        from apple_release.core import Release
        for error,expected in [(None,'published'),(TimeoutError(),'manual_review_required')]:
            with self.subTest(expected=expected),tempfile.TemporaryDirectory() as d:
                bot=SimpleNamespace(get_me=AsyncMock(return_value=SimpleNamespace(id=12)),get_chat=AsyncMock(return_value=SimpleNamespace(type='supergroup')),get_chat_member=AsyncMock(return_value=SimpleNamespace(status='administrator',can_post_messages=True)),send_photo=AsyncMock(return_value=SimpleNamespace(message_id=101),side_effect=error))
                m=Monitor(bot,{11},d);m.target='-100123';m.window=0;m.render=lambda _:b'png';m.store.set('auto_publish',True);m.store.set('preview_verified',True)
                r=Release('iOS','27','RELEASE',None,'A1','2026','https://developer.apple.com/news/releases/?id=old','old')
                n=Release('iOS','28','RC',1,'A2','2026','https://developer.apple.com/news/releases/?id=new','new')
                m.store.observe([r],1);m.store.observe([n],2);await m.publish();await m.publish()
                self.assertEqual(m.store.db.execute('SELECT status FROM publications').fetchone()[0],expected)
                self.assertEqual(bot.send_photo.await_count,1);m.store.close()

    async def test_telegram_rejected_and_rate_limited_are_not_marked_sent(self):
        from unittest.mock import AsyncMock
        from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter
        from aiogram.methods import SendPhoto
        from apple_release.core import Release
        method=SendPhoto(chat_id=1,photo='x')
        for error in [TelegramBadRequest(method,'bad request'),TelegramRetryAfter(method,'slow down',10)]:
            with self.subTest(error=type(error).__name__),tempfile.TemporaryDirectory() as d:
                bot=SimpleNamespace(get_me=AsyncMock(return_value=SimpleNamespace(id=12)),get_chat=AsyncMock(return_value=SimpleNamespace(type='supergroup')),get_chat_member=AsyncMock(return_value=SimpleNamespace(status='administrator',can_post_messages=True)),send_photo=AsyncMock(side_effect=error))
                m=Monitor(bot,{11},d);m.target='-100123';m.window=0;m.render=lambda _:b'png';m.store.set('auto_publish',True);m.store.set('preview_verified',True)
                r=Release('iOS','27','RELEASE',None,'A1','2026','https://developer.apple.com/news/releases/?id=old','old')
                n=Release('iOS','28','RC',1,'A2','2026','https://developer.apple.com/news/releases/?id=new','new')
                m.store.observe([r],1);m.store.observe([n],2);await m.publish();await m.publish()
                self.assertEqual(m.store.db.execute('SELECT status FROM publications').fetchone()[0],'failed')
                self.assertEqual(bot.send_photo.await_count,1);m.store.close()
