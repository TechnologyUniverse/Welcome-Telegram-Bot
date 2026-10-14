import asyncio
import html
import json
import re
import tempfile
import unittest
from pathlib import Path
from storage_updates import Config, State, Manager, RetryDelivery, UncertainDelivery, post, safe_path

class Tests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.now=1000;self.sent=[];self.fail=None
        self.cfg=Config(enabled=True,chat_id=-1001938728395)
        self.state=State(Path(self.tmp.name)/'state.json')
        async def publish(files, now):
            if self.fail: raise self.fail
            self.sent.append(files); return len(self.sent)
        async def verify(f): return not f.get('unavailable')
        self.manager=Manager(self.cfg,self.state,publish,verify,lambda:self.now)
        await self.observe([])
    def f(self,i='one',etag='v1',**kw):return {'id':i,'path':f'Программы для Mac/{i}.dmg','size':100,'etag':etag,**kw}
    async def observe(self,files,dt=0,uploading=False):
        self.now+=dt;await self.manager.observe({'ready':True,'root':'Для канала/','files':files,'uploading':uploading})
    async def complete(self,files):
        await self.observe(files);await self.observe(files,15);await self.observe(files,60)
    async def test_single(self):
        await self.complete([self.f()]);self.assertEqual(len(self.sent),1)
    async def test_five_batched(self):
        await self.complete([self.f(str(i)) for i in range(5)]);self.assertEqual(len(self.sent),1);self.assertEqual(len(self.sent[0]),5)
    async def test_slow_and_stability(self):
        await self.observe([self.f(busy=True)],200)
        await self.observe([self.f()],15);await self.observe([self.f(size=101)],15)
        await self.observe([self.f(size=101)],15);self.assertFalse(self.sent)
        await self.observe([self.f(size=101)],60);self.assertEqual(len(self.sent),1)
    async def test_replace(self):
        await self.complete([self.f()]);await self.complete([self.f(etag='v2')]);self.assertEqual(len(self.sent),2);self.assertTrue(self.sent[-1][0]['updated'])
    async def test_delete_and_rename(self):
        await self.complete([self.f()]);await self.observe([self.f(path='Программы для Mac/new.dmg')],70);await self.observe([],70);self.assertEqual(len(self.sent),1)
    async def test_restart_baseline_and_pending(self):
        await self.observe([self.f()]);await self.observe([self.f()],15)
        self.manager.state=State(self.state.path);self.state=self.manager.state
        await self.observe([self.f()],60);await self.observe([self.f()],120);self.assertEqual(len(self.sent),1)
        self.manager.state=State(self.state.path);await self.observe([self.f()],120);self.assertEqual(len(self.sent),1)
    async def test_retry_definite_and_ambiguous(self):
        self.fail=RetryDelivery(30);await self.complete([self.f()]);self.assertFalse(self.sent)
        self.fail=None;await self.observe([self.f()],31);self.assertEqual(len(self.sent),1)
        self.fail=UncertainDelivery();await self.complete([self.f(etag='v2')]);self.fail=None
        await self.observe([self.f(etag='v2')],500);self.assertEqual(len(self.sent),1);self.assertEqual(self.state.data['outbox'][0]['status'],'uncertain')
    async def test_private(self):
        files=[self.f(str(i),path=p) for i,p in enumerate(['.env','private/x.dmg','Программы для Mac/token.key','backups/db.dump','../secret.dmg','/etc/passwd','folder/.tu-trash/a.dmg','config.json'])]
        await self.complete(files);self.assertFalse(self.sent)
    async def test_disable(self):
        await self.observe([self.f()]);await self.manager.set_enabled(False);await self.observe([self.f()],100)
        await self.manager.set_enabled(True);await self.observe([self.f()],100);await self.observe([self.f()],100);self.assertFalse(self.sent)
    async def test_no_first_run_spam(self):
        self.state.data['baseline']=False;await self.complete([self.f()]);self.assertFalse(self.sent)
    async def test_max_wait(self):
        await self.observe([self.f()]);await self.observe([self.f()],15,True);await self.observe([self.f()],600,True);self.assertEqual(len(self.sent),1)
    async def test_missing_object(self):
        await self.complete([self.f(unavailable=True)]);self.assertFalse(self.sent)
    async def test_no_delete_before_publish(self):
        await self.observe([self.f()]);await self.observe([self.f()],15);await self.observe([],60);self.assertFalse(self.sent)
    async def test_crash_during_send(self):
        self.state.data['outbox']=[{'status':'sending'}];self.state.save()
        self.assertEqual(State(self.state.path).data['outbox'][0]['status'],'uncertain')
    async def test_caption_and_escape(self):
        files=[self.f(str(i),path='Программы для Mac/'+('😀'*100)+'<b>.dmg') for i in range(30)]
        text=post(files,self.now,self.cfg)
        rendered=html.unescape(re.sub('<[^>]*>','',text))
        self.assertLessEqual(len(rendered.encode('utf-16-le'))//2,1024)
        self.assertIn('И ещё 20',text)
        self.assertNotIn('Программы для Mac/',text)
        self.assertIn('&lt;b&gt;',post([self.f(path='a<b>.dmg')],self.now,self.cfg))
    async def test_config_fail_closed(self):
        await self.assertRaisesAsync()
    async def assertRaisesAsync(self):
        with self.assertRaises(ValueError):await self.manager.observe({'ready':True,'root':'private/','files':[]})
    async def test_existing_bot_functions_unchanged(self):
        import ast
        root=Path(__file__).parent
        old=ast.parse((root/'Welcome_Bot.before.py').read_text());new=ast.parse((root/'Welcome_Bot.py').read_text())
        functions=lambda tree:{n.name:ast.dump(n,include_attributes=False) for n in tree.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)) and n.name!='main'}
        self.assertEqual(functions(old),functions(new))
        source=(root/'Welcome_Bot.py').read_text();self.assertLess(source.index('storage_manager = install'),source.index('@dp.message(F.text)'))

if __name__=='__main__':unittest.main()
