import json
import tempfile
import unittest
from pathlib import Path
from apple_release.core import Release, ReleaseStore, message_for, parse_feed

def feed(*titles):
    data=''.join(f'<item><title>{title}</title><link>https://developer.apple.com/news/releases/?id={i}</link><guid>id-{i}</guid><pubDate>Mon, 26 Oct 2026 10:00:00 GMT</pubDate></item>' for i,title in enumerate(titles))
    return ('<rss><channel>'+data+'</channel></rss>').encode()

class Parsing(unittest.TestCase):
    def test_rc_final_and_beta_future_platforms(self):
        values=parse_feed(feed('iOS 28.1 RC (24B123)','macOS 29 RC 2 (25A234)','iPadOS 28.1 (24B123)','watchOS 30 beta 1 (30A1)','tvOS 28 Public Beta (A1)','visionOS 27.2 (24N120)','HomePod Software 28.0 (24X1)'),now=2_000_000_000)
        self.assertEqual([(x.platform,x.version,x.kind,x.rc) for x in values],[('iOS','28.1','RC',1),('macOS','29','RC',2),('iPadOS','28.1','RELEASE',None),('visionOS','27.2','RELEASE',None),('HomePod Software','28.0','RELEASE',None)])
        self.assertEqual(len({x.key for x in values}),5)
        self.assertNotIn('http',message_for(values))
    def test_reject_invalid_and_unverified(self):
        with self.assertRaises(ValueError):parse_feed(b'<!DOCTYPE foo [<!ENTITY x "hack">]><rss/>')
        malicious=feed('iOS 28.0 (24A123)').replace(b'developer.apple.com',b'evil.example.com')
        self.assertEqual(parse_feed(malicious,now=2_000_000_000),[])
    def test_missing_build_uses_source_identity(self):
        a=Release('iOS','28','RC',1,None,'2026-10-26','https://developer.apple.com/news/releases/?id=a','id-a')
        b=Release('iOS','28','RC',1,None,'2026-10-26','https://developer.apple.com/news/releases/?id=b','id-b')
        self.assertNotEqual(a.key,b.key)
class DurableState(unittest.TestCase):
    def test_baseline_group_dedup_and_restart_uncertainty(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'state.sqlite';s=ReleaseStore(p)
            old=parse_feed(feed('iOS 27.0 (24A1)'),now=2_000_000_000)
            new=parse_feed(feed('iOS 28 RC (24B1)','macOS 29 (25A1)'),now=2_000_000_000)
            self.assertTrue(s.observe(old,now=100));self.assertFalse(s.observe(old+new,now=200))
            self.assertFalse(s.observe(old+new,now=201));self.assertIsNone(s.reserve(window=30,now=229))
            group,items=s.reserve(window=30,now=230);self.assertEqual(len(items),2);self.assertIsNone(s.reserve(window=30,now=230));s.close()
            s=ReleaseStore(p)
            self.assertEqual(s.db.execute('SELECT status FROM publications WHERE id=?',(group,)).fetchone()[0],'manual_review_required')
            self.assertEqual(s.db.execute("SELECT count(*) FROM releases WHERE status='manual_review_required'").fetchone()[0],2)
            self.assertIsNone(s.reserve(window=0,now=500));s.close()
    def test_sent_persisted(self):
        with tempfile.TemporaryDirectory() as tmp:
            s=ReleaseStore(Path(tmp)/'state.sqlite');s.observe(parse_feed(feed('iOS 27 (1A1)'),now=2_000_000_000),now=100)
            s.observe(parse_feed(feed('iOS 28 (1A2)'),now=2_000_000_000),now=200)
            group,_=s.reserve(0,200);s.finish(group,'published',123)
            self.assertEqual(s.db.execute('SELECT message_id FROM publications').fetchone()[0],123);s.close()
if __name__=='__main__':unittest.main()
