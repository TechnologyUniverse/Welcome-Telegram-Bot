import unittest
from apple_release.core import Release, message_for
from apple_release.notes import extract, page_for

def release(platform='iOS',version='27.2',kind='RELEASE',build='a'):
    return Release(platform,version,kind,None,build,'','','')

class NotesTest(unittest.TestCase):
    def test_exact_version_and_platform_page(self):
        raw=b'<h2>iOS 27.2</h2><p>Section heading</p><ul><li>A verified improvement for the correct version.</li></ul><h2>iOS 27.1</h2><ul><li>Old improvement should not appear.</li></ul>'
        self.assertEqual(extract(raw,release()),['A verified improvement for the correct version.'])
        self.assertEqual(extract(raw,release(version='27.3')),[])
        self.assertIsNone(page_for(release(kind='RC')))
    def test_notes_under_correct_system_and_fallback(self):
        one=release(); two=release('watchOS',build='b')
        text=message_for([one,two],{one.key:['A verified improvement for the correct version.']})
        self.assertLess(text.index('iOS 27.2'),text.index('A verified improvement'))
        self.assertLess(text.index('A verified improvement'),text.index('watchOS 27.2'))
        self.assertIn('недоступны',text)
    def test_long_groups_stay_within_caption_limit(self):
        releases=[release(p,build=p) for p in ('iOS','iPadOS','macOS','watchOS','visionOS','tvOS','HomePod Software')]
        text=message_for(releases,{r.key:['A'*130] for r in releases})
        self.assertLessEqual(len(text.encode('utf-16-le'))//2,1024)
