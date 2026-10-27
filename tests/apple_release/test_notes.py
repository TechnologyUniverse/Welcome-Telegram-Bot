import unittest
from apple_release.core import Release, message_for
from apple_release.notes import extract, page_for

def release(platform='iOS',version='27.2',kind='RELEASE',build='a'):
    return Release(platform,version,kind,None,build,'','','')

class NotesTest(unittest.TestCase):
    def test_previous_generations_have_separate_sources(self):
        for platform, version, article in [('iOS','18.7','121161'), ('iPadOS','18.7','121162'),
             ('macOS','15.7','120283'), ('watchOS','11.6','121163'), ('visionOS','2.6','121164'),
             ('iOS','26.7','123075'), ('iPadOS','26.7','123074'), ('macOS','26.7','122868'),
             ('watchOS','26.6','123002'), ('visionOS','26.6','123024')]:
            with self.subTest(platform=platform, version=version):
                self.assertTrue(page_for(release(platform,version)).endswith(article))
        self.assertIsNone(page_for(release('macOS','18.1')))
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
