import io,tempfile,unittest
from pathlib import Path
from PIL import Image
from apple_release import library
from apple_release.cover import verified_wallpaper
class Library(unittest.TestCase):
    def test_upload_needs_verification_and_preserves_metadata(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d); raw=io.BytesIO();Image.new('RGB',(64,64),'blue').save(raw,'PNG')
            item=library.add(root,'iOS',27,'https://developer.apple.com/wwdc26/wallpaper/','Разрешение проверено',raw.getvalue())
            self.assertFalse(item['verified']);self.assertIsNone(verified_wallpaper(root,'iOS',27))
            library.approve(root,item['sha256']);self.assertIsNotNone(verified_wallpaper(root,'iOS',27));self.assertIsNone(verified_wallpaper(root,'iOS',28))
            (root/item['file']).write_bytes(b'bad')
            with self.assertRaises(ValueError):verified_wallpaper(root,'iOS',27)
    def test_invalid_source_and_image_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(ValueError):library.add(Path(d),'iOS',27,'https://evil.example/x','ok',b'not-image')
            with self.assertRaises(ValueError):library.add(Path(d),'iOS',27,'https://apple.com/x','ok',b'not-image')
if __name__=='__main__':unittest.main()
