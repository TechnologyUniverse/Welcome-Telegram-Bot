import hashlib,json,tempfile,unittest
from pathlib import Path
from PIL import Image
from apple_release.cover import make_cover,verified_wallpaper
from apple_release.core import Release
class Cover(unittest.TestCase):
    def test_reject_checkerboard_image_without_alpha(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d); logo=root/'logo.png'; device=root/'device.png'; config=root/'manifest.json'
            Image.new('RGBA',(300,100),'white').save(logo);Image.new('RGB',(200,100),'white').save(device)
            config.write_text(json.dumps({'device_sha256':hashlib.sha256(device.read_bytes()).hexdigest(),'approved':True,'screens':[{}]}))
            r=Release('iOS','28','RC',1,'A1','2026','https://developer.apple.com/news/releases/?id=x','x')
            with self.assertRaisesRegex(ValueError,'not_transparent'):make_cover([r],logo,device,root/'out.png',config)
    def test_approved_fixture_renders_png_1920_1080(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d); logo=root/'logo.png'; device=root/'device.png'; mask=root/'screen.png'; config=root/'manifest.json'
            Image.new('RGBA',(300,100),(40,80,180,255)).save(logo)
            frame=Image.new('RGBA',(200,100),(0,0,0,0));frame.save(device)
            Image.new('L',(200,100),255).save(mask)
            config.write_text(json.dumps({'device_sha256':hashlib.sha256(device.read_bytes()).hexdigest(),'approved':True,'screens':[{'platform':'iOS','mask':'screen.png','sha256':hashlib.sha256(mask.read_bytes()).hexdigest()}]}))
            r=Release('iOS','28','RC',1,'A1','2026','https://developer.apple.com/news/releases/?id=x','x')
            output=root/'cover.png';make_cover([r],logo,device,output,config)
            with Image.open(output) as im:self.assertEqual((im.format,im.size),('PNG',(1920,1080)))
    def test_wallpaper_needs_provenance(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);Image.new('RGB',(10,10),'red').save(root/'w.png')
            item={'platform':'iOS','generation':28,'file':'w.png','verified':True,'rights':'confirmed','source':'https://example.com/fake','sha256':hashlib.sha256((root/'w.png').read_bytes()).hexdigest()}
            (root/'manifest.json').write_text(json.dumps({'items':[item]}))
            self.assertIsNone(verified_wallpaper(root,'iOS',28))
if __name__=='__main__':unittest.main()
