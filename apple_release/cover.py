"""Render covers only from an explicitly approved transparent device template."""
import hashlib
import io
import json
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont, ImageOps
from urllib.parse import urlparse

MAX_IMAGE = 15_000_000

def checked_asset(path: Path, expected_hash: str) -> bytes:
    if not path.is_file() or path.stat().st_size > MAX_IMAGE: raise ValueError('asset_missing_or_too_large')
    raw=path.read_bytes()
    if hashlib.sha256(raw).hexdigest()!=expected_hash: raise ValueError('asset_checksum_mismatch')
    im=Image.open(io.BytesIO(raw)); im.verify()
    return raw

def verified_wallpaper(root: Path, platform: str, generation: int):
    manifest=root/'manifest.json'
    if not manifest.is_file(): return None
    for item in reversed(json.loads(manifest.read_text()).get('items',[])):
        if (item.get('platform') != platform or item.get('generation') != generation or item.get('verified') is not True or not item.get('rights') or urlparse(item.get('source','')).scheme != 'https' or urlparse(item.get('source','')).hostname not in {'apple.com','www.apple.com','developer.apple.com','support.apple.com'}): continue
        path=(root/item['file']).resolve()
        if not path.is_relative_to(root.resolve()): raise ValueError('invalid_asset_path')
        return Image.open(io.BytesIO(checked_asset(path,item['sha256']))).convert('RGB')
    return None

def make_cover(releases, logo: Path, approved_device: Path, output: Path, manifest: Path):
    # Device and mask images must be approved together by the operator.
    config=json.loads(manifest.read_text())
    devices=Image.open(io.BytesIO(checked_asset(approved_device,config['device_sha256']))).convert('RGBA')
    if devices.getchannel('A').getextrema()[0] != 0: raise ValueError('device_template_not_transparent')
    if not config.get('approved') or not config.get('screens'): raise ValueError('device_masks_unapproved')
    from .core import Release
    if not releases or any(not isinstance(r,Release) for r in releases): raise ValueError('invalid_releases')
    background=Path(__file__).resolve().parent.parent/'assets/apple_release/earth-original.png'
    if background.is_file():
        canvas=ImageOps.fit(Image.open(background).convert('RGB'),(1920,1080),method=Image.Resampling.LANCZOS)
    else:
        canvas=Image.new('RGB',(1920,1080),'#061329')
    draw=ImageDraw.Draw(canvas)
    logo_im=Image.open(logo).convert('RGBA');logo_im.thumbnail((1150,220))
    canvas.paste(logo_im,((1920-logo_im.width)//2,18),logo_im)
    # Approved masks include screen placement, and wallpapers must come from the verified local library.
    screens=Image.new('RGBA',devices.size,(0,0,0,0))
    for spec in config['screens']:
        mask_path=(manifest.parent/spec['mask']).resolve()
        if not mask_path.is_relative_to(manifest.parent.resolve()): raise ValueError('invalid_mask_path')
        mask=Image.open(io.BytesIO(checked_asset(mask_path,spec['sha256']))).convert('L')
        if mask.size != devices.size: raise ValueError('mask_size_mismatch')
        relevant=next((r for r in releases if r.platform==spec['platform']),None)
        wallpaper=verified_wallpaper(manifest.parent/'apple_wallpapers',spec['platform'],int(relevant.version.split('.')[0])) if relevant else None
        box=mask.getbbox()
        if not box: continue
        art=ImageOps.fit(wallpaper or Image.new('RGB',(100,100),'#102941'),(box[2]-box[0],box[3]-box[1]))
        layer=Image.new('RGBA',devices.size);layer.paste(art,box[:2]);screens.paste(layer,(0,0),mask)
    # Place image under the device frame; transparent areas of the display reveal the artwork.
    combined=Image.alpha_composite(screens,devices);combined.thumbnail((1780,650))
    canvas.paste(combined,((1920-combined.width)//2,220),combined)
    paths=['/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf','/System/Library/Fonts/Supplemental/Arial Bold.ttf']
    f=next((candidate for candidate in paths if Path(candidate).is_file()),None)
    if not f: raise ValueError('required_font_missing')
    font=ImageFont.truetype(f,66); sub=ImageFont.truetype(f,42)
    versions=' / '.join(dict.fromkeys(r.version for r in releases));typ='RELEASE CANDIDATE' if all(r.kind=='RC' for r in releases) else 'OFFICIAL RELEASE' if all(r.kind=='RELEASE' for r in releases) else 'APPLE SOFTWARE UPDATES'
    draw.text((960,890),f'Apple Software  {versions}',font=font,fill='white',anchor='mm');draw.text((960,985),typ,font=sub,fill='#8bd4ff',anchor='mm')
    output.parent.mkdir(parents=True,exist_ok=True);canvas.save(output,'PNG',optimize=True)
