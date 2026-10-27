"""Local, administrator-managed image library with bounded uploads and provenance."""
import hashlib
import io
import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from PIL import Image, UnidentifiedImageError

MAX_BYTES=12_000_000
MAX_PIXELS=30_000_000
PLATFORMS={'iOS','iPadOS','macOS','watchOS','tvOS','visionOS'}
ALLOWED_HOSTS={'www.apple.com','apple.com','developer.apple.com','support.apple.com'}

def inspect_image(raw):
    if not raw or len(raw)>MAX_BYTES:raise ValueError('Размер изображения должен быть не более 12 МБ.')
    try:
        with Image.open(io.BytesIO(raw)) as im:
            if im.format not in ('PNG','JPEG','WEBP'):raise ValueError('Допустимы PNG, JPEG и WebP.')
            w,h=im.size
            if w<32 or h<32 or w*h>MAX_PIXELS:raise ValueError('Недопустимый размер изображения.')
            im.verify()
    except (OSError,UnidentifiedImageError,Image.DecompressionBombError):raise ValueError('Изображение повреждено или слишком велико.') from None
    return w,h

def add(root,platform,generation,source,rights,raw):
    if platform not in PLATFORMS or not 1<=generation<=999:raise ValueError('Неверная платформа или поколение.')
    try:u=urlparse(source); valid=u.scheme=='https' and u.hostname in ALLOWED_HOSTS and not u.username and not u.password and u.port in (None,443)
    except ValueError:valid=False
    if not valid:raise ValueError('Нужен HTTPS-адрес официального источника Apple.')
    if not rights.strip() or len(rights)>500:raise ValueError('Укажите сведения о разрешении на использование.')
    width,height=inspect_image(raw);digest=hashlib.sha256(raw).hexdigest();root.mkdir(parents=True,exist_ok=True)
    filename=f'{platform.lower()}-{generation}-{digest[:20]}.img';target=root/filename
    if not target.exists():
        fd,tmp=tempfile.mkstemp(prefix='.upload-',dir=root)
        try:
            with os.fdopen(fd,'wb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
            os.replace(tmp,target)
        finally:
            if os.path.exists(tmp):os.unlink(tmp)
    path=root/'manifest.json';data=json.loads(path.read_text()) if path.exists() else {'items':[]}
    item={'platform':platform,'generation':generation,'file':filename,'source':source,'rights':rights.strip(),'verified':False,'sha256':digest,'width':width,'height':height,'added_at':datetime.now(timezone.utc).isoformat()}
    data['items']=[x for x in data.get('items',[]) if (x.get('platform'),x.get('generation'),x.get('sha256'))!=(platform,generation,digest)]+[item]
    fd,tmp=tempfile.mkstemp(prefix='.manifest-',dir=root)
    try:
        with os.fdopen(fd,'w') as f:json.dump(data,f,ensure_ascii=False,indent=2);f.flush();os.fsync(f.fileno())
        os.replace(tmp,path)
    finally:
        if os.path.exists(tmp):os.unlink(tmp)
    return item

def list_items(root):
    path=root/'manifest.json'
    return json.loads(path.read_text()).get('items',[]) if path.exists() else []

def approve(root,digest):
    path=root/'manifest.json';data=json.loads(path.read_text()) if path.exists() else {'items':[]}
    matches=[x for x in data['items'] if x.get('sha256')==digest]
    if len(matches)!=1:raise ValueError('Укажите полный SHA-256 файла из списка.')
    item=matches[0];target=(root/item['file']).resolve()
    if not target.is_relative_to(root.resolve()):raise ValueError('Недопустимый путь файла.')
    inspect_image(target.read_bytes())
    if hashlib.sha256(target.read_bytes()).hexdigest()!=digest:raise ValueError('Контрольная сумма не совпадает.')
    item['verified']=True;item['verified_at']=datetime.now(timezone.utc).isoformat()
    fd,tmp=tempfile.mkstemp(prefix='.manifest-',dir=root)
    try:
        with os.fdopen(fd,'w') as f:json.dump(data,f,ensure_ascii=False,indent=2);f.flush();os.fsync(f.fileno())
        os.replace(tmp,path)
    finally:
        if os.path.exists(tmp):os.unlink(tmp)
    return item
