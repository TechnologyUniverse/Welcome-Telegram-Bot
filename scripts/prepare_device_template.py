"""Build a conservative draft cutout and screen masks from the supplied device composition."""
from PIL import Image, ImageDraw, ImageChops, ImageFilter
from pathlib import Path
from hashlib import sha256
import json
ROOT=Path(__file__).resolve().parents[1]
out=ROOT/'assets/apple_release/template-draft';out.mkdir(parents=True,exist_ok=True)
img=Image.open(ROOT/'assets/apple_release/devices-original.png').convert('RGBA')
if img.size!=(1774,887):raise ValueError('Unexpected device image dimensions')
# Shapes follow the supplied composition. No device is redrawn or moved.
mask=Image.new('L',img.size,0);d=ImageDraw.Draw(mask)
d.rounded_rectangle((420,35,1370,638),radius=20,fill=255)
d.polygon([(836,635),(1000,635),(1018,760),(1043,778),(1042,784),(781,784),(781,778),(807,760),(815,635)],fill=255)
d.rounded_rectangle((68,392,571,797),radius=30,fill=255)
d.rounded_rectangle((522,463,699,834),radius=31,fill=255)
d.rounded_rectangle((927,653,1125,834),radius=37,fill=255)
d.rounded_rectangle((1129,551,1211,834),radius=18,fill=255)
d.polygon([(1215,718),(1222,675),(1234,655),(1260,638),(1295,621),(1340,608),(1405,602),(1475,605),(1538,621),(1576,640),(1599,663),(1611,696),(1613,744),(1608,776),(1594,797),(1565,815),(1532,827),(1503,828),(1475,812),(1437,786),(1405,777),(1374,780),(1342,798),(1310,822),(1281,828),(1255,819),(1230,806),(1216,789)],fill=255)
d.rounded_rectangle((1649,630,1714,688),radius=18,fill=255)
d.rounded_rectangle((1620,664,1739,805),radius=32,fill=255)
d.rounded_rectangle((1645,785,1715,834),radius=16,fill=255)
# Remove checker pixels at the outer rims of the glasses and watch.
# Restrict cleanup to the border so the light-coloured remote stays intact.
pixels=img.load();alpha=mask.load()
for x0,y0,x1,y1 in [(1210,785,1320,835),(1490,785,1620,835),(1615,625,1655,835),(1708,625,1745,835)]:
    for y in range(y0,y1):
        for x in range(x0,x1):
            red,green,blue,_=pixels[x,y]
            if min(red,green,blue)>=185 and max(red,green,blue)-min(red,green,blue)<=8:
                alpha[x,y]=0
# Supplied display coordinates, excluding devices in the foreground.
screens={
'macOS':((444,60,1348,533),[(0,391,572,887),(522,462,700,887)],2),
'iPadOS':((84,410,554,778),[(522,462,700,887)],15),
'iOS':((538,479,685,816),[],25),
'watchOS':((1630,672,1727,799),[],22),
}
entries=[]
for platform,(box,occlusions,radius) in screens.items():
 m=Image.new('L',img.size,0);md=ImageDraw.Draw(m);md.rounded_rectangle(box,radius=radius,fill=255)
 for rect in occlusions:md.rectangle(rect,fill=0)
 name=platform.lower()+'.png';m.save(out/name)
 mask=ImageChops.subtract(mask,m)
 entries.append({'platform':platform,'mask':name,'sha256':sha256((out/name).read_bytes()).hexdigest()})
img.putalpha(mask.filter(ImageFilter.GaussianBlur(.5)));img.save(out/'devices-draft.png',optimize=True)
manifest={'approved':False,'device_sha256':sha256((out/'devices-draft.png').read_bytes()).hexdigest(),'screens':entries,'note':'Draft geometry, visually inspect before approval.'}
(out/'device-manifest.json').write_text(json.dumps(manifest,indent=2,ensure_ascii=False))
print('Draft template prepared:',out)
