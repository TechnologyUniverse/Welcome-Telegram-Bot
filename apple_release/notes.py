"""Extract version-specific notes from fixed Apple Support pages only."""
import re
from html.parser import HTMLParser

PAGES = {
    ('iOS', '26'): 'https://support.apple.com/ru-ru/123075',
    ('iPadOS', '26'): 'https://support.apple.com/ru-ru/123074',
    ('macOS', '26'): 'https://support.apple.com/ru-ru/122868',
    ('watchOS', '26'): 'https://support.apple.com/ru-ru/123002',
    ('visionOS', '26'): 'https://support.apple.com/ru-ru/123024',
    ('iOS', '18'): 'https://support.apple.com/ru-ru/121161',
    ('iPadOS', '18'): 'https://support.apple.com/ru-ru/121162',
    ('macOS', '15'): 'https://support.apple.com/ru-ru/120283',
    ('watchOS', '11'): 'https://support.apple.com/ru-ru/121163',
    ('visionOS', '2'): 'https://support.apple.com/ru-ru/121164',
    ('iOS', '27'): 'https://support.apple.com/ru-ru/149076',
    ('iPadOS', '27'): 'https://support.apple.com/ru-ru/149075',
    ('macOS', '27'): 'https://support.apple.com/ru-ru/127257',
    ('watchOS', '27'): 'https://support.apple.com/ru-ru/149077',
    ('visionOS', '27'): 'https://support.apple.com/ru-ru/149067',
    ('tvOS', None): 'https://support.apple.com/ru-ru/106336',
    ('HomePod Software', None): 'https://support.apple.com/ru-ru/108045',
}

class Blocks(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True); self.blocks=[]; self.current=None; self.depth=0; self.hidden=0
    def handle_starttag(self, tag, attrs):
        if tag in ('script','style','nav','footer'): self.hidden+=1
        if self.hidden: return
        if self.current is not None: self.depth+=1
        elif tag in ('h2','h3','li','p'):
            self.current=[tag,[]]; self.depth=0
    def handle_endtag(self, tag):
        if tag in ('script','style','nav','footer') and self.hidden: self.hidden-=1; return
        if self.hidden or self.current is None: return
        if self.depth: self.depth-=1
        elif tag==self.current[0]:
            self.blocks.append((tag,' '.join(''.join(self.current[1]).split()))); self.current=None
    def handle_data(self, data):
        if self.current is not None and not self.hidden: self.current[1].append(data)

def page_for(release):
    if release.kind != 'RELEASE': return None
    return PAGES.get((release.platform, release.version.split('.')[0])) or PAGES.get((release.platform,None))

def extract(raw, release):
    if len(raw)>2_000_000: return []
    parser=Blocks(); parser.feed(raw.decode('utf-8','replace'))
    # Exact heading is required: do not attribute older or beta changes to this release.
    version=re.escape(release.version)
    pattern=re.compile(r'(?<![\d.])'+version+r'(?![\d.])')
    started=False; candidates=[]; paragraphs=[]
    for tag,text in parser.blocks:
        if tag in ('h2','h3'):
            if started and re.search(r'\d+(?:\.\d+)+',text): break
            if tag=='h2' and pattern.search(text): started=True
            continue
        if not started or tag not in ('p','li'): continue
        if re.search(r'https?://|support\.apple\.com|apple\.com',text,re.I): continue
        if tag=='p' and len(text.split())<5: continue
        if 20 <= len(text) <= 130:
            target = candidates if tag == 'li' else paragraphs
            if text not in target: target.append(text)
    return (candidates or paragraphs)[:3]
