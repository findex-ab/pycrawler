import urllib
import base64
import uuid
import re
import os

def url_remove_query(url: str) -> str:
    if url.endswith('#'):
        url = url[:-1]
    p = urllib.parse.urlparse(url)
    return url.replace(f'?{p.query}', '')

def url_get_domain(url: str) -> str:
    try:
        return urllib.parse.urlparse(url).netloc
    except Exception as e:
        print(e)
        return url

def url_get_filename(url: str) -> str | None:
    u = urllib.parse.urlparse(url)
    path = u.path
    if not path or path == '':
        return None

    return os.path.basename(path)

def url_get_extension(url: str) -> str | None:
    fname = url_get_filename(url)
    if not fname:
        return None
    _, ext = os.path.splitext(fname)
    return ext
    
def base64_encode(value: str) -> str:
    enc = b''

    try:
        enc = value.encode('utf-8')
    except:
        enc = value.encode('latin-1')

    try:
        return base64.b64encode(enc).decode()
    except Exception as e:
        print(e)
        return value


def unique(items):
    return list(set(items))


def chunkify(items, chunk_size=2):
    chunks = []
    i = 0
    count = len(items)
    while i < count:
        chunks.append(items[i:i+chunk_size])
        i += chunk_size
    return chunks


def create_uid(value: str) -> uuid.UUID:
    return uuid.uuid5(uuid.NAMESPACE_DNS, value)

def find_sentence(value: str, minlen: int = 0, maxlen: int = 140) -> str:
    def find():
        sent = value
        if '.' in value:
            sent = value[:value.index('.')]
            if len(sent) > 1:
                return sent

        if '\n' in value:
            sent = value[:value.index('\n')]
            if sent.startswith('(') and len(sent) < (minlen * 2) and ')' in sent:
                sent = value[:value.index(')')]
                sent = sent[:sent.index('\n')] if sent and '\n' in sent else ''
                
            if len(sent) > 1:
                return sent

        if '\r' in value:
            sent = value[:value.index('\r')]
            if len(sent) > 1:
                return sent

        if '?' in value:
            sent = value[:value.index('?')]
            if len(sent) > 1:
                return sent
        if '!' in value:
            sent = value[:value.index('!')]
            if len(sent) > 1:
                return sent
        return sent
    sent = find().strip()
    return sent[:maxlen]

def pad_right(value: str, want_len: int) -> str:
    le = len(value)
    if le >= want_len:
        return value

    remain = want_len - le
    pad = ''.join(list(map(lambda x: ' ', range(remain))))
    return f'{value}{pad}'

def strip(value: str) -> str:
    return re.sub('\ \ ', ' ', re.sub('\t', '', value).strip())


FILE_EXTENSIONS = [
    '.zip',
    '.rar',
    '.tar.gz',
    '.gz',
    '.pdf',
    '.docx',
    '.json',
    '.xls',
    '.csv',
    '.db',
    '.sqlite',
    '.sql',
    '.txt',
    '.ttf',
    '.otf',
    '.wav',
    '.mp3',
    '.flac',
    '.ogg',
    '.mp4',
    '.flv'
]

def is_file_extension(ext: str) -> bool:
    low = ext.lower()
    return low in FILE_EXTENSIONS

def is_file_url(url: str) -> bool:
    ext = url_get_extension(url)
    if not ext:
        return False
    return is_file_extension(ext)
