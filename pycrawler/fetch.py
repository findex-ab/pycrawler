from bs4 import BeautifulSoup
import requests


class Headers(object):
    data = dict()

    def set(self, key: str, value: str):
        self.data[key] = value
        self.data[key.upper()] = value
        self.data[key.title()] = value

def fetchDocument(url: str) -> BeautifulSoup | None:
    headers = Headers()
    headers.set('user-agent', 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36')
    headers.set('accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7')
    headers.set('sec-fetch-dest', 'document')
    headers.set('sec-ch-ua', '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"')
    headers.set('sec-ch-ua-mobile', '?0')
    headers.set('sec-ch-ua-platform', '"Linux"')
    headers.set('sec-fetch-mode', 'navigate')
    headers.set('sec-fetch-site', 'none')
    headers.set('sec-fetch-user', '?1')
    headers.set('upgrade-insecure-requests', '1')
    
    try:
        resp = requests.get(url, allow_redirects=True, timeout=4, headers=headers.data)
        if not resp.ok:
            return None
        
        text = resp.text
        
        return BeautifulSoup(text, 'html.parser')
    except:
        return None 
