from bs4 import BeautifulSoup
import requests

def fetchDocument(url: str) -> BeautifulSoup | None:
    try:
        resp = requests.get(url, allow_redirects=True, timeout=3, headers={
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36'
        })
        text = resp.text
        return BeautifulSoup(text, 'html.parser')
    except:
        return None 
