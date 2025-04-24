import typing
from pycrawler.fetch import fetchDocument
from pycrawler.models import Image, Website, Article
from pycrawler.db import connect_db
from bs4 import BeautifulSoup
import threading
import math
import urllib


SKIPPED_KEYWORDS = ['javascript:', 'mailto:', 'tel:']

class Crawler:
    visited: typing.Set[str] = set()
    queue: typing.Set[str] = set()

    def should_skip(self, url: str):
        if url in self.visited:
            return True
        for k in SKIPPED_KEYWORDS:
            if k in url:
                return True
        return False

    def extract_images(self, doc: BeautifulSoup, url: str):
        def extract_image(el: BeautifulSoup):
            src = el.get('src')
            name = el.get('title') or el.get('data-title') or el.get('alt') or el.get('data-alt')
            if not src:
                return None
            joined = urllib.parse.urljoin(url, src)
            return Image(src=joined, name=name).upsert()
            
        images = list(doc.select('img[src]'))
        return list(filter(lambda x: x is not None, map(extract_image, images)))

    def extract_articles(self, doc: BeautifulSoup, url: str):
        def extract_article(el: BeautifulSoup):
            title_tag = el.select_one('h1,h2,h3,h4')
            title = title_tag.text if title_tag else None
            if not title:
                return None
            text = '\n'.join(list(map(lambda p: p.text, el.select('p'))))
            if not text or len(text) <= 1:
                return None

            images = self.extract_images(el, url)

            uid = f'{url}-{title}'
            return Article(
                uid=uid,
                name=title,
                text=text,
                url=url,
                images=images
            ).upsert()
            

        article_elements = list(doc.select('article'))
        return list(filter(lambda x: x is not None, map(extract_article, article_elements)))

    def crawl(self, urls: typing.List[str], id: int):
        for url in urls:
            self.queue.add(url)

        while len(self.queue) > 0:
            url = self.queue.pop()
            if not url:
                break

            if self.should_skip(url):
                continue
            
            if len(self.visited) > 10000:
                print('---- CLEARING ----')
                self.visited.clear()

            print(f'{id} -> {url}')
            doc: BeautifulSoup = fetchDocument(url)
            if not doc:
                continue
            self.visited.add(url)

            nameEl = doc.select_one('title')
            name = nameEl.text if nameEl else None

            articles = self.extract_articles(doc, url)
            images = self.extract_images(doc, url)

            print(articles)
            
            website = Website(
                url=url,
                name=name,
                articles=articles,
                images=images
            ).upsert()
            

            if len(self.queue) < 500:
                links = doc.select('a[href]')
                for link in links:
                    if len(self.queue) > 500:
                        break
                    href = link.get('href')
                    if not href:
                        continue
                    joined = urllib.parse.urljoin(url, href)
                    if joined not in self.visited:
                        self.queue.add(joined)




class CrawlThread(threading.Thread):
    urls: typing.List[str] = []
    def __init__(self, *args, **kwargs):
        threading.Thread.__init__(self, *args, **kwargs)

    def create(self, urls: typing.List[str]):
        self.urls = urls
        return self
    
    def run(self):
        print(self.native_id, len(self.urls))
        crawler = Crawler()
        crawler.crawl(self.urls, self.native_id)

def crawl(urls: typing.List[str], num_threads: int = 2):
    connect_db()
    urls_per_thread = math.ceil(len(urls) / num_threads)

    chunks: typing.List[str] = []
    i = 0
    while i < len(urls):
        chunks.append(urls[i:i+urls_per_thread])
        i += urls_per_thread

    threads = [CrawlThread().create(chunk) for chunk in chunks]

    for thread in threads:
        thread.setDaemon(True)
        thread.start()

    for thread in threads:
        thread.join()
