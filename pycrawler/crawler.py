import typing
from pycrawler.fetch import fetchDocument
from pycrawler.models import CrawlerImage, CrawlerWebsite, CrawlerArticle, CrawlerFile
from pycrawler.db import connect_db
from pycrawler.db.qdrant import qdrant_connect
import pycrawler.utils as utils
import pycrawler.w2v as w2v
from bs4 import BeautifulSoup
from qdrant_client import models
import threading
import math
import urllib
import random
import re

SKIPPED_KEYWORDS = ['javascript:', 'mailto:', 'tel:']

MAX_VISITED_SIZE = 1024
MAX_QUEUE_SIZE = 300
MAX_DOMAIN_VISITS = 16
MAX_DOMAIN_VISITS_SIZE = 512
QDRANT_ENABLED = False

class Crawler:
    visited: typing.Set[str] = set()
    visited_domains: typing.Dict[str, int] = dict()
    queue: typing.Set[str] = set()
    blacklist: typing.List[str] = []

    def __init__(self, blacklist: typing.List[str] = []):
        self.blacklist = blacklist

    def pop(self):
        item = random.choice(list(self.queue) or [])
        self.queue.discard(item)
        return item
    
    def _should_skip(self, url: str):
        for black in self.blacklist:
            if re.match(black, url):
                return True
        if url in self.visited:
            return True
        for k in SKIPPED_KEYWORDS:
            if k in url:
                return True
        low = url.lower()
        if utils.is_file_url(low) or\
           low.endswith('.js') or\
           low.endswith('.css') or\
           low.endswith('.map') or\
           low.endswith('.svg') or\
           low.endswith('.ico') or\
           low.endswith('.gif') or\
           low.endswith('.bmp') or\
           low.endswith('.png') or\
           low.endswith('.jpg') or\
           low.endswith('.jpeg'):
            return True
        if url == "http://" or url == "https://":
            return True
        if len(url) <= 8:
            return True
        if self.visited_domains.get(utils.url_get_domain(url), 0) >= MAX_DOMAIN_VISITS:
            return True
        return False
    
    def should_skip(self, url: str):
        if self._should_skip(url) or self._should_skip(utils.url_remove_query(url)):
            return True
        return False

    def extract_title(self, doc: BeautifulSoup):
        name_el = doc.select_one('title')
        return name_el.text.strip() if name_el else None

    def extract_meta(self, doc: BeautifulSoup):
        meta_tags = list(doc.select('meta'))
        meta_dict = {}
        for tag in meta_tags:
            key = tag.get('property') or tag.get('name') or tag.get('key')
            value = tag.get('content') or tag.get('value')
            if key and value:
                meta_dict[key] = value
        return meta_dict

    def extract_keywords(self, doc: BeautifulSoup) -> typing.List[str]:
        meta = self.extract_meta(doc)
        keywords: str = meta.get('keywords') or meta.get('tags') or meta.get('og:keywords') or meta.get('og:tags')
        if not keywords:
            return []
        return utils.unique(list(map(lambda x: x.strip().lower(), keywords.strip().split(','))))
    
    def extract_images(self, doc: BeautifulSoup, url: str, fallback_title: str | None = None):
        def extract_image(el: BeautifulSoup):
            src = el.get('src')
            name = el.get('title') or el.get('data-title') or el.get('alt') or el.get('data-alt') or fallback_title
            if not src or not name:
                return None
            joined = urllib.parse.urljoin(url, src)
            return CrawlerImage(url=joined, name=name, domain=utils.url_get_domain(joined)).upsert()

        def image_from_meta():
            meta = self.extract_meta(doc)
            src = meta.get('og:image')
            if not src:
                return None
            joined = urllib.parse.urljoin(url, src) 
            name = meta.get('og:image:name') or meta.get('og:image:title') or meta.get('og:image:alt') or self.extract_title(doc) or fallback_title
            name = name.strip() if name else None
            if not name:
                return None
            return CrawlerImage(url=joined, name=name, domain=utils.url_get_domain(joined)).upsert()
            
            # width = meta.get('og:image:width')
            # height = meta.get('og:image:height')
            # mime = meta.get('og:image:type')
            
        images = list(doc.select('img[src]'))
        imgs = [*list(map(extract_image, images)), image_from_meta()]
        return list(filter(lambda x: x is not None, imgs))

    def extract_articles(self, doc: BeautifulSoup, url: str):
        # json_scripts = doc.select('script[type="application/ld+json"]')
        
        def extract_article(el: BeautifulSoup):
            title_tag = el.select_one('h1,h2,h3,h4')
            title = (title_tag.text or '').strip() if title_tag else None

            text = utils.strip('\n'.join(
                list(map(
                    lambda p: (p.text or '').strip(), el.select('p,li,bold,i')
                ))
            ))
            
            if not text or len(text) <= 1:
                return None

            if not title or title == '':
                title = utils.find_sentence(text, 3, 256)
                if not title or title == '':
                    return None
            
            images = self.extract_images(el, url)

            uid = utils.base64_encode((f'{url}-{title}').strip()) 
            return CrawlerArticle(
                uid=uid,
                name=title,
                text=text,
                url=url,
                images=images,
                domain=utils.url_get_domain(url)
            ).upsert()
            

        article_elements = list(doc.select('article,div.post'))
        return list(filter(lambda x: x is not None, map(extract_article, article_elements)))

    def extract_files(self, doc: BeautifulSoup, url: str):
        def extract_file(el: BeautifulSoup):
            src = el.get('src') or\
                el.get('data-src') or\
                el.get('href') or\
                el.get('data-href') or\
                el.get('source') or\
                el.get('content') or\
                el.get('value')
            if not src:
                return None
            joined = urllib.parse.urljoin(url, src)
            extension = utils.url_get_extension(joined)
            if not extension or not utils.is_file_extension(extension):
                return None
            filename = utils.url_get_filename(joined)
            domain = utils.url_get_domain(joined)
            return CrawlerFile(
                url=joined,
                domain=domain,
                name=filename,
                extension=extension
            ).upsert()
        elements = list(doc.select('a,source'))
        return filter(lambda x: x is not None, map(extract_file, elements))

    def crawl_url(self, url: str, thread_id: int, thread_name: str):
        if self.should_skip(url):
            return

        if len(self.visited) > MAX_VISITED_SIZE:
            print('---- CLEARING ----')
            self.visited.clear()

        if len(self.visited_domains) > MAX_DOMAIN_VISITS_SIZE:
            print('--- CLEARING DOMAIN VISITS ---')
            self.visited_domains.clear()

        domain = utils.url_get_domain(url)
        self.visited_domains[domain] = self.visited_domains.get(domain, 0) + 1

        print(f'{utils.pad_right(thread_name, 10)} -> {url}')
        doc: BeautifulSoup = fetchDocument(url)
        if not doc:
            return
        self.visited.add(url)

        name = self.extract_title(doc) 
        articles = self.extract_articles(doc, url)
        images = self.extract_images(doc, url, name)
        files = self.extract_files(doc, url)
        keywords = self.extract_keywords(doc)

        website = CrawlerWebsite(
            url=url,
            domain=domain,
            name=name,
            articles=articles,
            images=images,
            files=files,
            keywords=keywords
        ).upsert()

        if QDRANT_ENABLED:
            try:
                vec_id, vec = w2v.word2vec_with_id(' '.join(keywords) if len(keywords) > 0 else name)
                print(vec_id)

                qdrant.upsert(
                    collection_name="crawler_website",
                    points=[
                        models.PointStruct(
                            id=str(vec_id),
                            payload={
                                'name': name,
                                'url': url,
                                'domain': domain 
                            },  # Add any additional payload if necessary
                            vector=vec,
                        )
                    ],
                )
            except Exception as e:
                print(e)

        if len(self.queue) < MAX_QUEUE_SIZE:
            links = list(doc.select('a[href]'))
            random.shuffle(links)
            for link in links:
                if len(self.queue) > MAX_QUEUE_SIZE:
                    break
                href = link.get('href')
                if not href:
                    continue
                joined = urllib.parse.urljoin(url, href)
                if not self.should_skip(joined):
                    self.queue.add(joined)

    def crawl(self, urls: typing.List[str], thread_id: int, thread_name: str):
        for url in urls:
            self.queue.add(url)

        qdrant = qdrant_connect() if QDRANT_ENABLED else None

        while len(self.queue) > 0:
            url = self.pop()
            try:
                self.crawl_url(url, thread_id, thread_name)
            except Exception as e:
                print(e)
            


class CrawlThread(threading.Thread):
    urls: typing.List[str] = []
    blacklist: typing.List[str] = []
    def __init__(self, *args, **kwargs):
        threading.Thread.__init__(self, *args, **kwargs)

    def create(
            self,
            urls: typing.List[str],
            blacklist: typing.List[str] = []
    ):
        self.urls = urls
        self.blacklist = blacklist
        return self
    
    def run(self):
        print(self.native_id, len(self.urls))
        crawler = Crawler(self.blacklist)
        crawler.crawl(self.urls, self.ident, self.name)

def crawl(
        urls: typing.List[str] = [],
        num_threads: int = 2,
        blacklist: typing.List[str] = []
):
    connect_db()

    CrawlerWebsite.ensure_indexes()
    CrawlerArticle.ensure_indexes()
    CrawlerImage.ensure_indexes()
    CrawlerFile.ensure_indexes()

    print(blacklist)
    rand = list(map(lambda x: x.get('url'), CrawlerWebsite.get_random(10)))
    urls = utils.unique([*urls, *rand])
    urls_per_thread = math.ceil(len(urls) / num_threads)

    chunks: typing.List[str] = utils.chunkify(urls, urls_per_thread)
    threads = [CrawlThread().create(chunk, blacklist) for chunk in chunks]

    for i, thread in enumerate(threads):
        thread.setDaemon(True)
        thread.setName(f'thread_{i}')
        thread.start()

    for thread in threads:
        thread.join()
        print(f"::::::: thread {thread.name} finished. :::::::")
