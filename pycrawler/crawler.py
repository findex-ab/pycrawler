import typing
from pycrawler.fetch import fetchDocument
from pycrawler.models import CrawlerImage, CrawlerWebsite, CrawlerArticle, CrawlerFile
from pycrawler.db import connect_db
from pycrawler.db.qdrant import qdrant_connect
import pycrawler.utils as utils
import pycrawler.w2v as w2v
from pycrawler.config import CrawlerConfig
from pycrawler.page import Page
from bs4 import BeautifulSoup
from qdrant_client import models
import threading
import math
import urllib
import random
import re
import gc
import datetime

SKIPPED_KEYWORDS = ['javascript:', 'mailto:', 'tel:']

MAX_VISITED_SIZE = 1024
MAX_QUEUE_SIZE = 320
MAX_DOMAIN_VISITS = 10
MAX_DOMAIN_VISITS_SIZE = 512
GC_TIME_SECONDS = 60 * 5

class Crawler:
    visited: typing.Set[str] = set()
    visited_domains: typing.Dict[str, int] = dict()
    queue: typing.Set[str] = set()
    config: CrawlerConfig
    time_started = datetime.datetime.utcnow()
    time_gc = datetime.datetime.utcnow()

    def __init__(self, config: CrawlerConfig):
        self.config = config

    def pop(self):
        item = random.choice(list(self.queue) or [])
        self.queue.discard(item)
        return item

    def should_gc(self):
        now = datetime.datetime.utcnow()
        diff = now - self.time_gc
        if diff.seconds >= GC_TIME_SECONDS:
            print('**** GARBAGE COLLECT ****')
            gc.collect()
            self.time_gc = datetime.datetime.utcnow()
    
    def _should_skip(self, url: str):
        for black in self.config.blacklist:
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

    def crawl_url(self, url: str, thread_id: int, thread_name: str):
        if self.should_skip(url):
            return

        if len(self.visited) > MAX_VISITED_SIZE:
            print('---- CLEARING ----')
            self.visited.clear()
            gc.collect()

        if len(self.visited_domains) > MAX_DOMAIN_VISITS_SIZE:
            print('--- CLEARING DOMAIN VISITS ---')
            self.visited_domains.clear()
            gc.collect()

        domain = utils.url_get_domain(url)
        self.visited_domains[domain] = self.visited_domains.get(domain, 0) + 1

        print(f'{utils.pad_right(thread_name, 10)} -> {url}')
        doc: BeautifulSoup = fetchDocument(url)
        if not doc:
            return
        self.visited.add(url)

        page = Page(url, doc)
        # meta = self.extract_meta(doc)
        # lang = self.extract_language(doc, url, meta)
        # name = self.extract_title(doc) 
        # keywords = self.extract_keywords(doc, title=name)
        # articles = self.extract_articles(doc, url, fallback_title=name, keywords=keywords, lang=lang)
        # images = self.extract_images(doc, url, fallback_title=name, keywords=keywords, lang=lang)
        # files = self.extract_files(doc, url, keywords=keywords, lang=lang)

        website = CrawlerWebsite(
            url=page.url,
            domain=page.domain,
            name=page.title,
            articles=page.articles,
            images=page.images,
            files=page.files,
            keywords=page.keywords,
            language=page.language
        ).upsert()

        if self.config.qdrant_enabled:
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

        qdrant = qdrant_connect(self.config.qdrant_string) if self.config.qdrant_enabled else None

        while len(self.queue) > 0:
            url = self.pop()
            try:
                self.crawl_url(url, thread_id, thread_name)
            except Exception as e:
                print(f'******** Error crawling {url} ********')
                print(e)

            if self.should_gc():
                gc.collect()
            


class CrawlThread(threading.Thread):
    urls: typing.List[str] = []
    config: CrawlerConfig = CrawlerConfig()
    
    def __init__(self, *args, **kwargs):
        threading.Thread.__init__(self, *args, **kwargs)

    def create(
            self,
            urls: typing.List[str],
            config: CrawlerConfig = CrawlerConfig
    ):
        self.urls = urls
        self.config = config
        return self
    
    def run(self):
        print(self.native_id, len(self.urls))
        crawler = Crawler(self.config)
        crawler.crawl(self.urls, self.ident, self.name)

def crawl(
        urls: typing.List[str] = [],
        config: CrawlerConfig = CrawlerConfig()
):
    connect_db()

    CrawlerWebsite.ensure_indexes()
    CrawlerArticle.ensure_indexes()
    CrawlerImage.ensure_indexes()
    CrawlerFile.ensure_indexes()

    rand = list(map(lambda x: x.get('url'), CrawlerWebsite.get_random(2 * (len(urls) + 1))))
    urls = utils.unique([*urls, *rand])
    random.shuffle(urls)
    
    urls_per_thread = math.ceil(len(urls) / config.num_threads)

    chunks: typing.List[str] = utils.chunkify(urls, urls_per_thread)
    threads = [CrawlThread().create(chunk, config) for chunk in chunks]

    for i, thread in enumerate(threads):
        thread.setDaemon(True)
        thread.setName(f'thread_{i}')
        thread.start()

    for thread in threads:
        thread.join()
        print(f"::::::: thread {thread.name} finished. :::::::")
