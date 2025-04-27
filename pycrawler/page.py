import typing
from bs4 import BeautifulSoup
from pycrawler.meta import Meta
import pycrawler.utils as utils
import pycrawler.models as crawler_models
import urllib
import dateutil
import datetime

class Page(object):
    doc: BeautifulSoup
    meta: Meta
    url: str
    domain: str
    title: str
    language: str
    keywords: typing.List[str]
    files: typing.List[crawler_models.CrawlerFile]
    images: typing.List[crawler_models.CrawlerImage]
    articles: typing.List[crawler_models.CrawlerArticle]

    def __init__(self, url: str, doc: BeautifulSoup):
        self.url = url
        self.domain = utils.url_get_domain(url)
        self.doc = doc
        self.meta = self._extract_meta()
        self.title = self._extract_title()
        self.language = self._extract_language()
        self.keywords = self._extract_keywords()
        self.files = self._extract_files(self.doc)
        self.images = self._extract_images(doc, url, fallback_title=self.title, keywords=self.keywords, lang=self.language)
        self.articles = self._extract_articles(doc, url, fallback_title=self.title, keywords=self.keywords, lang=self.language)

    def _extract_meta(self):
        meta_tags = list(self.doc.select('meta'))
        meta_dict = {}
        for tag in meta_tags:
            key = tag.get('property') or tag.get('name') or tag.get('key')
            value = tag.get('content') or tag.get('value')
            if key and value:
                meta_dict[key] = value
        return Meta(meta_dict)

    def _extract_title(self):
        name_el = self.doc.select_one('title')
        return name_el.text.strip() if name_el else None

    def _extract_language(self):
        html = self.doc.select_one('html')
        if html:
            lang = html.get('lang')
            if lang:
                return lang
        meta_locale = self.meta.get('locale') or\
            self.meta.get('lang') or\
            self.meta.get('language')
        if meta_locale:
            return meta_locale
        return utils.url_get_language(self.url)

    def _extract_keywords(self) -> typing.List[str]:
        keywords: str = self.meta.get('keywords') or self.meta.get('tags')
        kws = list(map(lambda x: utils.normalize_string(x.strip().lower()), keywords.strip().split(','))) if keywords else []
        if self.title:
            kws.append(self.title)
        return utils.keywordify(kws)

    def _extract_files(self, doc: BeautifulSoup):
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
            joined = urllib.parse.urljoin(self.url, src)
            extension = utils.url_get_extension(joined)
            if not extension or not utils.is_file_extension(extension):
                return None
            filename = utils.url_get_filename(joined)
            domain = utils.url_get_domain(joined)
            return crawler_models.CrawlerFile(
                url=joined,
                domain=domain,
                name=filename,
                extension=extension,
                keywords=self.keywords,
                language=self.language
            ).upsert()
        elements = list(doc.select('a,source'))
        return filter(lambda x: x is not None, map(extract_file, elements))

    def _extract_images(self, doc: BeautifulSoup, url: str, fallback_title: str | None = None, keywords: typing.List[str] = [], lang: str | None = None):
        def extract_image(el: BeautifulSoup):
            src = el.get('src') or el.get('href')
            if not src:
                return None
            name = el.get('title') or el.get('data-title') or el.get('alt') or el.get('data-alt') or fallback_title
            if not name:
                return None
            name = utils.strip(name)
            joined = urllib.parse.urljoin(url, src)
            img_keywords = keywords.copy()
            img_keywords.extend(utils.keywordify(name))
            img_keywords = utils.unique(img_keywords)
            return crawler_models.CrawlerImage(url=joined, name=name, domain=utils.url_get_domain(joined), keywords=img_keywords, language=lang).upsert()

        def image_from_meta():
            src = self.meta.get('og:image')
            if not src:
                return None
            joined = urllib.parse.urljoin(url, src) 
            name = self.meta.get('image:name') or self.meta.get('image:title') or self.meta.get('image:alt') or self.title or fallback_title
            name = name.strip() if name else None
            if not name:
                return None
            name = utils.strip(name)
            img_keywords = keywords.copy()
            img_keywords.extend(utils.keywordify(name))
            img_keywords = utils.unique(img_keywords)
            return crawler_models.CrawlerImage(url=joined, name=name, domain=utils.url_get_domain(joined), keywords=img_keywords, language=lang).upsert()
            
            # width = meta.get('og:image:width')
            # height = meta.get('og:image:height')
            # mime = meta.get('og:image:type')
            
        images = list(doc.select('img[src],link[rel="icon"]'))
        imgs = [*list(map(extract_image, images)), image_from_meta()]
        return list(filter(lambda x: x is not None, imgs))

    def _extract_articles(self, doc: BeautifulSoup, url: str, fallback_title: str | None = None, keywords: typing.List[str] = [], lang: str | None = None):
        # json_scripts = doc.select('script[type="application/ld+json"]')

        def extract_article(el: BeautifulSoup):
            title_tag = el.select_one('h1,h2,h3,h4,.subject,.title')
            title = (title_tag.text or '').strip() if title_tag else None

            text = utils.strip('\n'.join(
                list(map(
                    lambda p: (p.text or '').strip(), list(el.select('p')) or list(el.select('li'))
                ))
            ))
            
            if not text or len(text) <= 1:
                return None

            if not title or title == '':
                title = utils.find_sentence(text, 3, 256) or fallback_title
                if not title or title == '':
                    return None

            title = utils.strip(title)
            article_keywords = keywords.copy()
            article_keywords.extend(utils.keywordify(title))
            article_keywords = utils.unique(article_keywords)
            images = self._extract_images(el, url, fallback_title=title, keywords=article_keywords, lang=lang)
            links = utils.unique(list(map(lambda x: urllib.parse.urljoin(url, x),
                             filter(lambda x: x is not None, map(lambda x: x.get('href'), list(el.select('a[href]')))))))
            
            links = list(filter(lambda x: utils.url_get_extension(x) not in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webm'], links))

            link = url
            if links and len(links) == 1:
                link = links[0]
            else:
                slug1 = utils.slugify(title, '-')
                slug2 = utils.slugify(title, '_')
                slug3 = utils.slugify(title, ' ')
                link = (utils.find(links, lambda x: slug1 in x or slug2 in x or slug3 in x) or utils.max_string(links) or url) if links else url

            uid = str(utils.create_uid(''.join(utils.unique([title, url, *article_keywords]))))
            source_date = datetime.datetime.utcnow()
            
            source_date_str = self.meta.get('date') or\
                self.meta.get('time') or\
                self.meta.get('date_published') or\
                self.meta.get('time_published') or\
                self.meta.get('date-published') or\
                self.meta.get('time-published') or\
                self.meta.get('date_modified') or\
                self.meta.get('time_modified') or\
                self.meta.get('date-modified') or\
                self.meta.get('time-modified') or\
                self.meta.get('timestamp')
            source_date_el = el.select_one('time')
            if source_date_el:
                val = source_date_el.get('datetime') or source_date_el.get('unixtime')
                if val and utils.is_valid_date_string(val):
                    source_date_str = val
                else:
                    val = source_date_el.text
                    if val and utils.is_valid_date_string(val):
                        source_date_str = val

            if source_date_str:
                try:
                    source_date = dateutil.parser.parse(source_date_str)
                except Exception as e:
                    print(f'Failed to parse date: {source_date_str} url={self.url}')
                    print(e)
                
            return crawler_models.CrawlerArticle(
                uid=uid,
                name=title,
                text=text,
                url=url,
                images=images,
                domain=utils.url_get_domain(url),
                keywords=article_keywords,
                language=lang,
                links=links,
                link=link,
                source_date = source_date
            ).upsert()
            

        article_elements = list(doc.select('article,div.post,.news-article,.article'))
        return list(filter(lambda x: x is not None, map(extract_article, article_elements)))
