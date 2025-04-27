from pycrawler.crawler import Crawler, crawl
from pycrawler.config import CrawlerConfig
import argparse
import json

parser = argparse.ArgumentParser()
parser.add_argument('seed', type=str, help="JSON file containing an array of URLS")
parser.add_argument('--threads', type=int, help="Number of threads", default=12)
parser.add_argument('--qdrant_enabled', type=bool, help="Enable qdrant", default=False)
parser.add_argument('--qdrant_string', type=str, help="qdrant connection string", default="http://localhost:6333")
parser.add_argument('--mongo_url', type=str, help="mongodb connection string", default='mongodb://127.0.0.1:27013/test')
args = parser.parse_args()

if __name__ == '__main__':
    urls = json.loads(open(args.seed).read())

    config = CrawlerConfig(
        blacklist=[
            'https://donate\.wikipedia.*?',
            'https://donate\.wikimedia.*?'
        ],
        num_threads=args.threads,
        qdrant_enabled=args.qdrant_enabled,
        qdrant_string=args.qdrant_string,
        mongo_url=args.mongo_url
    )
    crawl(urls=urls, config=config)
