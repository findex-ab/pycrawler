import typing


class CrawlerConfig:
    num_threads: int = 2
    blacklist: typing.List[str] = []
    qdrant_enabled: bool = False
    qdrant_string: str = "http://localhost:6333"
    mongo_url: str = 'mongodb://127.0.0.1:27013/test',
    
    def __init__(self, *args, **kwargs):
        for k, v in kwargs.items():
            self.__setattr__(k, v)
