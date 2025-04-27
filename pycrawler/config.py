import typing


class CrawlerConfig:
    num_threads: int = 2
    blacklist: typing.List[str] = []
    qdrant_enabled: bool = False
    qdrant_string: str = "http://localhost:6333"
    
    def __init__(self, *args, **kwargs):
        for k, v in kwargs.items():
            self.__setattr__(k, v)
