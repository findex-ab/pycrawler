import typing


class Meta(object):
    data: typing.Dict[str, str]

    def __init__(self, data: typing.Dict[str, str]):
        self.data = data

    def _get(self, key: str)  -> str | None:
        return self.data.get(key) or self.data.get(f'og:{key}')

    def get(self, key: str) -> str | None:
        return self._get(key) or\
            self._get(key.title()) or\
            self._get(key.upper()) or\
            self._get(key.capitalize())
