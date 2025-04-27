import mongoengine
from datetime import datetime
class BaseDocument(mongoengine.Document):
    meta = {
        'abstract': True
    }

    updated_at = mongoengine.DateTimeField(default=datetime.utcnow)
    created_at = mongoengine.DateTimeField(default=datetime.utcnow)

    def save(self, *args, **kwargs):
        if not self.created_at:
            self.created_at = datetime.now()
        self.updated_at = datetime.now()
        return super(BaseDocument, self).save(*args, **kwargs)

    def upsert(self, *args, **kwargs):
        cls = self.__class__
        unique_fields = [
            f for f, field in self._fields.items()
            if getattr(field, 'unique', False)
        ]

        if not unique_fields:
            raise ValueError(f"No unique fields defined in {cls.__name__} for upsert.")

        # Build query based on unique fields
        query = {field: getattr(self, field) for field in unique_fields}
        update_fields = {
            f"set__{field}": getattr(self, field)
            for field in self._fields
            if field not in ('id',) and getattr(self, field, None) is not None
        }

        cls.objects(**query).update_one(upsert=True, **update_fields)

        return cls.objects.get(**query)
    
class CrawlerImage(BaseDocument):
    category = mongoengine.StringField(required=True, default='IMAGE') 
    url = mongoengine.StringField(required=True, unique=True)
    domain = mongoengine.StringField(required=True)
    name = mongoengine.StringField(required=False)
    language = mongoengine.StringField(required=False)
    keywords = mongoengine.ListField(mongoengine.StringField(), required=False, default=[])

    meta = {
        'index_opts': {
            'language_override': 'lang'
        },
        'indexes': [
            '$name',
            'keywords',
            'language'
        ]
    }

class CrawlerFile(BaseDocument):
    category = mongoengine.StringField(required=True, default='FILE') 
    url = mongoengine.StringField(required=True, unique=True)
    domain = mongoengine.StringField(required=True)
    name = mongoengine.StringField(required=True)
    language = mongoengine.StringField(required=False)
    extension = mongoengine.StringField(required=True) 
    keywords = mongoengine.ListField(mongoengine.StringField(), required=False, default=[])

    meta = {
        'index_opts': {
            'language_override': 'lang'
        },
        'indexes': [
            '$name',
            'domain',
            'keywords',
            'language'
        ]
    }
    
class CrawlerWebsite(BaseDocument):
    category = mongoengine.StringField(required=True, default='WEBSITE') 
    url = mongoengine.StringField(required=True, unique=True)
    domain = mongoengine.StringField(required=True)
    name = mongoengine.StringField(required=False)
    language = mongoengine.StringField(required=False)
    articles = mongoengine.ListField(mongoengine.ReferenceField('CrawlerArticle'), required=False, default=[])
    images = mongoengine.ListField(mongoengine.ReferenceField(CrawlerImage), required=False, default=[])
    files = mongoengine.ListField(mongoengine.ReferenceField(CrawlerFile), required=False, default=[])
    keywords = mongoengine.ListField(mongoengine.StringField(), required=False, default=[])

    # https://docs.mongoengine.org/guide/defining-documents.html#indexes
    meta = {
        'index_opts': {
            'language_override': 'lang'
        },
        'indexes': [
            '$name',
            'language',
            'keywords'
        ]
    }

    def get_random(n: int):
        pipeline = [
            {'$sample': {'size': n}}
        ]
        return list(CrawlerWebsite.objects.aggregate(*pipeline))

class CrawlerArticle(BaseDocument):
    category = mongoengine.StringField(required=True, default='ARTICLE') 
    uid = mongoengine.StringField(required=True, unique=True)
    url = mongoengine.StringField(required=True)
    domain = mongoengine.StringField(required=True)
    name = mongoengine.StringField(required=False)
    language = mongoengine.StringField(required=False)
    text = mongoengine.StringField(required=True)
    images = mongoengine.ListField(mongoengine.ReferenceField(CrawlerImage), required=False, default=[])
    keywords = mongoengine.ListField(mongoengine.StringField(), required=False, default=[])
    links = mongoengine.ListField(mongoengine.StringField(), required=False, default=[])
    link = mongoengine.StringField(required=True)
    source_date = mongoengine.DateTimeField(default=datetime.utcnow, required=True)

    meta = {
        'index_opts': {
            'language_override': 'lang'
        },
        'indexes': [
            '$text',
            'name',
            'keywords',
            'language',
            'links',
            'link',
            'source_date'
        ]
    }
