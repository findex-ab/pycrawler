import mongoengine
from datetime import datetime
class BaseDocument(mongoengine.Document):
    meta = {
        'abstract': True
    }

    updated_at = mongoengine.DateTimeField(default=datetime.now)
    created_at = mongoengine.DateTimeField(default=datetime.now)

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
    
class Image(BaseDocument):
    src = mongoengine.StringField(required=True, unique=True)
    name = mongoengine.StringField(required=False)

class Website(BaseDocument):
    url = mongoengine.StringField(required=True, unique=True)
    name = mongoengine.StringField(required=False)
    articles = mongoengine.ListField(mongoengine.ReferenceField('Article'), required=False, default=[])
    images = mongoengine.ListField(mongoengine.ReferenceField(Image), required=False, default=[])

class Article(BaseDocument):
    uid = mongoengine.StringField(required=True, unique=True)
    url = mongoengine.StringField(required=True)
    name = mongoengine.StringField(required=False)
    text = mongoengine.StringField(required=True)
    images = mongoengine.ListField(mongoengine.ReferenceField(Image), required=False, default=[])
