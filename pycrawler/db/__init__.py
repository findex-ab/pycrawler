import mongoengine

def connect_db(host='mongodb://127.0.0.1:27013/test'):
    return mongoengine.connect(host=host)
