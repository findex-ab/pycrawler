import mongoengine

def connect_db():
    return mongoengine.connect('stuff', host='127.0.0.1', port=27013)
