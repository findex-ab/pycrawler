import json
import os
import typing
import pycrawler.utils as utils
import uuid

FNAME = os.path.join(os.path.realpath(os.path.dirname(__file__)), '../data/vectors.json')
vectors = None 

def average(vecs):
    comp = len(vecs[0])
    results = list(map(lambda x: 0, range(comp)))
    for vec in vecs:
        for i, x in enumerate(vec):
            results[i] += x
    for i, x in enumerate(results):
        results[i] = x / len(vecs)
    return results

def get_vec(value: str) -> typing.List[float]:
    if not vectors:
        vectors = json.loads(open(FNAME).read()) 
    
    vec = vectors.get(value)
    if vec and len(vec) > 1:
        return vec
    vec = vectors.get(value.lower())
    if vec and len(vec) > 1:
        return vec
    vec = vectors.get(value.title())
    if vec and len(vec) > 1:
        return vec
    vec = vectors.get(value.upper())
    if vec and len(vec) > 1:
        return vec
    return []

def word2vec(value: str) -> typing.List[float]:
    vec = get_vec(value)
    if vec and len(vec) > 0:
        return vec

    if ' ' in value:
        words = value.split(' ')
        vecs = list(filter(lambda x: x is not None and len(x) > 0, list(map(lambda x: get_vec(x), words))))
        if len(vecs) > 0:
            return average(vecs)

    pairs = utils.chunkify(value)

    vecs = list(filter(lambda x: x is not None and len(x) > 0, list(map(lambda x: get_vec(x), pairs))))

    if len(vecs) <= 0:
        return []

    return average(vecs)


def word2vec_with_id(value: str) -> typing.Tuple[uuid.UUID, typing.List[float]]:
    vec_id = utils.create_uid(value)
    return [vec_id, word2vec(value)]
