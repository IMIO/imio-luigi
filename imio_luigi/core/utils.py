# -*- coding: utf-8 -*-

from luigi.freezing import FrozenOrderedDict

import copy
import hashlib
import json
import os
import re

CACHE = {}
FSCACHE = None


def _cache_key(func, *args, **kwargs):
    return (func, args, frozenset(kwargs.items()))


def _cache_key_md5(func, *args, ignore_self=False, **kwargs):
    if ignore_self is True:
        func_md5 = hashlib.md5(
            "{0}-{1}".format(args[0].__class__.__name__, func.__name__).encode()
        ).hexdigest()
        arguments = list(copy.deepcopy(args[1:]))
    else:
        func_md5 = hashlib.md5(func.__name__.encode()).hexdigest()
        arguments = list(copy.deepcopy(args))

    for k, v in kwargs.items():
        arguments.append((k, v))
    args_md5 = hashlib.md5(json.dumps(arguments).encode()).hexdigest()
    return f"{func_md5}-{args_md5}"


def _cache(ignore_args=False):
    def decorator(func):
        def replacement(*args, **kwargs):
            if ignore_args is True:
                key = _cache_key(func)
            else:
                key = _cache_key(func, *args, **kwargs)
            if key in CACHE:
                return CACHE[key]
            else:
                result = func(*args, **kwargs)
                CACHE[key] = result
                return result

        return replacement

    return decorator


class MockedRequestResponse(object):
    def __init__(self, status_code, json_result):
        self.status_code = status_code
        self.json_result = json_result

    def json(self):
        return self.json_result


def _initiliaze_fs_cache():
    global FSCACHE

    if not os.path.exists(".cache"):
        os.mkdir(".cache")
    FSCACHE = {}


def _get_cache_path(cache_key):
    """Return path et filename for cache"""
    func, key = cache_key.split("-")
    key_path = re.findall(".{1,4}", key)
    return os.path.join(".cache", func, *key_path[:-1]), key_path[-1]


def _get_cache_from_fs(cache_key):
    if cache_key in FSCACHE:
        return FSCACHE[cache_key]
    path, fname = _get_cache_path(cache_key)
    fpath = os.path.join(path, fname)
    if os.path.exists(fpath):
        with open(fpath, "r") as f:
            FSCACHE[cache_key] = json.load(f)
        return FSCACHE[cache_key]


def _cache_on_fs(cache_key):
    path, fname = _get_cache_path(cache_key)
    if not os.path.exists(path):
        os.makedirs(path)
    with open(os.path.join(path, fname), "w") as f:
        json.dump(FSCACHE[cache_key], f)


def _cache_request(expected_codes=(200, 301), ignore_self=True):
    if FSCACHE is None:
        _initiliaze_fs_cache()

    def decorator(func):
        def replacement(*args, **kwargs):
            cache_key = _cache_key_md5(func, *args, ignore_self=ignore_self, **kwargs)
            cache = _get_cache_from_fs(cache_key)
            print(func, args, kwargs, cache_key)
            if cache:
                return MockedRequestResponse(cache["status_code"], cache["json"])
            result = func(*args, **kwargs)
            if result.status_code in expected_codes:
                FSCACHE[cache_key] = {
                    "status_code": result.status_code,
                    "json": result.json(),
                }
                _cache_on_fs(cache_key)
            return result

        return replacement

    return decorator


def mock_filename(task, name):
    if task.task_namespace:
        return f"{task.task_namespace}-{task.__class__.__name__}-{name}-{task.key}"
    else:
        return f"{task.__class__.__name__}-{name}-{task.key}"


def frozendict_to_dict(fdict):
    """Convert a frozen dict into a regular dict"""
    new_dict = dict(fdict)
    for key, value in new_dict.items():
        if isinstance(value, FrozenOrderedDict):
            value = frozendict_to_dict(value)
            new_dict[key] = value
        if isinstance(value, (set, tuple, list)):
            value = [
                isinstance(v, FrozenOrderedDict) and frozendict_to_dict(v) or v
                for v in value
            ]
            new_dict[key] = value
    return new_dict
