# -*- coding: utf-8 -*-

from luigi.freezing import FrozenOrderedDict

import json
import os
import hashlib

CACHE = {}
FSCACHE = {}


def _cache_key(func, *args, **kwargs):
    return (func, args, frozenset(kwargs.items()))


def _cache_key_md5(func, *args, **kwargs):
    func_md5 = hashlib.md5(func.__name__.encode()).hexdigest()
    for k, v in kwargs.items():
        if isinstance(v, dict):
            kwargs[k] = frozenset(v.items())
    args_md5 = hashlib.md5(str((args, frozenset(kwargs.items()))).encode()).hexdigest()
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


def _get_cache_from_fs():
    if not os.path.exists(".cache"):
        os.mkdir(".cache")
    for fname in os.listdir(".cache"):
        with open(os.path.join(".cache", fname), "r") as f:
            FSCACHE[fname] = json.load(f)


def _cache_on_fs(key):
    with open(os.path.join(".cache", key), "w") as f:
        json.dump(FSCACHE[key], f)


def _cache_request(expected_codes=(200, 301), ignore_self=True):
    if not FSCACHE:
        _get_cache_from_fs()
    def decorator(func):
        def replacement(*args, **kwargs):
            if ignore_self is True:
                cache_key = _cache_key_md5(func, *args[1:], **kwargs)
            else:
                cache_key = _cache_key_md5(func, *args, **kwargs)
            if cache_key in FSCACHE:
                cache = FSCACHE[cache_key]
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
