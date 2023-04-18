# -*- coding: utf-8 -*-

from luigi.freezing import FrozenOrderedDict

CACHE = {}


def _cache_key(func, *args, **kwargs):
    return (func, args, frozenset(kwargs.items()))


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
