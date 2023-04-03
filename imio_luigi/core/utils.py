# -*- coding: utf-8 -*-

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
        return f"{task.task_namespace}-{task.__class__.__name__}-" f"{name}-{task.key}"
    else:
        return f"{task.__class__.__name__}-{name}-{task.key}"
