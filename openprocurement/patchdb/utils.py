# -*- coding: utf-8 -*-
import time
import logging
import requests
import functools
import jsonpatch


LOG = logging.getLogger('patchdb')
SESSION = requests.Session()


def with_retry(tries, delay=1, backoff=2, log_error=LOG.error, expect=Exception, raise_on=None):
    def retry_decorator(func):
        @functools.wraps(func)
        def retry_function(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return func(*args, **kwargs)
                except (SystemExit, KeyboardInterrupt):
                    raise
                except expect as e:
                    if raise_on and isinstance(e, raise_on):
                        raise
                    if log_error:
                        log_error("{} {} {}".format(func.__name__, type(e).__name__, e))
                    for i in range(int(10 * mdelay)):
                        time.sleep(0.1)
                    mtries -= 1
                    mdelay *= backoff
            return func(*args, **kwargs)
        return retry_function
    return retry_decorator


@with_retry(tries=3)
def get_with_retry(url, require_text=''):
    LOG.debug("GET {}".format(url))
    resp = SESSION.get(url, timeout=30)
    resp.raise_for_status()
    if require_text and require_text not in resp.text:
        raise ValueError('bad response require_text not found')
    return resp.text


def get_revision_changes(dst, src):
    return jsonpatch.make_patch(dst, src).patch
