# -*- coding: utf-8 -*-
import time
import functools
import logging
import requests
from jsonpatch import make_patch


LOG = logging.getLogger('patchdb')
SESSION = requests.Session()


def get_with_retry(url, require_text='', max_retry=5):
    for retry in range(max_retry):
        try:
            LOG.debug("GET {}".format(url))
            resp = SESSION.get(url, timeout=30)
            resp.raise_for_status()
            if require_text and require_text not in resp.text:
                raise ValueError('bad response require_text not found')
            return resp.text
        except KeyboardInterrupt:
            raise
        except Exception as e:
            LOG.error("{} on GET {}".format(e.__class__.__name__, url))
            LOG.debug("ERROR {}".format(e))
            if retry >= max_retry - 1:
                raise
            time.sleep(retry + 1)


def get_revision_changes(dst, src):
    return make_patch(dst, src).patch


def retry(ExceptionToCheck=Exception, tries=5, delay=1, backoff=2, logger=None):
    def deco_retry(f):
        @functools.wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except (SystemExit, KeyboardInterrupt):
                    raise
                except ExceptionToCheck, e:
                    if logger:
                        logger.error("%s: %s, retrying in %d seconds...",
                                     type(e).__name__, str(e), mdelay)
                    for i in range(int(10 * mdelay)):
                        time.sleep(0.1)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)
        return f_retry
    return deco_retry
