# -*- coding: utf-8 -*-
import os
import sys
import time
import logging
import argparse
import requests
from copy import deepcopy
from ConfigParser import ConfigParser
from couchdb import Server, Session
from datetime import timedelta
from jsonpatch import make_patch
from .models import Tender, get_now, parse_local_date


__version__ = '0.1b1'

LOG = logging.getLogger('patchdb')
SESSION = requests.Session()


def parse_command_line(argv):
    formatter_class = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(description='Console utility for patching tender documents in couchdb',
                                     formatter_class=formatter_class)
    parser.add_argument('config', help='path to openprocurement.api.ini')
    parser.add_argument('patch_name', help='name of the applied patch')
    parser.add_argument('--version', action='version',
                        version='%(prog)s {}'.format(__version__))
    parser.add_argument('-v', '--verbose', dest='verbose_count',
                        action='count', default=0,
                        help='for more verbose use multiple times')
    parser.add_argument('-l', '--log',
                        type=argparse.FileType('at'), default=sys.stderr,
                        help='redirect log to a file')
    parser.add_argument('-o', '--output',
                        type=argparse.FileType('w'), default=sys.stdout,
                        help='redirect output to a file')
    parser.add_argument('-k', '--section', default='app:api',
                        help='section name in config, default [app:api]')
    parser.add_argument('-a', '--after', metavar='TENDER_ID',
                        help='start tenderID in format UA-YYYY-MM-DD')
    parser.add_argument('-b', '--before', metavar='TENDER_ID',
                        help='end tenderID in format UA-YYYY-MM-DD')
    parser.add_argument('-s', '--status', action='append',
                        help='filter by tender status (default any)')
    parser.add_argument('-i', '--ignore', action='append',
                        help='ignore some tenders by tender.id (not tenderID)')
    parser.add_argument('-u', '--api-url', default='127.0.0.1:8080',
                        help='url to API (default 127.0.0.1:8080)')
    # patch auction
    parser.add_argument('--auction-date', default='',
                        help='auctionPeriod.startDate in ISO format')
    # patch wrte
    parser.add_argument('--write', action='store_true',
                        help='Allow changes to couch database')

    args = parser.parse_args(argv[1:])
    return args


def get_with_retry(url, require_text=''):
    for i in range(5):
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
            if i > 3:
                raise
        for s in range(i * 10):
            time.sleep(0.1)


def force_unicode(s, onerror=u""):
    if not s:
        return u""
    try:
        u = s.decode('utf-8')
    except UnicodeEncodeError:
        u = onerror
    return u


def get_revision_changes(dst, src):
    return make_patch(dst, src).patch


class PatchApp(object):
    ALLOW_PATCHES = ['cancel_auction']

    def __init__(self, args):
        if args.patch_name not in self.ALLOW_PATCHES:
            raise ValueError("Unknown patch name '{}' allow one of {}"
                            .format(args.patch_name, self.ALLOW_PATCHES))
        if args.patch_name == 'cancel_auction' and len(args.auction_date) < 10:
            raise ValueError("--auction-date required full date YYYY-MM-DD")
        self.args = args

    def init_client(self):
        self.api_url = self.args.api_url
        if '://' not in self.api_url:
            self.api_url = 'http://' + self.api_url
        if '/api/' not in self.api_url:
            self.api_url += '/api/2.3/tenders'
        get_with_retry(self.api_url, 'data')

    def save_tender(self, tender, old, new):
        patch = get_revision_changes(new, old)
        if not patch:
            LOG.info('{} {} no changes made'.format(tender.id, tender.tenderID))
            return
        new['revisions'].append({'author': 'patchdb', 'changes': patch, 'rev': tender.rev})
        LOG.info('{} {} changes {}'.format(tender.id, tender.tenderID, patch))
        self.changed += 1
        if not self.args.write:
            LOG.info('Not saved')
            return False
        doc_id, doc_rev = self.db.save(new)
        LOG.info('Saved {} rev {}'.format(doc_id, doc_rev))
        self.saved += 1
        return True

    def check_tender(self, tender, check_text):
        url = "{}/{}".format(self.api_url, tender.id)
        get_with_retry(url, check_text)
        LOG.debug("Check OK, found {}".format(check_text))

    def cancel_auction(self, tender, doc):
        if tender.status not in ('active.tendering', 'active.auction'):
            return
        changed = False
        if 'lots' in doc and doc['lots']:
            new = deepcopy(doc)
            for lot in new['lots']:
                if 'auctionPeriod' in lot and 'startDate' in lot['auctionPeriod'] and lot['auctionPeriod']['startDate']:
                    if self.args.auction_date and lot['auctionPeriod']['startDate'].startswith(self.args.auction_date):
                        lot['auctionPeriod'].pop('startDate')
                        changed = True
        else:
            if 'auctionPeriod' in doc and 'startDate' in doc['auctionPeriod'] and doc['auctionPeriod']['startDate']:
                if self.args.auction_date and doc['auctionPeriod']['startDate'].startswith(self.args.auction_date):
                    new = deepcopy(doc)
                    new['auctionPeriod'].pop('startDate')
                    changed = True
        if changed:
            new['next_check'] = (get_now() + timedelta(minutes=10)).isoformat()
            self.save_tender(tender, doc, new)
            self.check_tender(tender, tender.tenderID)

    def patch(self):
        args = self.args
        config = ConfigParser()
        config.read(args.config)
        settings = dict(config.items(args.section))

        self.init_client()

        db_name = os.environ.get('DB_NAME', settings['couchdb.db_name'])
        server = Server(settings.get('couchdb.url'), session=Session(retry_delays=range(10)))
        self.db = db = server[db_name]

        self.total = self.changed = self.saved = 0

        patch_func = getattr(self, args.patch_name)

        for docid in db:
            doc = db.get(docid)
            if doc.get('doc_type') != 'Tender':
                continue
            tender = Tender().import_data(doc, partial=True)
            if not tender.tenderID:
                raise ValueError("Bad tenderID {}".format(docid))
            if args.after and tender.tenderID < args.after:
                LOG.debug("Ignore {} by tenderID {}".format(docid, tender.tenderID))
                continue
            if args.before and tender.tenderID > args.before:
                LOG.debug("Ignore {} by tenderID {}".format(docid, tender.tenderID))
                continue
            if args.status and tender.status not in args.status:
                LOG.debug("Ignore {} by status {}".format(docid, tender.status))
                continue
            if args.ignore and tender.id in args.ignore:
                LOG.debug("Ignore {} by tender.id in command line args".format(docid))
                continue

            LOG.debug("Tender {} {} {} {}".format(docid, tender.tenderID, tender.status, tender.dateModified))

            self.total += 1

            patch_func(tender, doc)

        LOG.info("Total {} tenders {} changed {} saved".format(self.total, self.changed, self.saved))
        del self.db


def main():
    args = parse_command_line(sys.argv)
    level = max(3 - args.verbose_count, 0) * 10
    logging.basicConfig(stream=args.log, level=level,
                        format='%(asctime)-15s %(levelname)s %(message)s')
    LOG.setLevel(level)

    app = PatchApp(args)

    try:
        app.patch()
    except KeyboardInterrupt:
        LOG.error('Program interrupted!')
    finally:
        logging.shutdown()


if __name__ == '__main__':
    sys.exit(main())
