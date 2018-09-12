# -*- coding: utf-8 -*-
import os
import sys
import pytz
import time
import logging
import argparse
import requests
import importlib
import threading
from ConfigParser import ConfigParser
from couchdb import Server, Session
from datetime import datetime
from jsonpatch import make_patch
from openprocurement.patchdb.models import Tender, generate_id, generate_tender_id


__version__ = '0.10b'

LOG = logging.getLogger('patchdb')
SESSION = requests.Session()
TZ = pytz.timezone(os.environ['TZ'] if 'TZ' in os.environ else 'Europe/Kiev')


def get_now():
    return datetime.now(TZ)


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
            LOG.debug("ERROR {}".format(e))
            if i > 3:
                raise
        for s in range(i * 10):
            time.sleep(0.1)


def get_revision_changes(dst, src):
    return make_patch(dst, src).patch


class PatchApp(object):
    ALLOW_PATCHES = ['cancel_auction', 'clone_tender', 'remove_auction_options', 'remove_auction_period',
                     'replace_documents_url', 'update_ts_features']

    def __init__(self, argv):
        self.load_commands()
        self.parse_arguments(argv)
        self.should_exit = False

    def parse_arguments(self, argv):
        formatter_class = argparse.RawDescriptionHelpFormatter
        epilog = "patchdb v{}".format(__version__)
        parser = argparse.ArgumentParser(description='Console utility for patching tender documents direct in couchdb',
                                         formatter_class=formatter_class, epilog=epilog)
        parser.add_argument('--version', action='version',
                            version='%(prog)s {}'.format(__version__))
        subparsers = parser.add_subparsers(dest='patch_name', metavar='patch_name')

        common = argparse.ArgumentParser(add_help=False)
        common.add_argument('-c', '--config', required=True,
                            help='path to openprocurement.api.ini (required)')
        common.add_argument('-r', '--concurrency', type=int, default=0,
                            help='number of concurent threads for performing requests')
        common.add_argument('-v', '--verbose', dest='verbose_count',
                            action='count', default=0,
                            help='for more verbose use multiple times')
        common.add_argument('-q', '--quiet', dest='quiet_count',
                            action='count', default=0,
                            help='for more quiet use multiple times')
        common.add_argument('-l', '--log',
                            type=argparse.FileType('at'), default=sys.stderr,
                            help='redirect log to a file')
        common.add_argument('-k', '--section', default='app:api',
                            help='section name in config, default [app:api]')
        common.add_argument('-a', '--after', metavar='TENDER_ID',
                            help='start tenderID in format UA-YYYY-MM-DD')
        common.add_argument('-b', '--before', metavar='TENDER_ID',
                            help='end tenderID in format UA-YYYY-MM-DD')
        common.add_argument('-t', '--tenderID', action='append',
                            help='process only these tenderID (may be multiple times)')
        common.add_argument('-d', '--docid', action='append',
                            help='process only these hex id (may be multiple times)')
        common.add_argument('-x', '--except', action='append', dest='ignore_id',
                            help='ignore some tenders by hex tender.id (not tenderID)')
        common.add_argument('-p', '--procedure', action='append', dest='method_type',
                            help='filter by tender procurementMethodType (default any)')
        common.add_argument('-s', '--status', action='append',
                            help='filter by tender status (default any)')
        common.add_argument('-n', '--limit', type=int, default=-1,
                            help='stop after found and patch N tenders')
        common.add_argument('-u', '--api-url', default='127.0.0.1:8080',
                            help='url to API (default 127.0.0.1:8080) or "disable"')
        common.add_argument('-m', '--dateModified', action='store_true',
                            help='update tender.dateModified (default no)')
        common.add_argument('--write', action='store_true',
                            help='save changes to couch database (default no)')

        for key in self.ALLOW_PATCHES:
            cmd =  self.commands[key]
            cmd.parser = subparsers.add_parser(key, help=cmd.help, parents=[common], epilog=epilog)
            group = cmd.parser.add_argument_group('{} arguments'.format(key))
            cmd.add_arguments(group)

        self.args = parser.parse_args(argv[1:])
        patch_class = self.commands.get(self.args.patch_name)
        self.patch = patch_class()
        try:
            self.patch.check_arguments(self.args)
        except Exception as e:
            self.patch.parser.print_usage()
            print self.patch.parser.prog, "error:", e
            sys.exit(1)

    def load_commands(self):
        self.commands = {}
        for command in self.ALLOW_PATCHES:
            name = "openprocurement.patchdb.commands.{}".format(command)
            module = importlib.import_module(name)
            self.commands[command] = module.Command

    def init_client(self):
        self.api_url = self.args.api_url
        if self.api_url == 'disable':
            return
        if '://' not in self.api_url:
            self.api_url = 'http://' + self.api_url
        if '/api/' not in self.api_url:
            self.api_url += '/api/2.3/tenders'
        get_with_retry(self.api_url, 'data')

    def create_tender(self, tender):
        if '_rev' in tender:
            raise ValueError('Cant create tender with _rev')
        old_id = tender.get('_id', '-')
        old_tenderID = tender.get('tenderID', '-')
        tender['_id'] = generate_id()
        tender['tenderID'] = generate_tender_id(tender['tenderID'], self.db, self.server_id, write=self.args.write)
        if old_id:
            LOG.info('Clone {} {} to {} {}'.format(old_id, old_tenderID, tender['_id'], tender['tenderID']))
        else:
            LOG.info('Create {} {}'.format(tender['_id'], tender['tenderID']))
        self.created += 1
        if not self.args.write:
            LOG.info('Not saved')
            return False
        return self.save_with_retry(tender)

    def save_with_retry(self, new, max_retry=5):
        retry = max_retry
        while retry:
            retry -= 1
            doc_id = None
            try:
                doc_id, doc_rev = self.db.save(new)
                LOG.info("Saved {} rev {}".format(doc_id, doc_rev))
                self.saved += 1
                retry = 0
            except Exception as e:
                if not doc_id:
                    doc_id, doc_rev = new['_id'], new.get('_rev', None)
                LOG.error("Can't save {} rev {} error {}".format(doc_id, doc_rev, e))
                if not retry:
                    self.should_exit = True
                    raise
                time.sleep(max_retry - retry)

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
        if self.args.dateModified:
            old_dateModified = new.get('dateModified', '')
            new['dateModified'] = get_now().isoformat()
            if old_dateModified and new['dateModified'] < old_dateModified:
                raise ValueError(
                    "Tender {} dateModified {} greater than new {}".format(
                    new['id'], old_dateModified, new['dateModified']))
        return self.save_with_retry(new)

    def check_tender(self, tender, check_text, check_write=False):
        if self.api_url == 'disable':
            LOG.debug("Not checked {}".format(tender.id))
            return
        if check_write and not self.args.write:
            LOG.debug("Not checked {}".format(tender.id))
            return
        url = "{}/{}".format(self.api_url, tender.id)
        get_with_retry(url, check_text)
        LOG.debug("Check OK, found {}".format(check_text))

    def patch_all(self, modulus=None, remainder=None):
        args = self.args
        config = ConfigParser()
        config.read(args.config)
        settings = dict(config.items(args.section))

        self.init_client()

        db_name = os.environ.get('DB_NAME', settings['couchdb.db_name'])
        server = Server(settings.get('couchdb.url'), session=Session(retry_delays=range(10)))
        self.db = db = server[db_name]
        self.server_id = settings.get('id', '1')

        self.total = self.changed = self.created = self.saved = 0

        docs_list = args.docid if args.docid else db

        for docid in docs_list:
            if self.should_exit:
                LOG.info("Exit by user interrupt".format(remainder))
                break
            if modulus and remainder is not None:
                try:
                    docno = int(docid[:8], 16)
                except ValueError:
                    docno = 1
                if docno % modulus != remainder:
                    continue

            doc = db.get(docid)

            if not doc:
                LOG.warning("Not found {}".format(docid))
                continue
            if doc.get('doc_type') == 'Tender':
                tender = Tender().import_data(doc, partial=True)
                if not tender.tenderID:
                    raise ValueError("Bad tenderID {}".format(docid))
            else:
                LOG.debug("Ignore {} by doc_type {}".format(docid, doc.get('doc_type')))
                continue
            if args.after and tender.tenderID < args.after:
                LOG.debug("Ignore {} by tenderID {}".format(docid, tender.tenderID))
                continue
            if args.before and tender.tenderID > args.before:
                LOG.debug("Ignore {} by tenderID {}".format(docid, tender.tenderID))
                continue
            if args.tenderID and tender.tenderID not in args.tenderID:
                LOG.debug("Ignore {} by tenderID {} not in -t/--tenderID".format(docid, tender.tenderID))
                continue
            if args.status and tender.status not in args.status:
                LOG.debug("Ignore {} by status {}".format(docid, tender.status))
                continue
            if args.docid and tender.id not in args.docid:
                LOG.debug("Ignore {} by tender.id in -d/--docid".format(docid))
                continue
            if args.ignore_id and tender.id in args.ignore_id:
                LOG.debug("Ignore {} by tender.id in -x/--except".format(docid))
                continue
            if args.method_type and tender.procurementMethodType not in args.method_type:
                LOG.debug("Ignore {} by procurementMethodType {}".format(docid, tender.procurementMethodType))
                continue

            LOG.debug("Tender {} {} {} {}".format(docid, tender.tenderID, tender.status, tender.dateModified))

            self.patch.patch_tender(self, tender, doc)

            self.total += 1

            if args.limit > 0 and self.total >= args.limit:
                LOG.info("Stop after limit {} reached".format(self.total))
                break

        if not modulus:
            self.print_total()
            self.db = None
            server = None

    def print_total(self, prefix=''):
        LOG.info("{}Total {} tenders {} changed {} saved".format(prefix, self.total, self.changed, self.saved))


def main():
    app = PatchApp(sys.argv)

    level = max(logging.INFO + 10 * app.args.quiet_count - 10 * app.args.verbose_count, logging.DEBUG)
    logformat = '%(asctime)-15s [%(threadName)s] %(levelname)s %(message)s'
    logging.basicConfig(stream=app.args.log, level=level, format=logformat)
    LOG.setLevel(level)
    app.logger = LOG

    if app.args.concurrency > 1:
        LOG.info("Start {} threads...".format(app.args.concurrency))

        threads_list = list()
        modulus = app.args.concurrency
        for remainder in range(modulus):
            thread = threading.Thread(target=app.patch_all, args=(modulus, remainder))
            threads_list.append(thread)
            thread.daemon = True
            thread.start()

        time.sleep(1)

        try:
            for thread in threads_list:
                thread.join()
        except KeyboardInterrupt:
            LOG.error('Program interrupted!')
            app.should_exit = True
            time.sleep(1)
        finally:
            logging.shutdown()

        app.print_total()
        return

    # else single thread
    try:
        app.patch_all()
    except KeyboardInterrupt:
        LOG.error('Program interrupted!')
    finally:
        logging.shutdown()
    return


if __name__ == '__main__':
    sys.exit(main())
