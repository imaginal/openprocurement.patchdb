# -*- coding: utf-8 -*-
import os
import sys
import argparse
import importlib
from ConfigParser import ConfigParser
from couchdb import Server, Session
from couchdb.http import ResourceConflict

from . import __version__
from .utils import get_with_retry, get_revision_changes, with_retry, LOG
from .models import get_now, generate_id, generate_tender_id, Tender, Plan, Contract, Auction


class PatchApp(object):
    ALLOW_PATCHES = ['cancel_auction', 'clone_tender', 'remove_auction_options', 'remove_auction_period',
                     'replace_documents_url', 'rollback_last_patch', 'update_ts_features']
    ALLOW_DOCTYPE = ['Tender', 'Plan', 'Contract', 'Auction']

    def __init__(self, argv):
        self.load_commands()
        self.parse_arguments(argv)
        self.has_error = False
        self.total = 0
        self.patched = 0
        self.changed = 0
        self.created = 0
        self.saved = 0
        self.lock = None

    def parse_arguments(self, argv):
        formatter_class = argparse.RawDescriptionHelpFormatter
        epilog = "patchdb v{}".format(__version__)
        parser = argparse.ArgumentParser(
            description='Console utility for patching tender documents direct in couchdb',
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
        common.add_argument('-L', '--label', metavar='CUSTOM_LABEL', default='',
                            help='custom patch label, will be saved at revisions')
        common.add_argument('-a', '--after', metavar='TENDER_ID',
                            help='start tenderID in format UA-YYYY-MM-DD')
        common.add_argument('-b', '--before', metavar='TENDER_ID',
                            help='end tenderID in format UA-YYYY-MM-DD')
        common.add_argument('-t', '--tenderID', action='append',
                            help='process only these tenderID (may be multiple times)')
        common.add_argument('-i', '--id', dest='docid', action='append',
                            help='process only these hex id (may be multiple times)')
        common.add_argument('-x', '--except', action='append', dest='ignore_id',
                            help='ignore some tenders by hex tender.id (not tenderID)')
        common.add_argument('-p', '--procedure', action='append', dest='method_type',
                            help='filter by tender procurementMethodType (default any)')
        common.add_argument('-s', '--status', action='append',
                            help='filter by tender status (default any)')
        common.add_argument('-T', '--type', action='append', dest='doc_type',
                            help='filter by model type (default Tender)')
        common.add_argument('-n', '--limit', type=int, default=-1,
                            help='stop after found and patch N tenders')
        common.add_argument('-u', '--api-url', default='127.0.0.1:8080',
                            help='url to API (default 127.0.0.1:8080) or "disable"')
        common.add_argument('-m', '--dateModified', action='store_true',
                            help='update tender.dateModified (default no)')
        common.add_argument('--write', action='store_true',
                            help='save changes to couch database (default no)')

        for key in self.ALLOW_PATCHES:
            cmd = self.commands[key]
            cmd.parser = subparsers.add_parser(key, help=cmd.help, parents=[common], epilog=epilog)
            group = cmd.parser.add_argument_group('{} arguments'.format(key))
            cmd.add_arguments(group)

        self.args = args = parser.parse_args(argv[1:])
        self.patch_label = args.label or args.patch_name
        if not self.args.doc_type:
            self.args.doc_type = ['Tender']
        elif not set(self.ALLOW_DOCTYPE) >= set(self.args.doc_type):
            print parser.prog, "error: unknown --type", self.args.doc_type, "allowed", self.ALLOW_DOCTYPE
            sys.exit(1)

        patch_class = self.commands.get(args.patch_name)
        self.patch = patch_class()
        try:
            self.patch.check_arguments(args)
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

    def safe_inc(self, attr):
        if self.lock:
            with self.lock:
                setattr(self, attr, getattr(self, attr, 0) + 1)
        else:
            setattr(self, attr, getattr(self, attr, 0) + 1)

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
        self.safe_inc('created')
        if not self.args.write:
            LOG.info('Not saved')
            return False
        return self.save_with_retry(tender)

    @with_retry(tries=3, raise_on=ResourceConflict)
    def save_with_retry(self, new):
        doc_id, doc_rev = self.db.save(new)
        LOG.info("Saved {} rev {}".format(doc_id, doc_rev))
        self.safe_inc('saved')
        return True

    def save_tender(self, tender, old, new):
        patch = get_revision_changes(new, old)
        if not patch:
            LOG.info('{} {} no changes made'.format(tender.id, tender.tenderID))
            return
        new['revisions'].append({
            'author': 'patchdb/{}'.format(self.patch_label),
            'changes': patch,
            'date': get_now().isoformat(),
            'rev': tender.rev})
        LOG.info('{} {} changes {}'.format(tender.id, tender.tenderID, patch))
        self.safe_inc('changed')
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

    @with_retry(tries=3)
    def patch_tender(self, docid):
        args = self.args

        doc = self.db.get(docid)

        if self.has_error:
            return

        if not doc:
            raise IndexError("Document Not Found {}".format(docid))

        doc_type = doc.get('doc_type')

        if not doc_type:
            LOG.debug("Ignore {} by empty doc_type".format(docid))
            return
        if doc_type not in args.doc_type:
            LOG.debug("Ignore {} by doc_type {}".format(docid, doc_type))
            return

        if doc_type == 'Tender':
            tender = Tender().import_data(doc, partial=True)
            if not tender.tenderID:
                raise ValueError("Bad tenderID {}".format(docid))
        elif doc_type == 'Plan':
            tender = Plan().import_data(doc, partial=True)
            if not tender.planID:
                raise ValueError("Bad planID {}".format(docid))
        elif doc_type == 'Contract':
            tender = Contract().import_data(doc, partial=True)
            if not tender.contractID:
                raise ValueError("Bad contractID {}".format(docid))
        elif doc_type == 'Auction':
            tender = Auction().import_data(doc, partial=True)
            if not tender.auctionID:
                raise ValueError("Bad auctionID {}".format(docid))
        else:
            LOG.debug("Ignore {} by doc_type {}".format(docid, doc_type))
            return
        if args.after and tender.tenderID < args.after:
            LOG.debug("Ignore {} by tenderID {}".format(docid, tender.tenderID))
            return
        if args.before and tender.tenderID > args.before:
            LOG.debug("Ignore {} by tenderID {}".format(docid, tender.tenderID))
            return
        if args.tenderID and tender.tenderID not in args.tenderID:
            LOG.debug("Ignore {} by tenderID {} not in -t/--tenderID".format(docid, tender.tenderID))
            return
        if args.status and tender.status not in args.status:
            LOG.debug("Ignore {} by status {}".format(docid, tender.status))
            return
        if args.docid and tender.id not in args.docid:
            LOG.debug("Ignore {} by tender.id in -i/--id".format(docid))
            return
        if args.ignore_id and tender.id in args.ignore_id:
            LOG.debug("Ignore {} by tender.id in -x/--except".format(docid))
            return
        if args.method_type and tender.procurementMethodType not in args.method_type:
            LOG.debug("Ignore {} by procurementMethodType {}".format(docid, tender.procurementMethodType))
            return

        LOG.debug("Tender {} {} {} {}".format(docid, tender.tenderID, tender.status, tender.dateModified))

        self.patch.patch_tender(self, tender, doc)

        self.safe_inc('patched')

    def init_app(self):
        config = ConfigParser()
        config.read(self.args.config)
        settings = dict(config.items(self.args.section))

        db_name = os.environ.get('DB_NAME', settings['couchdb.db_name'])
        self.server = Server(settings.get('couchdb.url'),
                             session=Session(retry_delays=range(10)))
        self.db = self.server[db_name]
        self.server_id = settings.get('id', '1')

        # init api client
        self.api_url = self.args.api_url
        if self.api_url != 'disable':
            if '://' not in self.api_url:
                self.api_url = 'http://' + self.api_url
            if '/api/' not in self.api_url:
                self.api_url += '/api/2.3/tenders'
            get_with_retry(self.api_url, 'data')

    def patch_thread(self, modulus=None, remainder=None):
        try:
            self.patch_all(modulus, remainder)
        except Exception:
            self.has_error = True
            raise

    def patch_all(self, modulus=None, remainder=None):
        args = self.args

        docs_list = args.docid if args.docid else self.db

        for docid in docs_list:
            if self.has_error:
                break
            if args.limit > 0 and self.changed >= args.limit:
                LOG.info("Stop after limit {} reached".format(self.changed))
                break
            if modulus and remainder is not None:
                try:
                    docno = int(docid[:8], 16)
                except ValueError:
                    docno = 1
                if docno % modulus != remainder:
                    continue

            self.patch_tender(docid)

            self.safe_inc('total')

        if not modulus:
            self.print_total()

    def print_total(self):
        LOG.info("Patched {} of {} docs {} changed {} saved".format(
                 self.patched, self.total, self.changed, self.saved))
