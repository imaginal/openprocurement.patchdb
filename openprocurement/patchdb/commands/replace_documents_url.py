import re
from copy import deepcopy
from openprocurement.patchdb.commands import BaseCommand


class Command(BaseCommand):
    help = 'Replace domain in documents or auction URL'
    required_document_fields = set(['id', 'title', 'format', 'url'])
    required_auction_fields = set(['id', 'title', 'value', 'auctionUrl'])
    auction_url_search = None
    doc_url_search = None

    @staticmethod
    def add_arguments(parser):
        parser.add_argument('--doc-url-search', default='',
                            help='document URL to search (regexp)')
        parser.add_argument('--doc-url-replace', default='',
                            help='document URL to replace')
        parser.add_argument('--auction-url-search', default='',
                            help='auction URL to search (regexp)')
        parser.add_argument('--auction-url-replace', default='',
                            help='auction URL to replace')

    def check_arguments(self, args):
        if not args.doc_url_search and not args.auction_url_search:
            raise ValueError("Nothing to search")
        if args.doc_url_search:
            self.doc_url_search = re.compile(args.doc_url_search)
        self.doc_url_replace = args.doc_url_replace
        if args.auction_url_search:
            self.auction_url_search = re.compile(args.auction_url_search)
        self.auction_url_replace = args.auction_url_replace

    def document_replace_url(self, doc):
        if self.doc_url_search and self.doc_url_search.search(doc['url']):
            doc['url'] = self.doc_url_search.sub(self.doc_url_replace, doc['url'])

    def auction_replace_url(self, doc):
        if self.auction_url_search and self.auction_url_search.search(doc['auctionUrl']):
            doc['auctionUrl'] = self.auction_url_search.sub(self.auction_url_replace, doc['auctionUrl'])

    def recursive_find_and_replace(self, root):
        if isinstance(root, dict):
            if set(root.keys()) >= self.required_document_fields:
                self.document_replace_url(root)
            if set(root.keys()) >= self.required_auction_fields:
                self.auction_replace_url(root)
            for item in root.values():
                if isinstance(item, (dict, list)):
                    self.recursive_find_and_replace(item)
        elif isinstance(root, list):
            for item in root:
                self.recursive_find_and_replace(item)

    def patch_tender(self, patcher, tender, doc):
        new = deepcopy(doc)
        self.recursive_find_and_replace(new)
        patcher.save_tender(tender, doc, new)
        patcher.check_tender(tender, tender.tenderID)
