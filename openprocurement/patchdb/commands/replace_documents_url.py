import re
from copy import deepcopy
from openprocurement.patchdb.commands import BaseCommand


class Command(BaseCommand):
    help = 'Replace domain in documents url'
    required_document_fields = set(['id', 'title', 'format', 'url'])

    def add_arguments(self, parser):
        parser.add_argument('--url-search', default='',
                            help='URL to search (regexp)')
        parser.add_argument('--url-replace', default='',
                            help='URL to replace')

    def check_arguments(self, args):
        if not args.url_search:
            raise ValueError("Nothing to search")
        self.url_search = re.compile(args.url_search)
        self.url_replace = args.url_replace

    def document_replace_url(self, doc):
        if self.url_search.search(doc['url']):
            doc['url'] = self.url_search.sub(self.url_replace, doc['url'])

    def recursive_find_and_replace(self, root):
        if isinstance(root, dict):
            if set(root.keys()) >= self.required_document_fields:
                self.document_replace_url(root)
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
