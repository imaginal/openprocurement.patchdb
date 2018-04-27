from copy import deepcopy
from openprocurement.patchdb.commands import BaseCommand


class Command(BaseCommand):
    help = 'Remove unnecessary auctionOptions from tender'

    def patch_tender(self, patcher, tender, doc):
        if tender.status in ('complete', 'unsuccessful', 'cancelled'):
            return
        if 'auctionOptions' in doc and doc['auctionOptions']:
            new = deepcopy(doc)
            new.pop('auctionOptions')
            patcher.save_tender(tender, doc, new)
            patcher.check_tender(tender, tender.tenderID)
