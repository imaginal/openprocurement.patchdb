from copy import deepcopy
from openprocurement.patchdb.commands import BaseCommand


class Command(BaseCommand):
    help = 'Remove unnecessary auctionPeriod from belowThresholdRFP tender'

    def patch_tender(self, patcher, tender, doc):
        if tender.procurementMethodType != 'belowThresholdRFP':
            return
        new = None
        changed = False
        if 'lots' in doc and doc['lots']:
            new = deepcopy(doc)
            for lot in new['lots']:
                if 'auctionPeriod' in lot and lot['auctionPeriod']:
                    lot.pop('auctionPeriod')
                    changed = True
        if 'auctionPeriod' in doc and doc['auctionPeriod']:
            if not new:
                new = deepcopy(doc)
            new.pop('auctionPeriod')
            changed = True
        if changed:
            patcher.save_tender(tender, doc, new)
            patcher.check_tender(tender, tender.tenderID)
