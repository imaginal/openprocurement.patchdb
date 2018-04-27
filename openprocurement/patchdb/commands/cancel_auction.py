from copy import deepcopy
from datetime import timedelta
from openprocurement.patchdb.commands import BaseCommand
from openprocurement.patchdb.models import get_now


class Command(BaseCommand):
    help = 'Cancel auction planning by remove auctionPeriod.startDate'

    def add_arguments(self, parser):
        parser.add_argument('--auction-date', default='',
                            help='auctionPeriod.startDate in ISO format')

    def check_arguments(self, args):
        if len(args.auction_date) < 10:
            raise ValueError("--auction-date required full date YYYY-MM-DD")
        self.auction_date = args.auction_date

    def patch_tender(self, patcher, tender, doc):
        if tender.status not in ('active.tendering', 'active.auction'):
            return
        changed = False
        if 'lots' in doc and doc['lots']:
            new = deepcopy(doc)
            for lot in new['lots']:
                if 'auctionPeriod' in lot and 'startDate' in lot['auctionPeriod'] and lot['auctionPeriod']['startDate']:
                    if self.auction_date and lot['auctionPeriod']['startDate'].startswith(self.auction_date):
                        lot['auctionPeriod'].pop('startDate')
                        changed = True
        else:
            if 'auctionPeriod' in doc and 'startDate' in doc['auctionPeriod'] and doc['auctionPeriod']['startDate']:
                if self.auction_date and doc['auctionPeriod']['startDate'].startswith(self.auction_date):
                    new = deepcopy(doc)
                    new['auctionPeriod'].pop('startDate')
                    changed = True
        if changed:
            new['next_check'] = (get_now() + timedelta(minutes=10)).isoformat()
            patcher.save_tender(tender, doc, new)
            patcher.check_tender(tender, tender.tenderID)
