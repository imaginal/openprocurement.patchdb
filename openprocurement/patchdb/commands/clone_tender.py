from copy import deepcopy
from openprocurement.patchdb.models import Tender
from openprocurement.patchdb.commands import BaseCommand


class Command(BaseCommand):
    help = 'Create copy of tender document with same data but with new id and tenderID'

    @staticmethod
    def add_arguments(parser):
        parser.add_argument('--clone-count', type=int, default=1,
                            help='number of copies to create')

    def check_arguments(self, args):
        if args.clone_count < 1 or args.clone_count > 10:
            raise ValueError("--clone-count must be in 1 .. 10")
        self.clone_count = args.clone_count

    def patch_tender(self, patcher, tender, doc):
        for n in range(self.clone_count):
            new = deepcopy(doc)
            new.pop('_rev')
            for key in ('title', 'title_en', 'title_ru', 'description', 'description_en', 'description_ru'):
                if key in new:
                    new[key] += ' (clone {} of {} parent {})'.format(n+1, tender.id, tender.tenderID)
            patcher.create_tender(new)
            new_tender = Tender().import_data(new, partial=True)
            patcher.check_tender(new_tender, new_tender.tenderID, check_write=True)

        patcher.check_tender(tender, tender.tenderID)
