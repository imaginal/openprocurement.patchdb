from jsonpatch import apply_patch
from openprocurement.patchdb.commands import BaseCommand


class Command(BaseCommand):
    help = 'Rollback last applieed patch by reverse revision changes'

    @staticmethod
    def add_arguments(parser):
        parser.add_argument('--date-after', default='',
                            help='Min patch revision date ISO format')
        parser.add_argument('--date-before', default='',
                            help='Max patch revision date ISO format')
        parser.add_argument('--patch-label', default='',
                            help='Desired patch label (name of patch)')

    def check_arguments(self, args):
        if not args.patch_label:
            raise ValueError("--patch-label is required")
        self.patch_author = "patchdb/{}".format(args.patch_label)
        self.date_after = args.date_after
        self.date_before = args.date_before

    def patch_tender(self, patcher, tender, doc):
        if not tender.revisions:
            return
        revision = tender.revisions[-1]
        if self.patch_author and self.patch_author != revision.author:
            return
        if self.date_after and self.date_after > revision.date:
            return
        if self.date_before and self.date_before < revision.date:
            return
        new = apply_patch(doc, revision.changes)
        patcher.save_tender(tender, doc, new)
        patcher.check_tender(tender, tender.tenderID)
