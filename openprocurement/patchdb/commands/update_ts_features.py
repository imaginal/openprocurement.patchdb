from copy import deepcopy
from openprocurement.patchdb.commands import BaseCommand


class Command(BaseCommand):
    help = 'Remove featureOf, relatedItem add default featureType:required in aboveThresholdTS'

    def patch_tender(self, patcher, tender, doc):
        if tender.procurementMethodType != 'aboveThresholdTS':
            return
        if 'features' in doc and doc['features']:
            new = deepcopy(doc)
            for feature in new['features']:
                feature.pop('featureOf', None)
                feature.pop('relatedItem', None)
                if 'featureType' not in feature:
                    feature['featureType'] = 'required'
            patcher.save_tender(tender, doc, new)
            patcher.check_tender(tender, tender.tenderID)
