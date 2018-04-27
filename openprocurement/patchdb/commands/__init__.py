
class BaseCommand(object):
    help = ''

    def add_arguments(self, parser):
        pass

    def check_arguments(self, args):
        pass

    def patch_tender(self, patcher, tender, doc):
        pass
