
class BaseCommand(object):
    help = ''

    @staticmethod
    def add_arguments(parser):
        pass

    def check_arguments(self, args):
        pass

    def patch_tender(self, patcher, tender, doc):
        pass
