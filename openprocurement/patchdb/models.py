import os
from pytz import timezone
from datetime import datetime
from iso8601 import parse_date, ParseError
from couchdb_schematics.document import SchematicsDocument
from schematics.exceptions import ConversionError, ValidationError
from schematics.models import Model
from schematics.types import BaseType, StringType
from schematics.types.compound import DictType, ListType, ModelType as BaseModelType


TZ = timezone(os.environ['TZ'] if 'TZ' in os.environ else 'Europe/Kiev')


def get_now():
    return datetime.now(TZ)


def parse_local_date(s):
    date = parse_date(s, None)
    if not date.tzinfo:
        date = TZ.localize(date)
    return date


class IsoDateTimeType(BaseType):
    MESSAGES = {
        'parse': u'Could not parse {0}. Should be ISO8601.',
    }

    def to_native(self, value, context=None):
        if isinstance(value, datetime):
            return value
        try:
            date = parse_date(value, None)
            if not date.tzinfo:
                date = TZ.localize(date)
            return date
        except ParseError:
            raise ConversionError(self.messages['parse'].format(value))
        except OverflowError as e:
            raise ConversionError(e.message)

    def to_primitive(self, value, context=None):
        return value.isoformat()


class Period(Model):
    startDate = IsoDateTimeType()  # The state date for the period.
    endDate = IsoDateTimeType()  # The end date for the period.

    def validate_startDate(self, data, value):
        if value and data.get('endDate') and data.get('endDate') < value:
            raise ValidationError(u"period should begin before its end")


class TenderAuctionPeriod(Period):
    shouldStartAfter = IsoDateTimeType()


class Revision(Model):
    author = StringType()
    date = IsoDateTimeType(default=get_now)
    changes = ListType(DictType(BaseType), default=list())
    rev = StringType()


class ModelType(BaseModelType):
    # disable default strict mode for partial data
    def __init__(self, model_class, **kwargs):
        BaseModelType.__init__(self, model_class, **kwargs)
        if getattr(self, 'strict', False):
            self.strict = False


class Tender(SchematicsDocument, Model):
    dateModified = IsoDateTimeType()
    revisions = ListType(ModelType(Revision), default=list())
    tenderID = StringType()
    status = StringType()
