import os
import re
from time import sleep
from uuid import uuid4
from pytz import timezone
from datetime import datetime
from iso8601 import parse_date, ParseError
from couchdb_schematics.document import SchematicsDocument
from schematics.exceptions import ConversionError, ValidationError
from schematics.models import Model
from schematics.types import BaseType, StringType
from schematics.types.compound import DictType, ListType, ModelType as BaseModelType


TZ = timezone(os.environ['TZ'] if 'TZ' in os.environ else 'Europe/Kiev')
DB_SHADOW = dict()


def get_now():
    return datetime.now(TZ)


def generate_id():
    return uuid4().hex


def shadow_get(db, key, default):
    if key in DB_SHADOW:
        return DB_SHADOW[key]
    return db.get(key, default)


def shadow_save(db, doc, write=False):
    if write:
        return db.save(doc)
    key = doc['_id']
    DB_SHADOW[key] = doc


def generate_tender_id(tenderID, db, server_id=None, write=False):
    # for UA-2017-07-12-000293-c
    # group(1): UA-
    # group(2): 2017-07-12
    # group(3): 000293
    # group(4): -c
    m = re.match(r'([\w\d\-]{1,10}-)(\d{4}-\d{2}-\d{2})-(\d{6})(-[\w\d]{1,3})?', tenderID)
    if not m:
        raise ValueError('tenderID dont match standart regex')
    if not server_id and m.group(4):
        server_id = m.group(4)[1:]
    key = m.group(2)
    tenderIDdoc = 'tenderID_' + server_id if server_id else 'tenderID'
    retry = 10
    while retry > 0:
        retry -= 1
        try:
            tenderID = shadow_get(db, tenderIDdoc, {'_id': tenderIDdoc})
            index = tenderID.get(key, 1)
            tenderID[key] = index + 1
            shadow_save(db, tenderID, write)
        except Exception as e:  # pragma: no cover
            if not retry:
                raise e
            sleep(0.1)
        else:
            break
    return '{}{}-{:06}{}'.format(m.group(1), m.group(2), index, server_id and '-' + server_id)


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
    procurementMethodType = StringType()
    revisions = ListType(ModelType(Revision), default=list())
    tenderID = StringType()
    status = StringType()
