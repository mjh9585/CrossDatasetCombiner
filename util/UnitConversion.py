
from abc import ABC
from dateutil.parser import parse
from pandas import Series
import time

class Unit(ABC):
    @staticmethod
    def convert(val):
        pass

class Generic(Unit):
    @staticmethod
    def convert(val):
        return val

## Time
class Seconds(Unit):
    @staticmethod
    def convert(val):
        return val

class Microseconds(Unit):
    @staticmethod
    def convert(val):
        return val/1000000.0

class Milliseconds(Unit):
    @staticmethod
    def convert(val):
        return val/1000.0

## Data Size
class Bytes(Unit):
    @staticmethod
    def convert(val):
        return val

## Data rate
class BytesPSec(Unit):
    @staticmethod
    def convert(val):
        return val
class BitsPSec(Unit):
    @staticmethod
    def convert(val):
        return val/8

## Date
class DateString(Unit):
    @staticmethod
    def convert(val):
        if(type(val) is Series):
            return val.apply(lambda t: time.mktime(parse(t).timetuple()))
            
        return time.mktime(parse(val).timetuple())
        
class UnixTime(Unit):
    @staticmethod
    def convert(val):
        return val

#Protocol
class ProtocolStr(Unit):
    # TODO Map protocol strings to Decimal Number!
    PROTOCOL_MAP = {

    }
    @staticmethod
    def convert(val):
        return val