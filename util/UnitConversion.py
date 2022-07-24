
from abc import ABC
from dateutil.parser import parse
from pandas import Series
import time

class Unit(ABC):
    @staticmethod
    def convert(val):
        pass

class Seconds(Unit):
    @staticmethod
    def convert(val):
        return val

class Microseconds(Unit):
    @staticmethod
    def convert(val):
        return val/1000000.0

class Generic(Unit):
    @staticmethod
    def convert(val):
        return val

class Bytes(Unit):
    @staticmethod
    def convert(val):
        return val

class BytePSec(Unit):
    @staticmethod
    def convert(val):
        return val

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
