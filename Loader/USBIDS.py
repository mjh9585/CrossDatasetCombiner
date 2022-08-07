from util.FeatureCalculation import featureBytesPerSecBwd, featureBytesPerSecFwd, featureTotalLenPkts, featureTotalPkts
from Loader.Dataset import Dataset
from util.UnitConversion import BytesPSec, Bytes, DateString, Generic, Microseconds, Unit
import pandas as pd
import time
from datetime import datetime

class USBDateString(Unit):
    @staticmethod
    def convert(val):
        if(type(val) is pd.Series):
            return val.apply(lambda t: time.mktime(datetime.strptime(t,"%m/%d/%Y %I:%M:%S %p").timetuple()))
            
        return time.mktime(datetime.strptime(val,"%m/%d/%Y %I:%M:%S %p").timetuple())

class USBIDS2021(Dataset):
    FEATURE_MAP = {
        "Flow ID":("ID",None),
        "Src IP": ("Src IP",None),
        "Src Port": ("Src Port",None),
        "Dst IP": ("Dst IP",None),
        "Dst Port": ("Dst Port",None),
        "Protocol": ("Protocol",None),
        "Timestamp": ("Timestamp",USBDateString),
        "Flow Duration": ("Flow Duration",Microseconds),
        "Total Fwd Packet": ("Total Fwd Packets",None),
        "Total Bwd packets": ("Total Bwd Packets",None),
        "Total Length of Bwd Packet": ("Total Length of Bwd Packets",None),
        "Total Length of Fwd Packet": ("Total Length of Fwd Packets",None),
        "Fwd Packet Length Max": ("Fwd Packet Length Max",None),
        "Fwd Packet Length Min": ("Fwd Packet Length Min",None),
        "Fwd Packet Length Mean": ("Fwd Packet Length Mean",None),
        "Fwd Packet Length Std": ("Fwd Packet Length Std",None),
        "Bwd Packet Length Max": ("Bwd Packet Length Max",None),
        "Bwd Packet Length Min": ("Bwd Packet Length Min",None),
        "Bwd Packet Length Mean": ("Bwd Packet Length Mean",None),
        "Bwd Packet Length Std": ("Bwd Packet Length Std",None),
        "Flow Bytes/s": ("Flow Bytes/s",BytesPSec),
        "Flow Packets/s": ("Flow Packets/s",BytesPSec),
        "Flow IAT Mean": ("Flow IAT Mean",Microseconds),
        "Flow IAT Std": ("Flow IAT Std",Microseconds),
        "Flow IAT Max": ("Flow IAT Max",Microseconds),
        "Flow IAT Min": ("Flow IAT Min",Microseconds),
        "Fwd IAT Total": ("Fwd IAT Total",Microseconds),
        "Fwd IAT Mean": ("Fwd IAT Mean",Microseconds),
        "Fwd IAT Std": ("Fwd IAT Std",Microseconds),
        "Fwd IAT Max": ("Fwd IAT Max",Microseconds),
        "Fwd IAT Min": ("Fwd IAT Min",Microseconds),
        "Bwd IAT Total": ("Bwd IAT Total",Microseconds),
        "Bwd IAT Mean": ("Bwd IAT Mean",Microseconds),
        "Bwd IAT Std": ("Bwd IAT Std",Microseconds),
        "Bwd IAT Max": ("Bwd IAT Max",Microseconds),
        "Bwd IAT Min": ("Bwd IAT Min",Microseconds),
        "Fwd Packets/s": ("Fwd Flow Packets/s",None),
        "Bwd Packets/s": ("Bwd Flow Packets/s",None),
        "Packet Length Min": ("Packet Length Min",None),
        "Packet Length Max": ("Packet Length Max",None),
        "Packet Length Mean": ("Packet Length Mean",None),
        "Packet Length Std": ("Packet Length Std",None),
        "Packet Length Variance": ("Packet Length Variance",None),
        "Down/Up Ratio": ("Down/Up Ratio",None),
        "Label": ("Label",None)
    }
    CALCULABLE_FEATURES = [
        ("Fwd Flow Byte/s", featureBytesPerSecFwd),
        ("Bwd Flow Byte/s", featureBytesPerSecBwd),
        ("Total Length of Packets", featureTotalLenPkts),
        ("Total Packets", featureTotalPkts)
    ]

    def __init__(self, filepath, name, calculateFeatures=True):
        super().__init__(filepath, name, calculateFeatures)
        self.reset()

    def reset(self):
        self.reader = pd.read_csv(self.filepath,chunksize=self.chunksize,skipinitialspace=True)


