from util.FeatureCalculation import featureBytesPerSecBwd, featureBytesPerSecFwd, featureTotalLenPkts, featureTotalPkts
from Loader.Dataset import Dataset
from util.UnitConversion import BytesPSec, Bytes, DateString, Generic, Microseconds

class USBIDS2021(Dataset):
    FEATURE_MAP = {
        "Flow ID":("ID",Generic),
        "Src IP": ("Src IP",Generic),
        "Src Port": ("Src Port",Generic),
        "Dst IP": ("Dst IP",Generic),
        "Dst Port": ("Dst Port",Generic),
        "Protocol": ("Protocol",Generic),
        "Timestamp": ("Timestamp",DateString),
        "Flow Duration": ("Flow Duration",Microseconds),
        "Total Fwd Packet": ("Total Fwd Packets",Generic),
        "Total Bwd packets": ("Total Bwd Packets",Generic),
        "Total Length of Bwd Packet": ("Total Length of Bwd Packets",Bytes),
        "Total Length of Fwd Packet": ("Total Length of Fwd Packets",Bytes),
        "Fwd Packet Length Max": ("Fwd Packet Length Max",Bytes),
        "Fwd Packet Length Min": ("Fwd Packet Length Min",Bytes),
        "Fwd Packet Length Mean": ("Fwd Packet Length Mean",Bytes),
        "Fwd Packet Length Std": ("Fwd Packet Length Std",Bytes),
        "Bwd Packet Length Max": ("Bwd Packet Length Max",Bytes),
        "Bwd Packet Length Min": ("Bwd Packet Length Min",Bytes),
        "Bwd Packet Length Mean": ("Bwd Packet Length Mean",Bytes),
        "Bwd Packet Length Std": ("Bwd Packet Length Std",Bytes),
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
        "Fwd Packets/s": ("Fwd Flow Packets/s",Generic),
        "Bwd Packets/s": ("Bwd Flow Packets/s",Generic),
        "Packet Length Min": ("Packet Length Min",Bytes),
        "Packet Length Max": ("Packet Length Max",Bytes),
        "Packet Length Mean": ("Packet Length Mean",Bytes),
        "Packet Length Std": ("Packet Length Std",Bytes),
        "Packet Length Variance": ("Packet Length Variance",Bytes),
        "Down/Up Ratio": ("Down/Up Ratio",Generic),
        "Label": ("Label",Generic)
    }
    CALCULABLE_FEATURES = [
        ("Fwd Flow Byte/s", featureBytesPerSecFwd),
        ("Bwd Flow Byte/s", featureBytesPerSecBwd),
        ("Total Length of Packets", featureTotalLenPkts),
        ("Total Packets", featureTotalPkts)
    ]

    def __init__(self, filepath, name, calculateFeatures=True):
        super().__init__(filepath, name, calculateFeatures)
