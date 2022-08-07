from util.FeatureCalculation import featureBytesPerSec, featureIATMean, featureID, featurePktLenMean, featurePktsPerSec, featurePktsPerSecBwd, featurePktsPerSecFwd, featureTotalLenPkts, featureTotalPkts, featureDownUpRatio
from Loader.Dataset import Dataset
from util.UnitConversion import BitsPSec, Bytes, Generic, Milliseconds, ProtocolStr, Seconds, UnixTime
import pandas as pd
class UNSWNB15(Dataset):
    FEATURE_MAP = {
        "srcip": ("Src IP",None),
        "sport": ("Src Port",None),
        "dstip": ("Dst IP",None),
        "dsport": ("Dst Port",None),
        "proto": ("Protocol",ProtocolStr),
        "dur": ("Flow Duration",None),
        "sbytes": ("Total Length of Fwd Packets",None),
        "dbytes": ("Total Length of Bwd Packets",None),
        "Sload": ("Fwd Flow Byte/s",BitsPSec),
        "Dload": ("Bwd Flow Byte/s",BitsPSec),
        "Spkts": ("Total Fwd Packets",None),
        "Dpkts": ("Total Bwd Packets",None),
        "smeansz": ("Fwd Packet Length Mean",None),
        "dmeansz": ("Bwd Packet Length Mean",None),
        "Stime": ("Timestamp",None),
        "Sintpkt": ("Fwd IAT Mean",Milliseconds),
        "Dintpkt": ("Bwd IAT Mean",Milliseconds),
        "attack_cat": ("Label",None),
    }

    CALCULABLE_FEATURES = [
        ("ID", featureID),
        ("Total Length of Packets", featureTotalLenPkts),
        ("Total Packets", featureTotalPkts),
        ("Packet Length Mean", featurePktLenMean),
        ("Flow Bytes/s", featureBytesPerSec),
        ("Flow Packets/s", featurePktsPerSec),
        ("Fwd Flow Packets/s", featurePktsPerSecFwd),
        ("Bwd Flow Packets/s", featurePktsPerSecBwd),
        ("Flow IAT Mean", featureIATMean),
        ("Down/Up Ratio", featureDownUpRatio)
    ]

    ORIGINAL_FEATURES = [
        "srcip","sport","dstip","dsport","proto","state","dur","sbytes","dbytes","sttl",
        "dttl","sloss","dloss","service","Sload","Dload","Spkts","Dpkts","swin","dwin",
        "stcpb","dtcpb","smeansz","dmeansz","trans_depth","res_bdy_len","Sjit","Djit",
        "Stime","Ltime","Sintpkt","Dintpkt","tcprtt","synack","ackdat","is_sm_ips_ports",
        "ct_state_ttl","ct_flw_http_mthd","is_ftp_login","ct_ftp_cmd","ct_srv_src",
        "ct_srv_dst","ct_dst_ltm","ct_src_ ltm","ct_src_dport_ltm","ct_dst_sport_ltm",
        "ct_dst_src_ltm","attack_cat","Label"
    ]

    def __init__(self, filepath, name, calculateFeatures=True):
        super().__init__(filepath, name, calculateFeatures)
        self.reset()
        #self.preProcess["preCalc"].append(self.replaceBadValues)
        
        self.addPreCalculateProcessor(self.replaceBadValues)
    
    def replaceBadValues(self, df):
        df["Label"] = df["Label"].fillna("BENIGN")
        df.loc[df["Flow Duration"] == 0, "Flow Duration"] = 0.0000001

        return df

    def reset(self):
        self.reader = pd.read_csv(self.filepath,chunksize=self.chunksize,skipinitialspace=True, header=None, names=self.ORIGINAL_FEATURES)
