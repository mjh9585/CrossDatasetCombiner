from enum import Enum

from Loader import USBIDS,CombinedDataset,UNSWNB15
from Loader.CombinedDataset import CombinationMethod

class Datasets(Enum):
    CIC_2020 = 1
    CIC_2019 = 2
    CIC_2018 = 3
    CIC_2017 = 4
    IOT23_2020 = 5
    USBIDS_2021 = 6
    UNSW_NB15_2015 = 7

def loadDataset(dataset : Datasets, path : str, name:str = None, calcFeatures = True):
    print(f'loading {dataset} from "{path}" with dataset name: {name}')
    if name is None:
        name = str(dataset)

    if dataset == Datasets.CIC_2017:
        pass
    elif dataset == Datasets.CIC_2018:
        pass
    elif dataset == Datasets.CIC_2019:
        pass
    elif dataset == Datasets.CIC_2020:
        pass
    elif dataset == Datasets.IOT23_2020:
        pass
    elif dataset == Datasets.UNSW_NB15_2015:
        #print("loading UNSW_NB15_2015")
        return UNSWNB15.UNSWNB15(path, name, calculateFeatures = calcFeatures)
    elif dataset == Datasets.USBIDS_2021:
        #print("loading USBIDS2021")
        return USBIDS.USBIDS2021(path, name, calculateFeatures = calcFeatures)
    else:
        print("unrecognized dataset, unable to load!")
        raise ValueError

def combineDatasets(*args, method=CombinationMethod.SEQUENTIAL, startTime=None):
    datasets = []
    for d in args:
        if(isinstance(d, list)):
            datasets.extend(d)
        else:
            datasets.append(d)

    commonFeatures = findCommonFeatures(*datasets)

    return CombinedDataset.CombinedDataset(datasets,commonFeatures,method,startTime=startTime)

masterFeatures = [
    #name, column number
    "ID", #1
    "Src IP", #2
    "Src Port", #3
    "Dst IP", #4
    "Dst Port", #5
    "Protocol", #6
    "Timestamp", #7
    "Flow Duration", #8
    "Packet Length Min", #9
    "Packet Length Max", #10
    "Packet Length Mean", #11
    "Packet Length Std", #12
    "Packet Length Variance", #13
    "Fwd Packet Length Min", #14
    "Fwd Packet Length Max", #15
    "Fwd Packet Length Mean", #16
    "Fwd Packet Length Std", #17
    "Bwd Packet Length Min", #18
    "Bwd Packet Length Max", #19
    "Bwd Packet Length Mean", #20
    "Bwd Packet Length Std", #21
    "Flow Bytes/s", #22
    "Fwd Flow Byte/s", #23
    "Bwd Flow Byte/s", #24
    "Flow Packets/s", #25
    "Fwd Flow Packets/s", #26
    "Bwd Flow Packets/s", #27
    "Total Length of Packets", #28
    "Total Length of Fwd Packets", #29
    "Total Length of Bwd Packets", #30
    "Total Packets", # 31
    "Total Fwd Packets", #32
    "Total Bwd Packets", #33
    "Flow IAT Mean", #34
    "Flow IAT Std", #35
    "Flow IAT Max", #36
    "Flow IAT Min", #37
    "Fwd IAT Min", #38
    "Fwd IAT Max", #39
    "Fwd IAT Mean", #40
    "Fwd IAT Std", #41
    "Fwd IAT Total", #42
    "Bwd IAT Min", #43
    "Bwd IAT Max", #44
    "Bwd IAT Mean", #45
    "Bwd IAT Std", #46
    "Bwd IAT Total", #47
    "Down/Up Ratio", #48
    "Label", #49
    "Dataset", #50
]

def findCommonFeatures(*args):
    '''
    Finds and sorts all of the common features
    
    Parameters:
        *args: The datasets to get the common features of

    Returns:
        list of feature names in the order of the common features

    '''
    numDatasets = 0
    #count all the features in the datasets
    features = {}
    for ds in args:
        numDatasets += 1
        for f in ds.getFeatures():
            if f in features:
                features[f] += 1
            else:
                features[f] = 1

    #reorder the features to the format of the master features
    #commonFeatures = [k for k,v in features.items() if v == numDatasets]
    sortedFeatures = []
    for f in masterFeatures:
        if(f in features and features[f] == numDatasets):
            sortedFeatures.append(f)
        
    return sortedFeatures