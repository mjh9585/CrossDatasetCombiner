from enum import Enum

from Loader import USBIDS

class Datasets(Enum):
    CIC_2020 = 1
    CIC_2019 = 2
    CIC_2018 = 3
    CIC_2017 = 4
    IOT23_2020 = 5
    USBIDS_2021 = 6
    UNSW_NB15_2015 = 7

def loadDataset(dataset : Datasets, path : str, note : str=None):
    print(f'going to load {dataset} for "{path}" with note: {note}')
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
        pass
    elif dataset == Datasets.USBIDS_2021:
        print("loading USBIDS2021")
        return USBIDS.USBIDS2021(path,note)
    else:
        print("invalid dataset")
        raise ValueError