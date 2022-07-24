from abc import ABC, abstractmethod
from os import rename
import pandas as pd

class Dataset(ABC):
    FEATURE_MAP = {}
    CALCULABLE_FEATURES = {}

    def __init__(self, filepath, note, calculateFeatures=True):
        self.filepath = filepath
        self.note = note
        self.reader = pd.read_csv(filepath,chunksize=10**2,skipinitialspace=True)
        self.doFeatureCalc = calculateFeatures
        self.__colMap = {}
        for col in self.FEATURE_MAP:
            self.__colMap[col] = self.FEATURE_MAP[col][0]

        print(self.__colMap)

    #     if(getColumns):
    #         df = self.reader.get_chunk(1)
    #         self.__getColumns(df)

    # def __getColumns(self, df):
    #     featureCheck = [f.lower() for f in self.FEATURES]

        # for col in df:
        #     if col.lower() in featureCheck

        
        #     if col.lower() in featureCheck and col not in self.FEATURES:
        #         self.reader.columns[col] = self.FEATURES[featureCheck.index(col.lower())]

    def prePreproces(self,df):
        dropCols = []
        for col in df.columns:
            if(col not in self.FEATURE_MAP):
                dropCols.append(col)

        df = df.drop(columns=dropCols)

        for colName in df:
            col = df[colName]
            print(col)
            converter = self.FEATURE_MAP[colName][1]
            print(converter.convert(col))
            df[colName] = converter.convert(col)

        df = df.rename(columns=self.__colMap)

        if(self.doFeatureCalc):
            for feature,funct in self.CALCULABLE_FEATURES.items():
                df[feature] = funct(df)
        
        return df

    def getFeatures(self):
        return self.FEATURE_MAP

    def __iter__(self):
        return self

    def __next__(self):
        df = next(self.reader)
        df = self.prePreproces(df)
        return df