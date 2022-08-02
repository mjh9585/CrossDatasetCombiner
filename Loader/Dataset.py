from abc import ABC, abstractmethod
from os import rename
import pandas as pd
import time
class Dataset(ABC):
    FEATURE_MAP = {}
    CALCULABLE_FEATURES = {}

    def __init__(self, filepath, name, calculateFeatures=True, **kwargs):
        self.filepath = filepath
        self.name = name
        self.chunksize = 10**5
        self.reader = pd.read_csv(filepath,chunksize=self.chunksize,skipinitialspace=True,**kwargs)
        self.doFeatureCalc = calculateFeatures
        self.features = []
        self.__colMap = {}
        self.startTime  = None
        for col in self.FEATURE_MAP:
            self.__colMap[col] = self.FEATURE_MAP[col][0]
            self.features.append(self.FEATURE_MAP[col][0])

        if(self.doFeatureCalc):
            for f in self.CALCULABLE_FEATURES:
                self.features.append(f[0])

        self.prePreprocessors = []

        #print(self.__colMap)

    #     if(getColumns):
    #         df = self.reader.get_chunk(1)
    #         self.__getColumns(df)

    # def __getColumns(self, df):
    #     featureCheck = [f.lower() for f in self.FEATURES]

        # for col in df:
        #     if col.lower() in featureCheck

        
        #     if col.lower() in featureCheck and col not in self.FEATURES:
        #         self.reader.columns[col] = self.FEATURES[featureCheck.index(col.lower())]

    def addPreprocessor(self, preprocess):
        self.prePreprocessors.append(preprocess)

    def prePreprocessor(self,df):
        #timeinit = time.time() #! timing test

        dropCols = []
        for col in df.columns:
            if(col not in self.FEATURE_MAP):
                dropCols.append(col)

        df = df.drop(columns=dropCols)
        #timedrop = time.time() #! timing test

        for colName in df:
            col = df[colName]
            #print(col)
            converter = self.FEATURE_MAP[colName][1]
            #print(converter.convert(col))
            df[colName] = converter.convert(col)

        #timeconvert = time.time() #! timing test

        df = df.rename(columns=self.__colMap)

        #timerename = time.time() #! timing test

        if(self.doFeatureCalc):
            for feature,funct in self.CALCULABLE_FEATURES:
                df[feature] = funct(df)

        #timecalc = time.time() #! timing test

        if(self.startTime is None):
            self.startTime = df["Timestamp"].iloc[0]
        
        df["relTime"] = df["Timestamp"] - self.startTime

        #timetime = time.time() #! timing test
        
        # print(f'TIME::Drop={(timedrop-timeinit)*1000.0}ms')
        # print(f'TIME::Convert={(timeconvert-timedrop)*1000.0}ms')
        # print(f'TIME::Rename={(timerename-timeconvert)*1000.0}ms')
        # print(f'TIME::Calc={(timecalc-timerename)*1000.0}ms')
        # print(f'TIME::Time={(timetime-timecalc)*1000.0}ms')
        return self.runAdditionalPreprocess(df)

    def runAdditionalPreprocess(self,df):
        timelast = time.time()
        for proc in self.prePreprocessors:
            df = proc(df)
            #print(f'TIME::proc-{proc.__name__}={(time.time()-timelast)*1000.0}ms')
            timelast = time.time()
        return df

    def getFeatures(self):
        return self.features

    def getChunk(self, rows):
        df = self.reader.get_chunk(rows)
        df = self.prePreprocessor(df)
        return df

    def __iter__(self):
        return self

    def __next__(self):
        timeinit = time.time()
        df = next(self.reader)
        #print(f'TIME::Load={(time.time()-timeinit)*1000.0}ms')
        df = self.prePreprocessor(df)
        return df