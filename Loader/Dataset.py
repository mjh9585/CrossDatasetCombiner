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

        self.preProcess = {
            "preConvert":[],
            "preCalc":[],
            "preProcess":[]
        }

    def addPreprocessor(self, preprocess):
        self.preProcess["preProcess"].append(preprocess)

    def prePreprocessor(self,df):
        timeinit = time.time() #! timing test

        #build list of columns to drop
        dropCols = []
        for col in df.columns:
            if(col not in self.FEATURE_MAP):
                dropCols.append(col)

        df = df.drop(columns=dropCols)
        timedrop = time.time() #! timing test

        #pre-conversion additional processing
        for conv in self.preProcess["preConvert"]:
            df = conv(df)

        #convert features to common units if needed
        timelast = time.time()
        for colName in df:
            converter = self.FEATURE_MAP[colName][1]
            if(converter is None):
                continue
            col = df[colName]
            
            df[colName] = converter.convert(col)
            #print(f'TIME::Conv-{converter.__name__}={(time.time()-timelast)*1000.0}ms')
            timelast = time.time()

        timeconvert = time.time() #! timing test

        df = df.rename(columns=self.__colMap)

        timerename = time.time() #! timing test

        #pre feature calculation additional processing
        for conv in self.preProcess["preCalc"]:
            df = conv(df)

        #Run feature calculation if enabled
        if(self.doFeatureCalc):
            timelast = time.time()
            for feature, funct in self.CALCULABLE_FEATURES:
                df[feature] = funct(df)
                #print(f'TIME::Conv-{funct.__name__}={(time.time()-timelast)*1000.0}ms')
                timelast = time.time()

        timecalc = time.time() #! timing test

        #calculate relative timestamps
        if(self.startTime is None):
            self.startTime = df["Timestamp"].iloc[0]
        
        df["relTime"] = df["Timestamp"] - self.startTime

        timetime = time.time() #! timing test
        
        # print(f'TIME::Drop={(timedrop-timeinit)*1000.0}ms') #! timing test
        # print(f'TIME::Convert={(timeconvert-timedrop)*1000.0}ms') #! timing test
        # print(f'TIME::Rename={(timerename-timeconvert)*1000.0}ms') #! timing test
        # print(f'TIME::Calc={(timecalc-timerename)*1000.0}ms') #! timing test
        # print(f'TIME::Time={(timetime-timecalc)*1000.0}ms') #! timing test
        # print(f'TIME::TotalProc={(timetime-timeinit)*1000.0}ms') #! timing test
        return self.runAdditionalPreprocess(df)

    def runAdditionalPreprocess(self,df):
        # timelast = time.time()
        for proc in self.preProcess["preProcess"]:
            df = proc(df)
            #print(f'TIME::proc-{proc.__name__}={(time.time()-timelast)*1000.0}ms')
            # timelast = time.time()
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