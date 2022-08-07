from Loader.Dataset import Dataset
from enum import Enum
import pandas as pd

class CombinationMethod(Enum):
    SEQUENTIAL = 1
    INTERLACE = 2

class CombinedDataset(Dataset):
    def __init__(self, datasets, features, method, name=None, startTime=None, offsetTime=None):
        super().__init__(None,name)
        self.datasets = datasets
        self.name = name if name is not None else "Combined"
        self.features = features
        self.features.append("relTime")
        self.features.append("Dataset")
        self.method = method

        self.startTime = startTime
        self.__dsTimeOffset = 0
        self.lastTime = 0

        self.currentDataset = 0

        #build a list of offset times for each dataset
        if(isinstance(offsetTime, list)):
            n = len(self.datasets)
            self.offsetTime = offsetTime[:n] + [0]*(n-len(offsetTime))
        else:
            amount = 0 if offsetTime is None else offsetTime
            self.offsetTime = [amount] * len(self.datasets)

        # d = [
        #     {"df":df, "index":i, "len":n}
        # ]

    
    # def addPreConvertProcessor(self, preprocess):
    #     '''
    #     Adds a Preprocessor to each dataset before performing unit conversion and column renaming
    #     '''
    #     self.preProcess["preConvert"].append(preprocess)

    # def addPreCalculateProcessor(self, preprocess):
    #     '''
    #     Adds a Preprocessor to each dataset before performing additional feature calculation
    #     '''
    #     self.preProcess["preCalc"].append(preprocess)

    def combineSequential(self,nRows):
        print(f'Combining {self.datasets[self.currentDataset].name}')
        frames = []
        count = 0
        while (count < nRows) and self.currentDataset < len(self.datasets):
            try:

                d = self.datasets[self.currentDataset].getChunk(nRows-count)
                d = d[d.columns.intersection(self.features)]
            except StopIteration:
                print(f'Reached end of {self.datasets[self.currentDataset].name} with {count} flows, ',end='')
                self.currentDataset += 1
                self.__dsTimeOffset += self.lastTime    

                if(self.currentDataset < len(self.datasets)):
                    print(f'switching to {self.datasets[self.currentDataset].name}')
                else:
                    print(f'end of combined dataset')        
                continue

            if (self.startTime is None):
                self.startTime = d["Timestamp"].iloc[0]

            ltime = d["relTime"].max()
            if(ltime > self.lastTime):
                self.lastTime = ltime

            d["relTime"] = d["relTime"] + self.__dsTimeOffset + self.offsetTime[self.currentDataset]
            d["Timestamp"] = d["relTime"] + self.startTime
            
            if("Dataset" not in d.columns):
                d["Dataset"] = self.datasets[self.currentDataset].name

            #d = d.drop("relTime", axis=1) # limit dataset to features
            count += len(d)
            frames.append(d)
            
            # df = pd.concat([df,d],ignore_index=True)

        df = pd.concat(frames,ignore_index=True)
        if(len(df) == 0):
            raise StopIteration

        return df
    
    def combineInterlaced(self, nRows):
        pass

    
    def reset(self):
        for ds in self.datasets:
            ds.reset()

    def getChunk(self, rows):
        if(self.currentDataset >= len(self.datasets)):
            raise StopIteration
        df = self.combineSequential(rows)
        df = self.runAdditionalPreprocess(df)
        return df

    def __iter__(self):
        return self

    def __next__(self):
        if(self.currentDataset >= len(self.datasets)):
            raise StopIteration
        df = self.combineSequential(self.chunksize)
        df = self.runAdditionalPreprocess(df)
        return df