from Loader.Dataset import Dataset
from enum import Enum
import pandas as pd

class CombinationMethod(Enum):
    SEQUENTIAL = 1
    INTERLACE = 2

class CombinedDataset(Dataset):
    def __init__(self, datasets, features, method,name=None, startTime=None, ):
        self.datasets = datasets
        self.name = name if name is not None else "Combined"
        self.features = features
        self.features.append("relTime")
        self.method = method

        self.datasetTimes = [] #dsStart,dsLast,offset
        self.startTime = startTime
        self.__dsTimeOffset = 0
        self.lastTime = 0
        self.prePreprocessors = []

        self.currentDataset = 0
        self.chunksize = datasets[0].chunksize
    
    def getFeatures(self):
        return self.features

    def prePreprocessor(self, df):
        return self.__runAdditionalPreprocess(df)

    def fixTimestamps(self, df, dsStartTime = 0, dsLastTime = 0, dsOffsetTime = 0):
        df["Timestamp"] = (df["Timestamp"] - dsStartTime) + dsOffsetTime
        return df

    def combineSequential2(self):
        print(f'Combining {self.datasets[self.currentDataset].name}')
        frames = []
        count = 0
        while (count < self.chunksize) and self.currentDataset < len(self.datasets):
            try:
                d = self.datasets[self.currentDataset].getChunk(self.chunksize-count)[self.features]
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

            d["Timestamp"] = d["relTime"] + self.startTime + self.__dsTimeOffset
            d["Dataset"] = self.datasets[self.currentDataset].name
            d = d.drop("relTime", axis=1) # limit dataset to features
            count += len(d)
            frames.append(d)
            
            # df = pd.concat([df,d],ignore_index=True)

        df = pd.concat(frames,ignore_index=True)
        if(len(df) == 0):
            raise StopIteration

        return df

    # TODO Remove
    # def combineSequential(self):
    #     print(f'Combining {self.datasets[self.currentDataset].name}')

    #     # get next chunk
    #     df = next(self.datasets[self.currentDataset])[self.features]

    #     #test if dataset has timing info
    #     if(len(self.datasetTimes) == 0):
    #         stime = df["Timestamp"][0]
    #         otime = self.startTime if self.startTime is not None else stime
    #         self.datasetTimes.append([stime,0,otime])

    #     #adjust the timing
    #     df = self.fixTimestamps(df,*self.datasetTimes[self.currentDataset])
    #     df["Dataset"] = self.datasets[self.currentDataset].name

    #     #find the largest time stamp for next dataset offset
    #     ltime = df["Timestamp"].max()
    #     if(ltime > self.datasetTimes[self.currentDataset][1]):
    #         self.datasetTimes[self.currentDataset][1] = ltime

    #     #test if full chunk was loaded
    #     while len(df) < self.chunksize:
    #         #change to next dataset to load the rest of the chunk
    #         self.currentDataset += 1
    #         if(self.currentDataset >= len(self.datasets)):
    #             break
            
    #         print(f'Only {len(df)} flows, combining additional {self.chunksize-len(df)} flows from {self.datasets[self.currentDataset].name}')
            
    #         #load remaining chunk
    #         d = self.datasets[self.currentDataset].getChunk(self.chunksize-len(df))[self.features]
    #         d["Dataset"] = self.datasets[self.currentDataset].name

    #         #create new time record
    #         stime = d["Timestamp"][0]
    #         otime = self.datasetTimes[self.currentDataset-1][1]
    #         self.datasetTimes.append([stime, 0, otime])

    #         d = self.fixTimestamps(d,*self.datasetTimes[self.currentDataset])
    #         self.datasetTimes[self.currentDataset][1] = d["Timestamp"].max()
            
    #         df = pd.concat([df,d])
    #     return df

    def __iter__(self):
        return self

    def __next__(self):
        if(self.currentDataset >= len(self.datasets)):
            raise StopIteration
        df = self.combineSequential2()
        df = self.runAdditionalPreprocess(df)
        return df